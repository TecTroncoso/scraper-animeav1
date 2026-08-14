#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
animeav1_scraper.py - Scraper ASINCRONO modular, robusto y respetuoso para animeav1.com
================================================================================

Archivo UNICO que integra todos los modulos del proyecto:

  - config            : URLs, rate-limits, paths, concurrencia
  - models            : pydantic v2 (Anime/Temporada/Capitulo/Proveedor)
  - utils             : decode_provider_url, resolver SvelteKit, logging, dedupe
  - parsers           : parsing HTML (fallback)
  - http_client       : httpx.AsyncClient + tenacity async + semaforo
  - catalog_scraper   : listado + paginacion (async)
  - anime_scraper     : detalle de anime (async)
  - episode_scraper   : detalle de capitulo (async, paralelo con gather)
  - home_scraper      : episodios del dia (async)
  - storage           : JSONL unificado + SQLite (upsert en cascada)
  - watcher           : daemon incremental cada N minutos (async)
  - main / CLI        : --mode full|catalog|today|anime|stats|validate|watch|full-and-watch

Mejoras respecto a la version sincrona:
  - 100% async con httpx.AsyncClient
  - asyncio.gather para paralelizar descarga de capitulos y animes
  - Semaforo configurable (--concurrency, default 5)
  - JSONL unificado: data/animes.jsonl (1 linea por anime, upsert por slug)
  - SQLite sigue siendo la fuente estructurada para consultas

Uso:
    python animeav1_scraper.py --mode catalog
    python animeav1_scraper.py --mode full --limit 5
    python animeav1_scraper.py --mode today
    python animeav1_scraper.py --mode anime "bleach-sennen-kessen-hen-kashin-tan"
    python animeav1_scraper.py --mode stats
    python animeav1_scraper.py --mode validate
    python animeav1_scraper.py --mode watch --watch-interval 1800
    python animeav1_scraper.py --mode full-and-watch

Requisitos:
    pip install httpx tenacity beautifulsoup4 lxml pydantic
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import json
import logging
import random
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional
from urllib.parse import unquote, urljoin, urlparse

# Terceros
import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, model_validator
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)


# ============================================================================
# 1. CONFIG
# ============================================================================

BASE_DIR: Path = Path(__file__).resolve().parent
DATA_DIR: Path = BASE_DIR / "data"
LOGS_DIR: Path = BASE_DIR / "logs"
LOG_FILE: Path = LOGS_DIR / "scrape.log"

# JSONL unificado: 1 anime por linea, upsert por slug
ANIMES_JSONL_PATH: Path = DATA_DIR / "animes.jsonl"
# Log de errores persistente (JSONL: 1 error por linea)
ERRORS_LOG_PATH: Path = DATA_DIR / "errors.jsonl"

# --- Configuracion del sitio ---
SITE_BASE_URL: str = "https://animeav1.com"
CDN_BASE_URL: str = "https://cdn.animeav1.com"
CATALOG_PATH: str = "/catalogo"
HOME_PATH: str = "/"

# --- HTTP ---
USER_AGENT: str = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": USER_AGENT,
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# --- Rate limiting ---
RATE_LIMIT_MIN_SECONDS: float = 0.5
RATE_LIMIT_MAX_SECONDS: float = 1.2

# --- Reintentos ---
MAX_RETRIES: int = 3
RETRY_BACKOFF_MIN: float = 1.0
RETRY_BACKOFF_MAX: float = 30.0
HTTP_TIMEOUT: float = 30.0

# --- Concurrencia ---
DEFAULT_CONCURRENCY: int = 5  # peticiones simultaneas maximas
CATALOG_PAGE_SIZE: int = 20
CATALOG_MAX_PAGES_FALLBACK: int = 100


def ensure_dirs() -> None:
    """Crea los directorios base si no existen."""
    for p in (DATA_DIR, LOGS_DIR):
        p.mkdir(parents=True, exist_ok=True)


# ============================================================================
# 2. MODELS (pydantic v2) — sin cambios
# ============================================================================

class EstadoAnime(Enum):
    FINALIZADO = "Finalizado"
    EN_EMISION = "En Emision"
    OVA = "OVA"
    PELICULA = "Pelicula"
    ESPECIAL = "Especial"
    TV_ANIME = "TV Anime"
    DESCONOCIDO = "Desconocido"


class MetodoDecodificacion(Enum):
    DIRECT = "direct"
    BASE64 = "base64"
    REVERSE_BASE64 = "reverse_base64"
    URL_DECODE = "url_decode"
    JS_FUNCTION = "js_function"
    FAILED = "failed"


class TipoProveedor(Enum):
    IFRAME = "iframe"
    EMBED = "embed"
    DIRECT = "direct"
    DOWNLOAD = "download"
    UNKNOWN = "unknown"


class Proveedor(BaseModel):
    nombre: str = Field(...)
    tipo: TipoProveedor = TipoProveedor.UNKNOWN
    url: Optional[str] = None
    url_raw: Optional[str] = None
    metodo_decodificacion: MetodoDecodificacion = MetodoDecodificacion.DIRECT
    resoluciones: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_url_or_raw(self):
        if not self.url and not self.url_raw:
            self.metodo_decodificacion = MetodoDecodificacion.FAILED
        return self

    @model_validator(mode="after")
    def _check_failed_consistency(self):
        if self.metodo_decodificacion == MetodoDecodificacion.FAILED:
            self.url = None
        return self


class Capitulo(BaseModel):
    numero: int
    titulo: Optional[str] = None
    descripcion: Optional[str] = None
    imagenes: list[str] = Field(default_factory=list)
    fecha_publicacion: Optional[str] = None
    url_origen: str
    proveedores: list[Proveedor] = Field(default_factory=list)
    descargas: list[Proveedor] = Field(default_factory=list)
    filler: bool = False
    sitio_id: Optional[int] = None

    @model_validator(mode="after")
    def _dedupe_providers(self):
        def _key(p): return (p.nombre or "", p.url or p.url_raw or "")
        if self.proveedores:
            self.proveedores = dedupe_preserve_order(self.proveedores, key=_key)
        if self.descargas:
            self.descargas = dedupe_preserve_order(self.descargas, key=_key)
        return self


class Temporada(BaseModel):
    numero: int
    titulo: Optional[str] = None
    capitulos: list[Capitulo] = Field(default_factory=list)

    @model_validator(mode="after")
    def _dedupe_capitulos(self):
        if not self.capitulos:
            return self
        seen: set[int] = set()
        unique: list[Capitulo] = []
        for c in self.capitulos:
            if c.numero in seen:
                continue
            seen.add(c.numero)
            unique.append(c)
        unique.sort(key=lambda x: x.numero)
        self.capitulos = unique
        return self


class Genero(BaseModel):
    id: Optional[int] = None
    nombre: str
    slug: Optional[str] = None


class Anime(BaseModel):
    id: str
    titulo: str
    titulo_alternativo: Optional[str] = None
    descripcion: Optional[str] = None
    estado: EstadoAnime = EstadoAnime.DESCONOCIDO
    estado_raw: str = ""
    status_code: Optional[int] = None
    categoria: Optional[str] = None
    foto_portada: Optional[str] = None
    trailer_youtube_id: Optional[str] = None
    generos: list[Genero] = Field(default_factory=list)
    url_origen: str
    temporadas: list[Temporada] = Field(default_factory=list)
    sitio_id: Optional[int] = None
    mal_id: Optional[int] = None
    score: Optional[float] = None
    votos: Optional[int] = None
    fecha_inicio: Optional[str] = None
    fecha_fin: Optional[str] = None
    scraped_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @model_validator(mode="after")
    def _dedupe_temporadas(self):
        if not self.temporadas:
            return self
        seen: set[int] = set()
        unique: list[Temporada] = []
        for t in self.temporadas:
            if t.numero in seen:
                continue
            seen.add(t.numero)
            unique.append(t)
        unique.sort(key=lambda x: x.numero)
        self.temporadas = unique
        return self

    def count_capitulos(self) -> int:
        return sum(len(t.capitulos) for t in self.temporadas)

    def count_proveedores(self) -> int:
        return sum(
            len(p)
            for t in self.temporadas
            for c in t.capitulos
            for p in (c.proveedores, c.descargas)
        )

    def validate_hierarchy(self) -> dict:
        issues: list[str] = []
        proveedores_sin_url = 0
        capitulos_sin_proveedor = 0
        temporadas_vacias = 0

        if self.temporadas:
            numeros = sorted(t.numero for t in self.temporadas)
            esperado = list(range(numeros[0], numeros[0] + len(numeros)))
            if numeros != esperado:
                issues.append(f"Temporadas con numeros no correlativos: {numeros}")

        for ti, t in enumerate(self.temporadas):
            if not t.capitulos:
                temporadas_vacias += 1
                issues.append(f"Temporada {t.numero} (idx={ti}) no tiene capitulos")
                continue
            cap_numeros = [c.numero for c in t.capitulos]
            if cap_numeros != sorted(cap_numeros):
                issues.append(f"Temporada {t.numero}: capitulos fuera de orden: {cap_numeros}")
            for c in t.capitulos:
                if not c.proveedores and not c.descargas:
                    capitulos_sin_proveedor += 1
                    issues.append(f"Temporada {t.numero} cap {c.numero}: sin proveedores ni descargas")
                for p in c.proveedores + c.descargas:
                    if not p.url and not p.url_raw:
                        proveedores_sin_url += 1
                        issues.append(f"Temporada {t.numero} cap {c.numero} proveedor '{p.nombre}': sin url ni url_raw")

        return {
            "anime_id": self.id,
            "temporadas": len(self.temporadas),
            "capitulos": self.count_capitulos(),
            "proveedores_stream": sum(len(c.proveedores) for t in self.temporadas for c in t.capitulos),
            "proveedores_download": sum(len(c.descargas) for t in self.temporadas for c in t.capitulos),
            "proveedores_sin_url": proveedores_sin_url,
            "capitulos_sin_proveedor": capitulos_sin_proveedor,
            "temporadas_vacias": temporadas_vacias,
            "issues": issues,
        }


class AnimeDocument(BaseModel):
    anime: Anime

    def validate_hierarchy(self) -> dict:
        return self.anime.validate_hierarchy()


# ============================================================================
# 3. UTILS
# ============================================================================

# --- Logging ---

def setup_logging(log_file: Path, level: int = logging.INFO) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("animeav1")
    logger.setLevel(level)
    logger.propagate = False
    if not logger.handlers:
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger


# --- Normalizacion ---

_WS_RE = re.compile(r"\s+")


def normalize_text(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    text = unicodedata.normalize("NFC", text)
    text = _WS_RE.sub(" ", text).strip()
    return text or None


def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[-\s]+", "-", text) or "item"


# --- Rate limiting async ---

async def async_rate_limit_sleep(min_s: float, max_s: float) -> None:
    """Sleep async con jitter aleatorio entre min_s y max_s."""
    await asyncio.sleep(random.uniform(min_s, max_s))


# --- Decodificacion de URLs de proveedores (sin cambios) ---

_URL_SCHEME_RE = re.compile(r"^https?://", re.IGNORECASE)


def _looks_like_url(s: str) -> bool:
    if not isinstance(s, str) or len(s) < 7:
        return False
    if _URL_SCHEME_RE.match(s):
        try:
            parsed = urlparse(s)
            return bool(parsed.netloc)
        except Exception:
            return False
    return False


def _try_base64(s: str, urlsafe: bool = False) -> Optional[str]:
    if not s or len(s) < 4:
        return None
    try:
        b = s.encode("ascii", errors="ignore")
        missing = len(b) % 4
        if missing:
            b += b"=" * (4 - missing)
        if urlsafe:
            raw = base64.urlsafe_b64decode(b)
        else:
            raw = base64.b64decode(b, validate=False)
        decoded = raw.decode("utf-8", errors="strict")
        if decoded and all(31 < ord(c) < 0x10FFFF or c in "\t\n\r" for c in decoded):
            return decoded
    except (binascii.Error, ValueError, UnicodeDecodeError):
        return None
    except Exception:
        return None
    return None


def _try_url_decode(s: str) -> Optional[str]:
    if not s or "%" not in s:
        return None
    try:
        decoded = unquote(s)
        if decoded != s:
            return decoded
    except Exception:
        return None
    return None


_JS_REPLACE_RE = re.compile(
    r"\.replace\(/\[(?P<from>[^\]]+)\]/g,\s*[\"'](?P<to>[^\"']*)[\"']\)"
)
_JS_ATOB_RE = re.compile(r"\batob\s*\(\s*[\"']([A-Za-z0-9+/=_-]+)[\"']\s*\)")


def _try_js_patterns(s: str) -> tuple[Optional[str], Optional[str]]:
    m = _JS_ATOB_RE.search(s)
    if m:
        decoded = _try_base64(m.group(1)) or _try_base64(m.group(1), urlsafe=True)
        if decoded and _looks_like_url(decoded):
            return decoded, MetodoDecodificacion.JS_FUNCTION.value
    result = s
    matched = False
    for rm in _JS_REPLACE_RE.finditer(s):
        matched = True
        from_chars = rm.group("from")
        to_char = rm.group("to")[:1] if rm.group("to") else ""
        for ch in from_chars:
            result = result.replace(ch, to_char)
        result = result.replace(rm.group(0), "")
    if matched and _looks_like_url(result):
        return result, MetodoDecodificacion.JS_FUNCTION.value
    return None, None


def decode_provider_url(raw_url: Optional[str]) -> tuple[Optional[str], str]:
    if raw_url is None:
        return None, MetodoDecodificacion.FAILED.value
    raw = raw_url.strip()
    if not raw:
        return None, MetodoDecodificacion.FAILED.value
    if _looks_like_url(raw):
        return raw, MetodoDecodificacion.DIRECT.value
    decoded = _try_url_decode(raw)
    if decoded and _looks_like_url(decoded):
        return decoded, MetodoDecodificacion.URL_DECODE.value
    decoded = _try_base64(raw)
    if decoded and _looks_like_url(decoded):
        return decoded, MetodoDecodificacion.BASE64.value
    decoded = _try_base64(raw, urlsafe=True)
    if decoded and _looks_like_url(decoded):
        return decoded, MetodoDecodificacion.BASE64.value
    reversed_raw = raw[::-1]
    decoded = _try_base64(reversed_raw)
    if decoded and _looks_like_url(decoded):
        return decoded, MetodoDecodificacion.REVERSE_BASE64.value
    decoded = _try_base64(reversed_raw, urlsafe=True)
    if decoded and _looks_like_url(decoded):
        return decoded, MetodoDecodificacion.REVERSE_BASE64.value
    js_result, js_method = _try_js_patterns(raw)
    if js_result:
        return js_result, js_method
    return None, MetodoDecodificacion.FAILED.value


# --- Resolver SvelteKit ---

def resolve_sveltekit_payload(payload: dict) -> Optional[dict]:
    nodes = payload.get("nodes") or []
    target_node = None
    for n in nodes:
        if n and isinstance(n, dict) and n.get("type") == "data":
            target_node = n
    if not target_node or not target_node.get("data"):
        return None
    data_array = target_node["data"]
    if not data_array:
        return None

    def resolve_value(val: Any, seen: set[int]) -> Any:
        if isinstance(val, dict):
            return {k: resolve_ref(v, seen) for k, v in val.items()}
        if isinstance(val, list):
            return [resolve_ref(v, seen) for v in val]
        return val

    def resolve_ref(val: Any, seen: set[int]) -> Any:
        if isinstance(val, bool):
            return val
        if isinstance(val, int) and 0 <= val < len(data_array) and val not in seen:
            target = data_array[val]
            if isinstance(target, (dict, list)):
                return resolve_value(target, seen | {val})
            return target
        return resolve_value(val, seen)

    try:
        return resolve_ref(0, set())
    except RecursionError:
        return None


_EMBEDDED_DATA_RE = re.compile(
    r"data:\s*\[(?P<body>.*?)\]\s*,\s*form:\s*null",
    re.DOTALL,
)


def extract_embedded_sveltekit_data(html: str) -> Optional[dict]:
    m = _EMBEDDED_DATA_RE.search(html)
    if not m:
        return None
    body = m.group("body")
    body = body.replace("void 0", "null")
    body = re.sub(
        r"(?P<prefix>[{,]\s*)(?P<key>[A-Za-z_$][A-Za-z0-9_$]*)(\s*):",
        lambda mm: mm.group("prefix") + '"' + mm.group("key") + '":',
        body,
    )
    if "(function" in body:
        return None
    try:
        arr = json.loads("[" + body + "]")
    except json.JSONDecodeError:
        return None
    for n in reversed(arr):
        if isinstance(n, dict) and n.get("type") == "data" and isinstance(n.get("data"), dict):
            return n["data"]
    return None


def dedupe_preserve_order(items: list[Any], key=None) -> list[Any]:
    seen = set()
    out = []
    for it in items:
        k = key(it) if key else it
        if k in seen:
            continue
        seen.add(k)
        out.append(it)
    return out


def _safe_int(v) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


# ============================================================================
# 4. PARSERS (HTML fallback) — sin cambios
# ============================================================================

_ANIME_CARD_LINK_RE = re.compile(r"^/media/([^/?#]+)$")

_STATUS_CODE_MAP = {
    1: EstadoAnime.FINALIZADO,
    2: EstadoAnime.EN_EMISION,
}
_CATEGORY_MAP = {
    "TV Anime": EstadoAnime.TV_ANIME,
    "OVA": EstadoAnime.OVA,
    "Pelicula": EstadoAnime.PELICULA,
    "Especial": EstadoAnime.ESPECIAL,
}


def _abs(url: str) -> str:
    if url.startswith(("http://", "https://")):
        return url
    return urljoin(SITE_BASE_URL + "/", url.lstrip("/"))


def parse_catalog_html(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    out: list[dict] = []
    seen_slugs: set[str] = set()
    for article in soup.find_all("article"):
        link = article.find("a", href=True)
        if not link:
            continue
        m = _ANIME_CARD_LINK_RE.match(link["href"])
        if not m:
            continue
        slug = m.group(1)
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        h3 = article.find("h3")
        titulo = h3.get_text(strip=True) if h3 else slug
        img = article.find("img", src=True)
        foto = _abs(img["src"]) if img else None
        desc = None
        p = article.find("p")
        if p:
            desc = p.get_text(strip=True) or None
        cat = None
        for div in article.find_all("div", class_=True):
            txt = div.get_text(strip=True)
            if txt in {"TV Anime", "OVA", "Pelicula", "Especial"}:
                cat = txt
                break
        out.append({
            "id": slug, "slug": slug, "titulo": titulo,
            "url_origen": _abs(f"/media/{slug}"),
            "foto_portada": foto, "descripcion": desc, "categoria": cat,
        })
    return out


def parse_catalog_pagination(html: str) -> Optional[int]:
    soup = BeautifulSoup(html, "lxml")
    max_page = 0
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]page=(\d+)", a["href"])
        if m:
            max_page = max(max_page, int(m.group(1)))
    return max_page or None


def parse_anime_detail_html(html: str, slug: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    h1 = soup.find("h1")
    titulo = h1.get_text(strip=True) if h1 else slug
    h2_alt = None
    for h2 in soup.find_all("h2"):
        txt = h2.get_text(strip=True)
        if txt and txt != "Episodios" and txt != "Relacionados":
            h2_alt = txt
            break
    generos = []
    for a in soup.find_all("a", href=True):
        m = re.match(r"^/catalogo\?genre=([^&]+)$", a["href"])
        if m:
            nombre = a.get_text(strip=True)
            if nombre:
                generos.append({"slug": m.group(1), "nombre": nombre})
    episodios = []
    seen = set()
    for a in soup.find_all("a", href=True):
        m = re.match(rf"^/media/{re.escape(slug)}/(\d+)$", a["href"])
        if m:
            n = int(m.group(1))
            if n in seen:
                continue
            seen.add(n)
            episodios.append({"numero": n, "url": _abs(a["href"])})
    return {
        "id": slug, "slug": slug, "titulo": titulo,
        "titulo_alternativo": h2_alt, "generos": generos,
        "episodios": episodios, "url_origen": _abs(f"/media/{slug}"),
    }


def normalize_estado(
    category_name: Optional[str] = None,
    status_code: Optional[int] = None,
) -> EstadoAnime:
    if status_code is not None and status_code in _STATUS_CODE_MAP:
        return _STATUS_CODE_MAP[status_code]
    if category_name and category_name in _CATEGORY_MAP:
        return _CATEGORY_MAP[category_name]
    return EstadoAnime.DESCONOCIDO


# ============================================================================
# 5. HTTP CLIENT ASINCRONO
# ============================================================================

logger = logging.getLogger("animeav1")


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.ConnectError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status >= 500 or status == 429
    if isinstance(exc, httpx.TransportError):
        return True
    return False


def _log_retry(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    attempt = retry_state.attempt_number
    logging.getLogger("animeav1.http").warning(
        "Reintento #%d tras error: %r", attempt, exc
    )


_retry_decorator = retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=RETRY_BACKOFF_MIN, max=RETRY_BACKOFF_MAX),
    before_sleep=_log_retry,
    reraise=True,
)


class AsyncHttpClient:
    """Cliente HTTP async con semaforo de concurrencia y rate-limiting.

    - Usa httpx.AsyncClient con http2=True y un connection pool.
    - Semaforo ``concurrency`` para limitar peticiones simultaneas.
    - Rate limit jitter aplicado DENTRO del semaforo (para respetar el sitio
      aunque tengamos 50 tasks en cola).
    - Reintentos con tenacity async.
    """

    def __init__(
        self,
        base_url: str = SITE_BASE_URL,
        headers: Optional[dict] = None,
        timeout: float = HTTP_TIMEOUT,
        rate_min: float = RATE_LIMIT_MIN_SECONDS,
        rate_max: float = RATE_LIMIT_MAX_SECONDS,
        concurrency: int = DEFAULT_CONCURRENCY,
    ):
        self._base_url = base_url.rstrip("/")
        self._headers = {**DEFAULT_HEADERS, **(headers or {})}
        self._timeout = timeout
        self._rate_min = rate_min
        self._rate_max = rate_max
        self._semaphore = asyncio.Semaphore(concurrency)
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "AsyncHttpClient":
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers=self._headers,
            timeout=self._timeout,
            follow_redirects=True,
            http2=True,
            limits=httpx.Limits(
                max_connections=self._semaphore._value * 2,
                max_keepalive_connections=self._semaphore._value,
            ),
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _respect_rate_limit(self) -> None:
        await async_rate_limit_sleep(self._rate_min, self._rate_max)

    @_retry_decorator
    async def get_text(self, path: str, *, params: Optional[dict] = None) -> str:
        async with self._semaphore:
            await self._respect_rate_limit()
            assert self._client is not None
            resp = await self._client.get(path, params=params)
            if resp.status_code >= 400:
                logging.getLogger("animeav1.http").error(
                    "HTTP %d en %s (params=%s)", resp.status_code, path, params
                )
                resp.raise_for_status()
            return resp.text

    @_retry_decorator
    async def get_json(self, path: str, *, params: Optional[dict] = None) -> dict:
        async with self._semaphore:
            await self._respect_rate_limit()
            assert self._client is not None
            resp = await self._client.get(path, params=params)
            if resp.status_code >= 400:
                logging.getLogger("animeav1.http").error(
                    "HTTP %d en %s (params=%s)", resp.status_code, path, params
                )
                resp.raise_for_status()
            return resp.json()

    async def fetch_robots_txt(self) -> str:
        try:
            assert self._client is not None
            resp = await self._client.get("/robots.txt")
            if resp.status_code == 200:
                return resp.text
        except Exception as exc:
            logging.getLogger("animeav1.http").warning(
                "No se pudo obtener robots.txt: %r", exc
            )
        return ""


def build_async_http_client(args) -> AsyncHttpClient:
    """Construye un AsyncHttpClient a partir de args CLI."""
    concurrency = getattr(args, "concurrency", DEFAULT_CONCURRENCY)
    return AsyncHttpClient(
        rate_min=getattr(args, "rate_min", RATE_LIMIT_MIN_SECONDS),
        rate_max=getattr(args, "rate_max", RATE_LIMIT_MAX_SECONDS),
        concurrency=concurrency,
    )


# ============================================================================
# 6. CATALOG SCRAPER (async)
# ============================================================================

def _data_json_path(path: str) -> str:
    return path.rstrip("/") + "/__data.json"


def _parse_catalog_payload(payload: dict) -> tuple[list[dict], int]:
    root = payload.get("results") or []
    total = payload.get("total") or len(root)
    out = []
    for item in root:
        if not isinstance(item, dict):
            continue
        category = item.get("category") or {}
        out.append({
            "sitio_id": _safe_int(item.get("id")),
            "id": item.get("slug") or "",
            "slug": item.get("slug") or "",
            "titulo": item.get("title") or "",
            "descripcion": item.get("synopsis") or None,
            "categoryId": item.get("categoryId"),
            "categoria": category.get("name") if isinstance(category, dict) else None,
            "url_origen": f"{SITE_BASE_URL}/media/{item.get('slug')}",
        })
    return out, int(total)


async def scrape_catalog_page_async(
    client: AsyncHttpClient, page: int
) -> tuple[list[dict], Optional[int]]:
    """Scrapea una pagina del catalogo (async)."""
    log = logging.getLogger("animeav1.catalog")
    try:
        payload = await client.get_json(
            _data_json_path(CATALOG_PATH),
            params={"page": page} if page > 1 else None,
        )
        resolved = resolve_sveltekit_payload(payload)
        if resolved and isinstance(resolved, dict) and "results" in resolved:
            animes, total = _parse_catalog_payload(resolved)
            return animes, (total if page == 1 else None)
        log.warning("Payload __data.json inesperado en pagina %d; cayendo a HTML", page)
    except Exception as exc:
        log.warning("Fallo __data.json en pagina %d (%r); cayendo a HTML", page, exc)

    html = await client.get_text(CATALOG_PATH, params={"page": page} if page > 1 else None)
    animes = parse_catalog_html(html)
    max_page = parse_catalog_pagination(html) if page == 1 else None
    total_estimado = (max_page * CATALOG_PAGE_SIZE) if max_page else None
    return animes, total_estimado


async def scrape_catalog_pages_async(
    client: AsyncHttpClient, page_numbers: list[int]
) -> list[tuple[int, list[dict], Optional[int]]]:
    """Scrapea multiples paginas en paralelo con asyncio.gather.

    Devuelve una lista de tuplas (page, animes, total) en el mismo orden
    que page_numbers.
    """
    tasks = [scrape_catalog_page_async(client, p) for p in page_numbers]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out = []
    for page, r in zip(page_numbers, results):
        if isinstance(r, Exception):
            logging.getLogger("animeav1.catalog").error(
                "Error en pagina %d: %r", page, r
            )
            out.append((page, [], None))
        else:
            animes, total = r
            out.append((page, animes, total))
    return out


async def scrape_all_animes_async(
    client: AsyncHttpClient,
    *,
    max_pages: Optional[int] = None,
    limit: Optional[int] = None,
) -> list[dict]:
    """Itera todas las paginas del catalogo en paralelo.

    Estrategia:
      1. Scrapea pagina 1 para conocer el total.
      2. Calcula cuantas paginas faltan.
      3. Scrapea todas las paginas restantes EN PARALELO con asyncio.gather.
      4. Dedupe por slug.
    """
    log = logging.getLogger("animeav1.catalog")
    # Pagina 1
    animes_p1, total = await scrape_catalog_page_async(client, 1)
    if not animes_p1:
        log.warning("Catalogo vacio en pagina 1")
        return []

    if total is not None:
        log.info("Catalogo: total reportado por el sitio = %d animes", total)

    seen_slugs: set[str] = set()
    all_animes: list[dict] = []
    for a in animes_p1:
        slug = a.get("slug") or a.get("id")
        if slug and slug not in seen_slugs:
            seen_slugs.add(slug)
            all_animes.append(a)
    log.info("Pagina 1: %d animes (acumulado=%d)", len(animes_p1), len(all_animes))

    # Calcular paginas restantes
    if len(animes_p1) < CATALOG_PAGE_SIZE:
        log.info("Pagina 1 trajo < %d animes, fin del catalogo", CATALOG_PAGE_SIZE)
        if limit is not None:
            all_animes = all_animes[:limit]
        return all_animes

    if total is not None:
        estimated_pages = (total + CATALOG_PAGE_SIZE - 1) // CATALOG_PAGE_SIZE
    else:
        # Sin total, estimar por pagina 1 (20 animes)
        estimated_pages = 50  # valor conservador
    if max_pages is not None:
        estimated_pages = min(estimated_pages, max_pages)
    estimated_pages = min(estimated_pages, CATALOG_MAX_PAGES_FALLBACK)

    remaining_pages = list(range(2, estimated_pages + 1))
    log.info(
        "Scrapeando %d paginas restantes en paralelo (concurrency=%d)...",
        len(remaining_pages), client._semaphore._value,
    )

    # Scrapear todas las paginas restantes en paralelo
    pages_results = await scrape_catalog_pages_async(client, remaining_pages)

    for page, animes, _ in pages_results:
        if not animes:
            log.warning("Pagina %d vacia", page)
            continue
        nuevos = 0
        for a in animes:
            slug = a.get("slug") or a.get("id")
            if slug and slug not in seen_slugs:
                seen_slugs.add(slug)
                all_animes.append(a)
                nuevos += 1
        log.info("Pagina %d: %d animes (%d nuevos, acumulado=%d)",
                 page, len(animes), nuevos, len(all_animes))
        if limit is not None and len(all_animes) >= limit:
            break

    if limit is not None:
        all_animes = all_animes[:limit]
    log.info("Catalogo completo: %d animes", len(all_animes))
    return all_animes


# ============================================================================
# 7. ANIME SCRAPER (async)
# ============================================================================

def _build_generos(payload_media: dict) -> list[Genero]:
    out = []
    for g in payload_media.get("genres") or []:
        if not isinstance(g, dict):
            continue
        out.append(Genero(
            id=_safe_int(g.get("id")),
            nombre=g.get("name") or "",
            slug=g.get("slug"),
        ))
    return out


def _build_capitulos_from_episodes(payload_media: dict, slug: str) -> list[Capitulo]:
    out: list[Capitulo] = []
    for ep in payload_media.get("episodes") or []:
        if not isinstance(ep, dict):
            continue
        numero = _safe_int(ep.get("number"))
        if numero is None:
            continue
        ep_id = _safe_int(ep.get("id"))
        out.append(Capitulo(
            numero=numero,
            url_origen=f"{SITE_BASE_URL}/media/{slug}/{numero}",
            sitio_id=ep_id,
        ))
    return out


def _build_anime_from_payload(payload: dict, slug: str) -> Anime:
    media = payload.get("media") if isinstance(payload, dict) else None
    if not isinstance(media, dict):
        raise ValueError("Payload sin clave 'media'")

    generos = _build_generos(media)
    category = media.get("category") if isinstance(media.get("category"), dict) else {}
    category_name = category.get("name") if category else None
    status_code = _safe_int(media.get("status"))
    estado = normalize_estado(category_name=category_name, status_code=status_code)

    poster = media.get("poster")
    sitio_id = _safe_int(media.get("id"))
    if not poster and sitio_id is not None:
        poster = f"{CDN_BASE_URL}/covers/{sitio_id}.jpg"

    aka = media.get("aka") if isinstance(media.get("aka"), dict) else {}
    titulo_alt = aka.get("ja-jp") or aka.get("en-us")

    capitulos = _build_capitulos_from_episodes(media, slug)
    seasons_raw = media.get("seasons")
    if isinstance(seasons_raw, list) and seasons_raw:
        temporadas = []
        for i, s in enumerate(seasons_raw, start=1):
            if isinstance(s, dict):
                temporadas.append(Temporada(
                    numero=_safe_int(s.get("number")) or i,
                    titulo=s.get("title"),
                    capitulos=capitulos,
                ))
            else:
                temporadas.append(Temporada(numero=i, capitulos=capitulos))
    else:
        temporadas = [Temporada(numero=1, capitulos=capitulos)] if capitulos else []

    estado_raw = category_name or ""
    if status_code is not None:
        estado_raw = f"{estado_raw} (status={status_code})".strip()

    return Anime(
        id=slug,
        titulo=media.get("title") or slug,
        titulo_alternativo=titulo_alt,
        descripcion=media.get("synopsis"),
        estado=estado,
        estado_raw=estado_raw,
        status_code=status_code,
        categoria=category_name,
        foto_portada=poster,
        trailer_youtube_id=media.get("trailer"),
        generos=generos,
        url_origen=f"{SITE_BASE_URL}/media/{slug}",
        temporadas=temporadas,
        sitio_id=sitio_id,
        mal_id=_safe_int(media.get("malId")),
        score=media.get("score"),
        votos=_safe_int(media.get("votes")),
        fecha_inicio=media.get("startDate"),
        fecha_fin=media.get("endDate"),
    )


def _extract_slug(slug_or_url: str) -> Optional[str]:
    if not slug_or_url:
        return None
    s = slug_or_url.strip()
    if s.startswith(("http://", "https://")):
        m = re.search(r"/media/([^/?#]+)", s)
        return m.group(1) if m else None
    if s.startswith("/media/"):
        return s.split("/")[2] if len(s.split("/")) > 2 else None
    return s


async def scrape_anime_async(
    client: AsyncHttpClient, slug_or_url: str
) -> Optional[Anime]:
    """Scrapea el detalle de un anime (async)."""
    log = logging.getLogger("animeav1.anime")
    slug = _extract_slug(slug_or_url)
    if not slug:
        log.error("No se pudo extraer slug de %r", slug_or_url)
        return None

    try:
        payload = await client.get_json(f"/media/{slug}/__data.json")
        resolved = resolve_sveltekit_payload(payload)
        if resolved and isinstance(resolved, dict) and "media" in resolved:
            anime = _build_anime_from_payload(resolved, slug)
            log.info(
                "Anime %s: %d temp(s), %d caps (via __data.json)",
                slug, len(anime.temporadas), anime.count_capitulos(),
            )
            return anime
        log.warning("Payload __data.json sin 'media' para %s", slug)
    except Exception as exc:
        log.warning("Fallo __data.json para %s (%r); cayendo a HTML", slug, exc)

    html = await client.get_text(f"/media/{slug}")
    partial = parse_anime_detail_html(html, slug)
    capitulos = [
        Capitulo(numero=e["numero"], url_origen=e["url"])
        for e in partial.get("episodios", [])
    ]
    return Anime(
        id=slug,
        titulo=partial.get("titulo") or slug,
        titulo_alternativo=partial.get("titulo_alternativo"),
        url_origen=f"{SITE_BASE_URL}/media/{slug}",
        generos=[Genero(nombre=g["nombre"], slug=g["slug"]) for g in partial.get("generos", [])],
        temporadas=[Temporada(numero=1, capitulos=capitulos)] if capitulos else [],
        estado=EstadoAnime.DESCONOCIDO,
        estado_raw="",
    )


# ============================================================================
# 8. EPISODE SCRAPER (async + gather)
# ============================================================================

_ZILLA_NETWORKS_PLAY_RE = re.compile(
    r"^https?://player\.zilla-networks\.com/play/(?P<id>[A-Za-z0-9_-]+)/?$",
    re.IGNORECASE,
)


# Tabla extensible: (host, path_prefix_antiguo, path_prefix_nuevo).
# Cuando un proveedor entrega una URL HTML/embed, se reescribe al endpoint
# real del stream antes de persistirla.
_STREAM_HOST_REWRITES: list[tuple[str, str, str]] = [
    # zilla-networks: /play/<id> (pagina HTML con JW Player) -> /m3u8/<id> (HLS)
    ("player.zilla-networks.com", "/play/", "/m3u8/"),
]


def resolve_provider_stream_url(url: Optional[str]) -> tuple[Optional[str], bool]:
    """Si la URL apunta a un reproductor HTML embebido, resuelve la URL del stream real.

    Devuelve ``(url_resuelta, fue_resuelta)``:
      * ``(None, False)`` si la entrada es None o vacia.
      * ``(url, False)`` si la URL no requiere transformacion.
      * ``(url_nueva, True)`` si se aplico una reescritura segun ``_STREAM_HOST_REWRITES``.
    """
    if not url:
        return None, False
    try:
        parsed = urlparse(url)
    except Exception:
        return url, False
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    for host_pattern, old_prefix, new_prefix in _STREAM_HOST_REWRITES:
        if host == host_pattern and path.startswith(old_prefix):
            new_path = new_prefix + path[len(old_prefix):]
            resolved = f"{parsed.scheme or 'https'}://{parsed.netloc}{new_path}"
            if parsed.query:
                resolved += "?" + parsed.query
            if parsed.fragment:
                resolved += "#" + parsed.fragment
            return resolved, True
    return url, False


def _resolve_zilla_hls(url: Optional[str]) -> Optional[str]:
    """Wrapper de compatibilidad: solo devuelve la URL resuelta o la original."""
    resolved, _ = resolve_provider_stream_url(url)
    return resolved


def _clasificar_tipo(server: str, url: str) -> TipoProveedor:
    s = (server or "").lower()
    u = (url or "").lower()
    if any(k in s for k in ("mega", "mp4upload", "1fichier", "transferit")) and "download" in s:
        return TipoProveedor.DOWNLOAD
    # Reglas por URL (tienen prioridad sobre el nombre del server, porque tras
    # resolver zilla-networks el server sigue siendo "HLS" pero la URL ya es m3u8).
    if u.endswith(".mp4") or u.endswith(".mkv") or "/m3u8" in u or u.endswith(".m3u8"):
        return TipoProveedor.DIRECT
    if "mega.nz/embed" in u or "mp4upload.com/embed" in u:
        return TipoProveedor.IFRAME
    if "embed" in u or "/embed" in u or "iframe" in s:
        return TipoProveedor.EMBED
    if "hls" in s or "stream" in s or "player" in s or "play" in s:
        return TipoProveedor.EMBED
    return TipoProveedor.UNKNOWN


def _build_proveedor(server: str, raw_url: str, *, is_download: bool = False) -> Proveedor:
    log = logging.getLogger("animeav1.episode")
    if raw_url is None:
        raw_url = ""
    decoded, metodo = decode_provider_url(raw_url)
    if metodo == MetodoDecodificacion.FAILED.value:
        log.warning(
            "No se pudo decodificar URL del proveedor '%s' (raw=%r)",
            server, raw_url[:120],
        )
    final_url = decoded or raw_url
    resolved_url, was_resolved = resolve_provider_stream_url(final_url)
    if was_resolved:
        # El resolver realizo una transformacion programatica (player HTML -> HLS).
        metodo = MetodoDecodificacion.JS_FUNCTION.value
        final_url = resolved_url
    tipo = _clasificar_tipo(server, final_url)
    if is_download and tipo == TipoProveedor.UNKNOWN:
        tipo = TipoProveedor.DOWNLOAD
    return Proveedor(
        nombre=server or "Unknown",
        tipo=tipo,
        url=final_url or None,
        url_raw=raw_url if raw_url else None,
        metodo_decodificacion=MetodoDecodificacion(metodo),
    )


def _build_providers_list(embeds_dict: dict, *, is_download: bool = False) -> list[Proveedor]:
    out: list[Proveedor] = []
    if not isinstance(embeds_dict, dict):
        return out
    for lang_key, items in embeds_dict.items():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            server = item.get("server") or f"Unknown-{lang_key}"
            raw_url = item.get("url") or ""
            nombre = f"{server} [{lang_key}]" if lang_key else server
            prov = _build_proveedor(nombre, raw_url, is_download=is_download)
            res = item.get("resoluciones") or item.get("quality")
            if res:
                prov.resoluciones = [res] if isinstance(res, str) else list(res)
            out.append(prov)
    return out


def _build_capitulo_from_payload(payload: dict, slug: str, numero: int) -> Capitulo:
    ep = payload.get("episode") if isinstance(payload, dict) else None
    if not isinstance(ep, dict):
        ep = {}
    media = payload.get("media") if isinstance(payload, dict) else None
    sitio_media_id = None
    if isinstance(media, dict):
        sitio_media_id = _safe_int(media.get("id"))
    imagenes: list[str] = []
    if sitio_media_id is not None:
        imagenes.append(f"{CDN_BASE_URL}/screenshots/{sitio_media_id}/{numero}.jpg")
    embeds = payload.get("embeds") if isinstance(payload, dict) else None
    downloads = payload.get("downloads") if isinstance(payload, dict) else None
    proveedores = _build_providers_list(embeds, is_download=False) if embeds else []
    descargas = _build_providers_list(downloads, is_download=True) if downloads else []
    raw_title = ep.get("title")
    titulo = raw_title if isinstance(raw_title, str) and raw_title.strip() else None
    descripcion = None
    raw_desc = ep.get("synopsis") or ep.get("description")
    if isinstance(raw_desc, str) and raw_desc.strip():
        descripcion = raw_desc.strip()
    return Capitulo(
        numero=numero,
        titulo=titulo,
        descripcion=descripcion,
        imagenes=imagenes,
        fecha_publicacion=ep.get("publishedAt"),
        url_origen=f"{SITE_BASE_URL}/media/{slug}/{numero}",
        proveedores=proveedores,
        descargas=descargas,
        filler=bool(ep.get("filler")),
        sitio_id=_safe_int(ep.get("id")),
    )


async def scrape_episode_async(
    client: AsyncHttpClient, slug: str, numero: int
) -> Optional[Capitulo]:
    """Scrapea el detalle de un capitulo (async)."""
    log = logging.getLogger("animeav1.episode")
    path = f"/media/{slug}/{numero}/__data.json"
    try:
        payload = await client.get_json(path)
    except Exception as exc:
        log.error("Fallo descargando episodio %s/%d: %r", slug, numero, exc)
        return Capitulo(
            numero=numero,
            url_origen=f"{SITE_BASE_URL}/media/{slug}/{numero}",
        )
    resolved = resolve_sveltekit_payload(payload)
    if not resolved or not isinstance(resolved, dict):
        log.warning("No se pudo resolver payload de episodio %s/%d", slug, numero)
        return Capitulo(
            numero=numero,
            url_origen=f"{SITE_BASE_URL}/media/{slug}/{numero}",
        )
    cap = _build_capitulo_from_payload(resolved, slug, numero)
    log.info(
        "Episodio %s/%d: %d proveedores, %d descargas",
        slug, numero, len(cap.proveedores), len(cap.descargas),
    )
    return cap


async def fill_episode_providers_async(
    client: AsyncHttpClient, anime: Anime
) -> Anime:
    """Rellena los proveedores de TODOS los capitulos del anime en paralelo.

    Usa asyncio.gather para descargar todos los episodios concurrentemente.
    Respeta el semaforo de concurrencia del cliente.
    """
    log = logging.getLogger("animeav1.episode")
    total_caps = anime.count_capitulos()
    if total_caps == 0:
        log.info("Anime %s no tiene capitulos; nada que rellenar", anime.id)
        return anime

    # Aplanar: lista de (temporada_idx, capitulo_idx, numero)
    tasks_flat: list[tuple[int, int, int]] = []
    for ti, temporada in enumerate(anime.temporadas):
        for ci, cap in enumerate(temporada.capitulos):
            tasks_flat.append((ti, ci, cap.numero))

    log.info(
        "Anime %s: descargando %d capitulos en paralelo...",
        anime.id, len(tasks_flat),
    )
    # Lanzar todos los scrape_episode en paralelo
    numeros = [n for _, _, n in tasks_flat]
    caps_detailed = await asyncio.gather(
        *[scrape_episode_async(client, anime.id, n) for n in numeros],
        return_exceptions=True,
    )

    # Reasignar resultados al anime in-place
    processed = 0
    for (ti, ci, numero), result in zip(tasks_flat, caps_detailed):
        if isinstance(result, Exception):
            log.error("Error en cap %d: %r", numero, result)
            continue
        if result is not None:
            existing = anime.temporadas[ti].capitulos[ci]
            if existing.sitio_id and not result.sitio_id:
                result.sitio_id = existing.sitio_id
            anime.temporadas[ti].capitulos[ci] = result
            processed += 1

    log.info(
        "Anime %s: detalle de episodios completo (%d/%d)",
        anime.id, processed, total_caps,
    )
    return anime


# ============================================================================
# 9. HOME SCRAPER (async)
# ============================================================================

def _is_today(iso_or_ts: Optional[str]) -> bool:
    if not iso_or_ts:
        return False
    s = iso_or_ts.strip()
    try:
        if "T" not in s:
            s = s.replace(" ", "T", 1)
        dt = datetime.fromisoformat(s)
    except Exception:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - dt).total_seconds() < 24 * 3600


async def scrape_today_episodes_async(client: AsyncHttpClient) -> list[dict]:
    """Devuelve los episodios publicados hoy segun el home (async)."""
    log = logging.getLogger("animeav1.home")
    try:
        payload = await client.get_json("/__data.json")
    except Exception as exc:
        log.error("Fallo descargando home __data.json: %r", exc)
        return []

    resolved = resolve_sveltekit_payload(payload)
    if not resolved or not isinstance(resolved, dict):
        log.warning("No se pudo resolver payload del home")
        return []

    latest = resolved.get("latestEpisodes") or []
    out: list[dict] = []
    for ep in latest:
        if not isinstance(ep, dict):
            continue
        media = ep.get("media") if isinstance(ep.get("media"), dict) else {}
        slug = media.get("slug")
        numero = _safe_int(ep.get("number"))
        if not slug or numero is None:
            continue
        published_at = ep.get("publishedAt") or ep.get("createdAt")
        out.append({
            "media_id": _safe_int(media.get("id")),
            "media_slug": slug,
            "media_titulo": media.get("title") or "",
            "numero": numero,
            "episode_id": _safe_int(ep.get("id")),
            "published_at": published_at,
            "is_today": _is_today(published_at),
            "comments_count": _safe_int(ep.get("commentsCount")),
            "url_origen": f"{SITE_BASE_URL}/media/{slug}/{numero}",
        })

    log.info(
        "Home: %d episodios recientes (%d son de hoy)",
        len(out), sum(1 for e in out if e["is_today"]),
    )
    return out


async def scrape_latest_media_async(client: AsyncHttpClient) -> list[dict]:
    log = logging.getLogger("animeav1.home")
    try:
        payload = await client.get_json("/__data.json")
    except Exception as exc:
        log.error("Fallo descargando home __data.json: %r", exc)
        return []
    resolved = resolve_sveltekit_payload(payload)
    if not resolved or not isinstance(resolved, dict):
        return []
    latest = resolved.get("latestMedia") or []
    out = []
    for m in latest:
        if not isinstance(m, dict):
            continue
        cat = m.get("category") if isinstance(m.get("category"), dict) else {}
        out.append({
            "sitio_id": _safe_int(m.get("id")),
            "slug": m.get("slug"),
            "titulo": m.get("title") or "",
            "descripcion": m.get("synopsis"),
            "categoria": cat.get("name") if cat else None,
            "url_origen": f"{SITE_BASE_URL}/media/{m.get('slug')}" if m.get("slug") else None,
            "created_at": m.get("createdAt"),
        })
    return out


# ============================================================================
# 10. STORAGE: JSONL unificado (sin base de datos)
# ============================================================================
#
# JSONL unificado (data/animes.jsonl):
#   - 1 anime por linea (compacto, sin indentar).
#   - Estructura jerarquica completa:
#       Anime -> [Temporadas] -> [Capitulos] -> [Proveedores]
#   - Upsert por slug: si el slug ya existe, se reemplaza la linea.
#   - Se mantiene un indice (data/animes.index.json) que mapea slug -> presente
#     para membership check en O(1).
#   - NO se generan archivos individuales por anime: todo esta en este JSONL.
#
# Indices de consulta rapida:
#   - ``data/animes.index.json``: {slug: true}
#   - ``data/errors.jsonl``: log de errores (1 por linea)
#
# Validacion de jerarquia:
#   - ``--mode validate`` genera ``data/hierarchy_report.json`` consolidado con
#     el reporte de integridad de TODOS los animes del JSONL.
#   - ``Anime.validate_hierarchy()`` valida la jerarquia
#     Anime -> [Temporadas] -> [Capitulos] -> [Proveedores] en cada save.


# --- JSONL unificado ---

def _load_jsonl_index() -> dict[str, bool]:
    """Carga el indice {slug: true} en memoria leyendo el JSONL unificado."""
    index = {}
    if not ANIMES_JSONL_PATH.exists():
        return index
    try:
        with ANIMES_JSONL_PATH.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    if isinstance(d, dict) and d.get("anime", {}).get("id"):
                        index[d["anime"]["id"]] = True
                except json.JSONDecodeError:
                    continue
    except Exception:
        pass
    return index


def _save_jsonl_index(index: dict[str, bool]) -> None:
    """No-op (el indice se carga en memoria desde el JSONL)."""
    pass


def save_anime_jsonl(anime: Anime) -> Path:
    """Hace upsert de un anime en el JSONL unificado.

    Estrategia:
      - Si el slug ya esta en el indice, sobrescribe la linea correspondiente.
      - Si es nuevo, agrega al final del JSONL.
      - Actualiza el indice.
      - Ejecuta validate_hierarchy y registra cualquier issue en el log
        (NO genera archivos individuales por anime).

    Args:
        anime: el Anime a persistir.

    Returns:
        La ruta del JSONL.
    """
    log = logging.getLogger("animeav1.storage")
    ANIMES_JSONL_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Validar jerarquia antes de serializar (solo para log, no genera archivos)
    report = anime.validate_hierarchy()
    if report["issues"]:
        log.warning(
            "Anime %s: %d problemas de integridad jerarquica detectados:",
            anime.id, len(report["issues"]),
        )
        for issue in report["issues"][:10]:
            log.warning("  - %s", issue)
        if len(report["issues"]) > 10:
            log.warning("  ... y %d mas", len(report["issues"]) - 10)
    else:
        log.debug(
            "Anime %s: jerarquia OK (%d temp, %d caps, %d prov stream, %d dl)",
            anime.id, report["temporadas"], report["capitulos"],
            report["proveedores_stream"], report["proveedores_download"],
        )

    # Serializar como 1 linea compacta (JSONL = JSON Lines)
    doc = AnimeDocument(anime=anime)
    line = doc.model_dump_json(indent=None, exclude_none=False)

    # Cargar indice
    index = _load_jsonl_index()

    # Upsert: si el slug existe, reemplazamos la linea; si no, append.
    if anime.id in index:
        all_lines: list[str] = []
        if ANIMES_JSONL_PATH.exists():
            all_lines = ANIMES_JSONL_PATH.read_text(encoding="utf-8").splitlines()
        replaced = False
        for i, l in enumerate(all_lines):
            try:
                d = json.loads(l)
                if isinstance(d, dict) and d.get("anime", {}).get("id") == anime.id:
                    all_lines[i] = line
                    replaced = True
                    break
            except json.JSONDecodeError:
                continue
        if not replaced:
            all_lines.append(line)
        ANIMES_JSONL_PATH.write_text(
            "\n".join(all_lines) + "\n", encoding="utf-8"
        )
    else:
        with ANIMES_JSONL_PATH.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        index[anime.id] = True
        _save_jsonl_index(index)

    log.debug("Anime %s guardado en JSONL (linea: %d bytes)", anime.id, len(line))
    return ANIMES_JSONL_PATH


def load_anime_from_jsonl(slug: str) -> Optional[Anime]:
    """Carga un anime concreto desde el JSONL unificado."""
    if not ANIMES_JSONL_PATH.exists():
        return None
    with ANIMES_JSONL_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
                if isinstance(d, dict) and d.get("anime", {}).get("id") == slug:
                    return AnimeDocument(**d).anime
            except json.JSONDecodeError:
                continue
    return None


def iter_animes_from_jsonl():
    """Iterador generador sobre todos los animes del JSONL."""
    if not ANIMES_JSONL_PATH.exists():
        return
    with ANIMES_JSONL_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
                if isinstance(d, dict) and "anime" in d:
                    yield AnimeDocument(**d).anime
            except json.JSONDecodeError:
                continue


# --- Log de errores (JSONL append-only) ---

def log_error(scope: str, msg: str, *, severity: str = "ERROR") -> None:
    """Registra un error en data/errors.jsonl (1 linea por error)."""
    try:
        ERRORS_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with ERRORS_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps({
                "ts": datetime.now(timezone.utc).isoformat(),
                "scope": scope,
                "severity": severity,
                "msg": msg[:2000],
            }, ensure_ascii=False) + "\n")
    except Exception as exc:
        logging.getLogger("animeav1.storage").warning(
            "No se pudo escribir error en errors.jsonl: %r", exc
        )


# --- Helpers de guardado ---

async def save_anime_async(anime: Anime, args) -> None:
    """Persiste el anime en el JSONL unificado y en Turso.

    - save_anime_jsonl() ya hace upsert por slug y ejecuta validate_hierarchy.
    - Además se guarda en la base de datos Turso via database.py.
    """
    no_json = getattr(args, "no_json", False)
    if not no_json:
        # save_anime_jsonl es sync; ejecutar en thread pool para no bloquear
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, save_anime_jsonl, anime)
        
    try:
        from database import save_anime_to_turso
        await save_anime_to_turso(anime)
        logging.getLogger("animeav1.storage").info("Anime %s guardado en Turso exitosamente.", anime.id)
    except Exception as e:
        logging.getLogger("animeav1.storage").error("Error guardando %s en Turso: %s", anime.id, e)

# --- Helpers de consulta basados en JSONL ---

def _chapter_exists_in_jsonl(slug: str, numero: int) -> bool:
    """Comprueba si un capitulo (slug + numero) ya existe en el JSONL.

    Usa el indice de slugs para saltear el analisis del JSONL cuando el anime
    no esta presente (caso comun en el watcher para animes nuevos)."""
    index = _load_jsonl_index()
    if slug not in index:
        return False
    anime = load_anime_from_jsonl(slug)
    if anime is None:
        return False
    for temporada in anime.temporadas:
        for cap in temporada.capitulos:
            if cap.numero == numero:
                return True
    return False


def _anime_exists_in_jsonl(slug: str) -> bool:
    """Comprueba si un anime (por slug) ya existe en el JSONL."""
    return slug in _load_jsonl_index()


def jsonl_stats() -> dict:
    """Calcula estadisticas agregadas a partir del JSONL unificado.
    Reemplaza a la antigua db_stats() que consultaba SQLite."""
    counts = {
        "animes": 0,
        "temporadas": 0,
        "capitulos": 0,
        "proveedores_stream": 0,
        "proveedores_download": 0,
        "generos": 0,
        "metodos_decodificacion": {},
        "errores": 0,
    }
    for anime in iter_animes_from_jsonl():
        counts["animes"] += 1
        counts["temporadas"] += len(anime.temporadas)
        counts["capitulos"] += anime.count_capitulos()
        counts["generos"] += len(anime.generos)
        for t in anime.temporadas:
            for c in t.capitulos:
                counts["proveedores_stream"] += len(c.proveedores)
                counts["proveedores_download"] += len(c.descargas)
                for p in c.proveedores + c.descargas:
                    key = p.metodo_decodificacion.value if p.metodo_decodificacion else "null"
                    counts["metodos_decodificacion"][key] = counts["metodos_decodificacion"].get(key, 0) + 1
    if ERRORS_LOG_PATH.exists():
        with ERRORS_LOG_PATH.open("r", encoding="utf-8") as f:
            counts["errores"] = sum(1 for _ in f)
    return counts


# ============================================================================
# 11. WATCHER DAEMON (async)
# ============================================================================

async def upsert_episode_incremental_async(
    client: AsyncHttpClient, slug: str, numero: int, *, args
) -> dict:
    """Actualiza UN capitulo concreto sin re-scrapear todo el anime (async)."""
    log = logging.getLogger("animeav1.watcher")
    result = {
        "action": "unknown", "slug": slug, "numero": numero,
        "proveedores": 0, "descargas": 0, "ok": False, "error": None,
    }

    # 1) Cargar anime local (desde JSONL unificado)
    anime = load_anime_from_jsonl(slug)
    if anime is None:
        log.info("[watcher] Anime nuevo detectado: %s. Scrapeando completo...", slug)
        try:
            anime = await scrape_anime_async(client, slug)
            if anime is None:
                result["error"] = "no se pudo scrapear el anime"
                return result
            if not getattr(args, "no_episodes", False) and anime.count_capitulos() > 0:
                await fill_episode_providers_async(client, anime)
            await save_anime_async(anime, args)
            result["action"] = "anime_new_full"
            result["ok"] = True
            result["proveedores"] = sum(
                len(c.proveedores) for t in anime.temporadas for c in t.capitulos
            )
            result["descargas"] = sum(
                len(c.descargas) for t in anime.temporadas for c in t.capitulos
            )
            return result
        except Exception as exc:
            result["error"] = repr(exc)
            log.exception("[watcher] Error scrapeando anime nuevo %s", slug)
            return result

    # 2) Anime ya existe: comprobar si el capitulo ya esta
    if _chapter_exists_in_jsonl(slug, numero):
        log.debug("[watcher] Capitulo %s/%d ya en JSONL, saltando", slug, numero)
        result["action"] = "chapter_skip_existing"
        result["ok"] = True
        return result

    # 3) Scrapear solo el capitulo nuevo
    log.info("[watcher] Capitulo nuevo: %s/%d. Scrapeando detalle...", slug, numero)
    try:
        cap = await scrape_episode_async(client, slug, numero)
        if cap is None:
            result["error"] = "scrape_episode devolvio None"
            return result

        # Insertar/actualizar el capitulo en el anime
        replaced = False
        for temporada in anime.temporadas:
            for i, existing in enumerate(temporada.capitulos):
                if existing.numero == numero:
                    temporada.capitulos[i] = cap
                    replaced = True
                    break
            if replaced:
                break
        if not replaced:
            if not anime.temporadas:
                anime.temporadas = [Temporada(numero=1, capitulos=[cap])]
            else:
                t1 = next((t for t in anime.temporadas if t.numero == 1), anime.temporadas[0])
                t1.capitulos.append(cap)

        await save_anime_async(anime, args)
        result["action"] = "chapter_upsert"
        result["ok"] = True
        result["proveedores"] = len(cap.proveedores)
        result["descargas"] = len(cap.descargas)
        log.info(
            "[watcher] Capitulo %s/%d actualizado (%d prov, %d dl)",
            slug, numero, len(cap.proveedores), len(cap.descargas),
        )
        return result
    except Exception as exc:
        result["error"] = repr(exc)
        log.exception("[watcher] Error actualizando capitulo %s/%d", slug, numero)
        return result


async def watch_home_once_async(client: AsyncHttpClient, *, args) -> dict:
    """Ejecuta un ciclo del watcher en paralelo (async).

    Descarga el home y para cada episodio llama a upsert_episode_incremental
    concurrentemente con asyncio.gather.
    """
    log = logging.getLogger("animeav1.watcher")
    started = time.time()

    try:
        episodes = await scrape_today_episodes_async(client)
    except Exception as exc:
        log.exception("[watcher] Error descargando home")
        return {
            "animes_nuevos": 0, "capitulos_nuevos": 0,
            "capitulos_saltados": 0, "errores": [repr(exc)],
            "duracion_segundos": round(time.time() - started, 2),
            "ok": False,
        }

    if not episodes:
        log.info("[watcher] Home sin episodios recientes")
        return {
            "animes_nuevos": 0, "capitulos_nuevos": 0,
            "capitulos_saltados": 0, "errores": [],
            "duracion_segundos": round(time.time() - started, 2),
            "ok": True, "episodios_home": 0,
        }

    log.info("[watcher] Iniciando ciclo: %d episodios en el home", len(episodes))

    # Lanzar todos los upserts en paralelo (respetando el semaforo del cliente)
    tasks = []
    for ep in episodes:
        slug = ep.get("media_slug")
        numero = ep.get("numero")
        if not slug or numero is None:
            continue
        tasks.append((slug, numero, ep.get("published_at"), upsert_episode_incremental_async(client, slug, numero, args=args)))

    results = await asyncio.gather(*[t[3] for t in tasks], return_exceptions=True)

    animes_nuevos = 0
    capitulos_nuevos = 0
    capitulos_saltados = 0
    errores: list[str] = []
    for (slug, numero, published_at, _), r in zip(tasks, results):
        if isinstance(r, Exception):
            errores.append(f"{slug}/{numero}: {r!r}")
            continue
        if r["action"] == "anime_new_full":
            animes_nuevos += 1
            capitulos_nuevos += 1
        elif r["action"] == "chapter_upsert":
            capitulos_nuevos += 1
        elif r["action"] == "chapter_skip_existing":
            capitulos_saltados += 1
        if not r["ok"] and r["error"]:
            errores.append(f"{slug}/{numero}: {r['error']}")

    resumen = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "ok": len(errores) == 0,
        "episodios_home": len(episodes),
        "animes_nuevos": animes_nuevos,
        "capitulos_nuevos": capitulos_nuevos,
        "capitulos_saltados": capitulos_saltados,
        "errores": errores,
        "duracion_segundos": round(time.time() - started, 2),
    }

    log.info(
        "[watcher] Ciclo completado en %.1fs | animes nuevos=%d | caps nuevos=%d | caps saltados=%d | errores=%d",
        resumen["duracion_segundos"], animes_nuevos, capitulos_nuevos,
        capitulos_saltados, len(errores),
    )
    return resumen


async def run_watch_async(args) -> int:
    """Daemon async que ejecuta watch_home_once_async cada N segundos."""
    log = logging.getLogger("animeav1.watcher")
    interval = getattr(args, "watch_interval", 1800)
    once = getattr(args, "watch_once", False)
    log.info("[watcher] Daemon async iniciado | intervalo=%ds | once=%s | concurrency=%d",
             interval, once, getattr(args, "concurrency", DEFAULT_CONCURRENCY))

    ciclo = 0
    async with build_async_http_client(args) as client:
        while True:
            ciclo += 1
            log.info("[watcher] === Ciclo #%d ===", ciclo)
            try:
                await watch_home_once_async(client, args=args)
            except KeyboardInterrupt:
                raise
            except Exception:
                log.exception("[watcher] Error en ciclo #%d", ciclo)

            if once:
                log.info("[watcher] --watch-once activo, saliendo tras 1 ciclo")
                return 0

            remaining = interval
            log.info("[watcher] Siguiente ciclo en %d segundos...", remaining)
            while remaining > 0:
                step = min(10, remaining)
                await asyncio.sleep(step)
                remaining -= step


# ============================================================================
# 12. MAIN / CLI (async)
# ============================================================================

async def save_anime(anime: Anime, args) -> None:
    """Wrapper async para guardar anime en JSONL + SQLite."""
    await save_anime_async(anime, args)


async def run_full_async(args) -> dict:
    """Catalogo completo + detalle + episodios, todo en paralelo."""
    log = logging.getLogger("animeav1.main")
    started = time.time()
    errors: list[str] = []
    stats = {"animes": 0, "temporadas": 0, "capitulos": 0, "proveedores": 0}

    async with build_async_http_client(args) as client:
        robots = await client.fetch_robots_txt()
        log.info("robots.txt: %d bytes", len(robots))

        animes_meta = await scrape_all_animes_async(
            client, max_pages=args.max_pages, limit=args.limit
        )
        log.info("Catalogo obtenido: %d animes", len(animes_meta))

        # Procesar todos los animes en paralelo
        semaphore = asyncio.Semaphore(getattr(args, "concurrency", DEFAULT_CONCURRENCY))

        async def process_one(meta: dict, idx: int, total: int) -> tuple[Anime, dict]:
            async with semaphore:
                slug = meta.get("slug") or meta.get("id")
                log.info("[%d/%d] Procesando anime: %s", idx, total, slug)
                anime = await scrape_anime_async(client, slug)
                if anime is None:
                    raise RuntimeError(f"anime {slug}: sin datos")
                if not anime.descripcion and meta.get("descripcion"):
                    anime.descripcion = meta["descripcion"]
                if not anime.foto_portada and meta.get("foto_portada"):
                    anime.foto_portada = meta["foto_portada"]
                if not anime.categoria and meta.get("categoria"):
                    anime.categoria = meta["categoria"]
                if not getattr(args, "no_episodes", False) and anime.count_capitulos() > 0:
                    await fill_episode_providers_async(client, anime)
                await save_anime_async(anime, args)
                return anime, meta

        total = len(animes_meta)
        tasks = [process_one(meta, i, total) for i, meta in enumerate(animes_meta, 1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                log.error("Error procesando anime: %r", r)
                errors.append(repr(r))
                log_error("anime", repr(r))
                continue
            anime, _ = r
            stats["animes"] += 1
            stats["temporadas"] += len(anime.temporadas)
            stats["capitulos"] += anime.count_capitulos()
            stats["proveedores"] += anime.count_proveedores()

    stats["elapsed_seconds"] = round(time.time() - started, 2)
    stats["errors"] = errors
    stats["warnings_count"] = len(errors)
    return stats


async def run_catalog_async(args) -> dict:
    """Solo listado de animes (async)."""
    started = time.time()
    log = logging.getLogger("animeav1.main")
    async with build_async_http_client(args) as client:
        animes = await scrape_all_animes_async(
            client, max_pages=args.max_pages, limit=args.limit
        )
    cat_path = DATA_DIR / "catalog.json"
    with cat_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "total": len(animes),
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "animes": animes,
            },
            f, ensure_ascii=False, indent=2,
        )
    log.info("Catalogo guardado en %s (%d animes)", cat_path, len(animes))
    return {"animes": len(animes), "elapsed_seconds": round(time.time() - started, 2)}


async def run_today_async(args) -> dict:
    """Solo episodios del dia (home, async)."""
    started = time.time()
    log = logging.getLogger("animeav1.main")
    async with build_async_http_client(args) as client:
        episodes = await scrape_today_episodes_async(client)
        latest_media = await scrape_latest_media_async(client)
    out_path = DATA_DIR / "today.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "episodes_today": [e for e in episodes if e.get("is_today")],
                "episodes_latest": episodes,
                "latest_media": latest_media,
            },
            f, ensure_ascii=False, indent=2,
        )
    log.info("Home guardado en %s (%d episodios, %d hoy)",
             out_path, len(episodes), sum(1 for e in episodes if e.get("is_today")))
    return {
        "episodes_total": len(episodes),
        "episodes_today": sum(1 for e in episodes if e.get("is_today")),
        "latest_media": len(latest_media),
        "elapsed_seconds": round(time.time() - started, 2),
    }


async def run_anime_async(args) -> dict:
    """Un anime concreto (URL o slug, async)."""
    started = time.time()
    log = logging.getLogger("animeav1.main")
    target = args.anime_url
    if not target:
        log.error("--mode anime requiere un argumento <url|slug>")
        return {"error": "missing anime argument"}

    async with build_async_http_client(args) as client:
        anime = await scrape_anime_async(client, target)
        if anime is None:
            log.error("No se pudo scrapear el anime: %s", target)
            return {"error": "anime not found"}
        if not getattr(args, "no_episodes", False) and anime.count_capitulos() > 0:
            await fill_episode_providers_async(client, anime)
        await save_anime_async(anime, args)

    return {
        "anime": anime.id,
        "temporadas": len(anime.temporadas),
        "capitulos": anime.count_capitulos(),
        "proveedores": anime.count_proveedores(),
        "elapsed_seconds": round(time.time() - started, 2),
    }


def run_validate(args) -> dict:
    """Recorre todos los animes del JSONL unificado y valida su jerarquia."""
    log = logging.getLogger("animeav1.main")
    if not ANIMES_JSONL_PATH.exists():
        log.warning("No existe %s", ANIMES_JSONL_PATH)
        return {"animes_revisados": 0}

    total = {"animes": 0, "temporadas": 0, "capitulos": 0,
             "proveedores_stream": 0, "proveedores_download": 0,
             "issues_total": 0, "animes_con_issues": 0}
    per_anime: list[dict] = []

    for anime in iter_animes_from_jsonl():
        report = anime.validate_hierarchy()
        total["animes"] += 1
        total["temporadas"] += report["temporadas"]
        total["capitulos"] += report["capitulos"]
        total["proveedores_stream"] += report["proveedores_stream"]
        total["proveedores_download"] += report["proveedores_download"]
        total["issues_total"] += len(report["issues"])
        if report["issues"]:
            total["animes_con_issues"] += 1
        per_anime.append({
            "anime_id": report["anime_id"],
            "ok": len(report["issues"]) == 0,
            "temporadas": report["temporadas"],
            "capitulos": report["capitulos"],
            "proveedores_stream": report["proveedores_stream"],
            "proveedores_download": report["proveedores_download"],
            "issues_count": len(report["issues"]),
            "issues": report["issues"][:5],
        })

    report_path = DATA_DIR / "hierarchy_report.json"
    with report_path.open("w", encoding="utf-8") as f:
        json.dump({"total": total, "per_anime": per_anime}, f, ensure_ascii=False, indent=2)
    log.info("Reporte de jerarquia guardado en %s", report_path)
    log.info(
        "Resumen: %d animes | %d temporadas | %d capitulos | %d prov stream | %d descargas",
        total["animes"], total["temporadas"], total["capitulos"],
        total["proveedores_stream"], total["proveedores_download"],
    )
    log.info(
        "Animes con issues: %d / %d | Total issues: %d",
        total["animes_con_issues"], total["animes"], total["issues_total"],
    )
    return total


async def run_full_and_watch_async(args) -> int:
    """Scrapeo completo + daemon async."""
    log = logging.getLogger("animeav1.main")
    log.info("=== full-and-watch: iniciando scrapeo completo (async) ===")
    stats = await run_full_async(args)
    log.info("=== full-and-watch: scrapeo completo terminado ===")
    log.info("  animes=%d capitulos=%d proveedores=%d errores=%d",
             stats.get("animes", 0), stats.get("capitulos", 0),
             stats.get("proveedores", 0), stats.get("warnings_count", 0))
    log.info("=== full-and-watch: arrancando watcher daemon async ===")
    return await run_watch_async(args)


def run_stats(args) -> dict:
    """Calcula estadisticas a partir del JSONL unificado."""
    return jsonl_stats()


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="animeav1-scraper",
        description="Scraper ASINCRONO modular para animeav1.com (archivo unico)",
    )
    p.add_argument(
        "--mode",
        choices=[
            "full-and-watch", "full", "catalog", "today", "anime",
            "stats", "validate", "watch",
        ],
        default="full-and-watch",
        help=(
            "Modo de ejecucion (default: full-and-watch). Scrapea el catalogo "
            "completo y luego arranca el daemon watcher cada 30 min. "
            "Otros modos: 'full' (solo catalogo), 'watch' (solo daemon), "
            "'catalog'/'today'/'anime' (consultas puntuales), 'stats' "
            "(estadisticas del JSONL), 'validate' (valida jerarquia)."
        ),
    )
    p.add_argument("anime_url", nargs="?", help="URL o slug (solo modo anime)")
    p.add_argument("--limit", type=int, default=None, help="Limitar a N animes")
    p.add_argument("--max-pages", type=int, default=None, help="Tope de paginas")
    p.add_argument("--no-episodes", action="store_true", help="Saltar detalle de episodios")
    p.add_argument("--no-json", action="store_true", help="No escribir JSONL (solo memoria)")
    p.add_argument("--rate-min", type=float, default=RATE_LIMIT_MIN_SECONDS)
    p.add_argument("--rate-max", type=float, default=RATE_LIMIT_MAX_SECONDS)
    p.add_argument(
        "--concurrency", type=int, default=DEFAULT_CONCURRENCY,
        help=f"Peticiones HTTP simultaneas (default: {DEFAULT_CONCURRENCY})",
    )
    p.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    # --- Watcher ---
    p.add_argument(
        "--watch-interval", type=int, default=1800,
        help="Intervalo en segundos entre ciclos del watcher (default: 1800 = 30 min)",
    )
    p.add_argument(
        "--watch-once", action="store_true",
        help="Ejecutar solo un ciclo del watcher y salir",
    )
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    ensure_dirs()
    args = parse_args(argv)
    setup_logging(LOG_FILE, level=getattr(logging, args.log_level))
    log = logging.getLogger("animeav1.main")

    log.info("=== animeav1 scraper ASYNC :: mode=%s concurrency=%d ===",
             args.mode, args.concurrency)
    started = time.time()

    # Modos async
    if args.mode in ("full", "catalog", "today", "anime", "watch", "full-and-watch"):
        try:
            if args.mode == "full":
                stats = asyncio.run(run_full_async(args))
            elif args.mode == "catalog":
                stats = asyncio.run(run_catalog_async(args))
            elif args.mode == "today":
                stats = asyncio.run(run_today_async(args))
            elif args.mode == "anime":
                stats = asyncio.run(run_anime_async(args))
            elif args.mode == "watch":
                return asyncio.run(run_watch_async(args))
            elif args.mode == "full-and-watch":
                return asyncio.run(run_full_and_watch_async(args))
        except KeyboardInterrupt:
            log.warning("Interrumpido por el usuario")
            return 130
        except Exception:
            log.exception("Error fatal en modo %s", args.mode)
            return 1

        elapsed = round(time.time() - started, 2)
        log.info("=== Reporte final (mode=%s) ===", args.mode)
        log.info("Tiempo total: %.2fs", elapsed)
        for k, v in stats.items():
            if k == "errors" and isinstance(v, list):
                log.info("Errores (%d):", len(v))
                for e in v[:20]:
                    log.info("  - %s", e)
            else:
                log.info("%s: %s", k, v)
        return 0

    # Modos sync
    try:
        if args.mode == "stats":
            stats = run_stats(args)
        elif args.mode == "validate":
            stats = run_validate(args)
        else:
            log.error("Modo desconocido: %s", args.mode)
            return 2
    except KeyboardInterrupt:
        log.warning("Interrumpido por el usuario")
        return 130
    except Exception:
        log.exception("Error fatal en modo %s", args.mode)
        return 1

    elapsed = round(time.time() - started, 2)
    log.info("=== Reporte final (mode=%s) ===", args.mode)
    log.info("Tiempo total: %.2fs", elapsed)
    for k, v in stats.items():
        log.info("%s: %s", k, v)
    return 0


if __name__ == "__main__":
    sys.exit(main())
