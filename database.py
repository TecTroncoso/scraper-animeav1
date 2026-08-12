import os
import json
import libsql_client
from dotenv import load_dotenv

load_dotenv()

TURSO_URL = os.getenv("TURSO_DATABASE_URL", "file:animeav1_local.db")
TURSO_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "")

def get_client():
    if "file:" in TURSO_URL:
        return libsql_client.create_client(TURSO_URL)
    else:
        return libsql_client.create_client(TURSO_URL, auth_token=TURSO_TOKEN)

async def init_db():
    async with get_client() as client:
        await client.execute("""
            CREATE TABLE IF NOT EXISTS content (
                slug TEXT PRIMARY KEY,
                content_type TEXT NOT NULL,
                title TEXT NOT NULL,
                poster TEXT,
                total_episodes INTEGER DEFAULT 1,
                details TEXT NOT NULL
            )
        """)

async def save_anime_to_turso(anime_obj):
    """
    Recibe el objeto Anime de pydantic (animeav1_scraper.py), 
    lo mapea al esquema de Hentaila y lo inserta en Turso.
    """
    # Mapeo idéntico al del importer.py
    slug = anime_obj.id
    titulo = anime_obj.titulo
    portada = anime_obj.foto_portada or ""
    descripcion = anime_obj.descripcion or ""
    estado = anime_obj.estado.value if hasattr(anime_obj.estado, "value") else str(anime_obj.estado)
    
    categorias = [g.nombre for g in anime_obj.generos if g.nombre]
    
    capitulos_mapped = []
    total_episodes = 0
    
    for temp in anime_obj.temporadas:
        for cap in temp.capitulos:
            proveedores = [{"nombre": p.nombre, "url": p.url or p.url_raw} for p in cap.proveedores]
            descargas = [{"nombre": d.nombre, "url": d.url or d.url_raw} for d in cap.descargas]
            
            capitulos_mapped.append({
                "numero": cap.numero,
                "titulo": cap.titulo or f"Episodio {cap.numero}",
                "url": cap.url_origen,
                "proveedores": proveedores,
                "descargas": descargas
            })
            total_episodes += 1
            
    details = {
        "slug": slug,
        "titulo": titulo,
        "portada": portada,
        "descripcion": descripcion,
        "backdrop": "",  
        "estado": estado,
        "categorias": categorias,
        "capitulos": capitulos_mapped
    }
    
    details_json = json.dumps(details, ensure_ascii=False)
    
    async with get_client() as client:
        await client.execute(
            """
            INSERT INTO content (slug, content_type, title, poster, total_episodes, details)
            VALUES (?, 'anime', ?, ?, ?, ?)
            ON CONFLICT(slug) DO UPDATE SET
                title=excluded.title,
                poster=excluded.poster,
                total_episodes=excluded.total_episodes,
                details=excluded.details
            """,
            (slug, titulo, portada, total_episodes, details_json)
        )

async def count_animes():
    async with get_client() as client:
        rs = await client.execute("SELECT COUNT(*) FROM content WHERE content_type = 'anime'")
        return rs.rows[0][0]

async def get_all_animes_basic():
    async with get_client() as client:
        rs = await client.execute("""
            SELECT title, slug, poster, total_episodes 
            FROM content 
            WHERE content_type = 'anime'
        """)
        return [
            {
                "titulo": row[0],
                "slug": row[1],
                "portada": row[2],
                "capitulos_total": row[3]
            }
            for row in rs.rows
        ]
