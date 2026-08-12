import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
import uvicorn

from database import init_db, count_animes, get_all_animes_basic
from animeav1_scraper import run_full_and_watch_async, parse_args

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicializar base de datos de Turso
    await init_db()
    
    # Preparamos los argumentos por defecto (full-and-watch)
    args = parse_args([])
    
    logging.getLogger("animeav1.api").info("Arrancando el scraper AnimeAV1 en segundo plano...")
    # Lanzar la tarea principal del scraper (raspado completo + watcher)
    task = asyncio.create_task(run_full_and_watch_async(args))
    
    yield
    
    # Cancelar la tarea al apagar
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(title="AnimeAV1 API Scraper", lifespan=lifespan)

@app.get("/")
def home():
    return {"status": "online", "message": "API de AnimeAV1 funcionando con auto-scraping a Turso."}

@app.get("/api/animes")
async def get_animes():
    data = await get_all_animes_basic()
    return data

@app.get("/api/stats")
async def get_stats():
    count = await count_animes()
    return {"animes_en_turso": count}

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8001, reload=True)
