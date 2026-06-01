"""
Servicio de Cache - Intercepta consultas y las resuelve usando Redis.
Si hay cache hit retorna directamente; si no, delega al Generador de Respuestas.
Registra todas las metricas enviandolas via HTTP al servicio de Metricas.
Soporta politicas LRU, LFU y FIFO con TTL configurable por tipo de consulta.
"""
import os
import time
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Any
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .cache import CacheClient

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [cache-svc] %(message)s")
log = logging.getLogger(__name__)

# Variables de entorno
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
RESPONSE_GEN_URL = os.getenv("RESPONSE_GEN_URL",
                             "http://generador_respuestas:5001")
METRICAS_URL = os.getenv("METRICAS_URL", "http://metricas:5002")

# TTL por tipo de consulta
TTL_BY_QUERY = {
    "Q1": int(os.getenv("TTL_Q1", "300")),
    "Q2": int(os.getenv("TTL_Q2", "300")),
    "Q3": int(os.getenv("TTL_Q3", "180")),
    "Q4": int(os.getenv("TTL_Q4", "120")),
    "Q5": int(os.getenv("TTL_Q5", "600")),
}

# Clientes globales
cache: CacheClient | None = None
http: httpx.AsyncClient | None = None


def _build_cache_key(query_type: str, params: dict[str, Any]) -> str:
    """Genera la cache key segun el formato del enunciado."""
    qt = query_type.upper()
    if qt == "Q1":
        return f"count:{params['zone_id']}:conf={params.get('confidence_min', 0.0):.2f}"
    if qt == "Q2":
        return f"area:{params['zone_id']}:conf={params.get('confidence_min', 0.0):.2f}"
    if qt == "Q3":
        return f"density:{params['zone_id']}:conf={params.get('confidence_min', 0.0):.2f}"
    if qt == "Q4":
        return (
            f"compare:density:{params['zone_a']}:{params['zone_b']}"
            f":conf={params.get('confidence_min', 0.0):.2f}"
        )
    if qt == "Q5":
        return f"confidence_dist:{params['zone_id']}:bins={int(params.get('bins', 5))}"
    raise ValueError(f"Tipo de consulta desconocido: {query_type}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global cache, http
    cache = CacheClient(REDIS_HOST, REDIS_PORT)
    http = httpx.AsyncClient(timeout=30.0)
    log.info("Cache Service listo")
    yield
    await http.aclose()


app = FastAPI(title="Cache API", lifespan=lifespan)


class QueryRequest(BaseModel):
    query_type: str
    params: dict[str, Any] = Field(default_factory=dict)
    client_id: str | None = None


async def _send_metric(event: dict):
    """Envia un evento de metrica al servicio de metricas (fire-and-forget)."""
    try:
        await http.post(f"{METRICAS_URL}/event", json=event, timeout=2.0)
    except Exception as e:
        log.debug(f"Metrics post failed: {e}")


@app.get("/health")
async def health():
    return {"status": "ok",
            "policy": cache.policy if cache else None}


@app.get("/stats")
async def stats():
    if cache is None:
        raise HTTPException(503, "Cache no inicializado")
    return cache.stats()


@app.post("/flush")
async def flush():
    if cache is None:
        raise HTTPException(503, "Cache no inicializado")
    cache.flushall()
    return {"status": "flushed"}


@app.post("/query")
async def query(req: QueryRequest):
    if cache is None or http is None:
        raise HTTPException(503, "Servicio no inicializado")

    t_total_start = time.perf_counter()

    try:
        key = _build_cache_key(req.query_type, req.params)
    except (KeyError, ValueError) as e:
        raise HTTPException(400, f"Parametros invalidos: {e}")

    # Busqueda en cache
    t_lookup_start = time.perf_counter()
    cached = cache.get(key)
    t_lookup_ms = (time.perf_counter() - t_lookup_start) * 1000

    # CACHE HIT
    if cached is not None:
        latency_ms = (time.perf_counter() - t_total_start) * 1000

        asyncio.create_task(_send_metric({
            "event": "hit",
            "query_type": req.query_type.upper(),
            "key": key,
            "latency_ms": latency_ms,
            "lookup_ms": t_lookup_ms,
            "ts": time.time(),
        }))
        return {
            "result": cached,
            "cache": "HIT",
            "latency_ms": latency_ms,
            "key": key,
        }

    # CACHE MISS - delegar al generador de respuestas
    try:
        resp = await http.post(
            f"{RESPONSE_GEN_URL}/query",
            json={"query_type": req.query_type, "params": req.params},
            timeout=10.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as e:
        asyncio.create_task(_send_metric({
            "event": "error",
            "query_type": req.query_type.upper(),
            "key": key,
            "error": str(e),
            "ts": time.time(),
        }))
        raise HTTPException(502, f"Generador de respuestas fallo: {e}")

    # Guardar en cache con TTL por tipo de consulta
    result = data["result"]
    compute_ms = data["compute_time_ms"]

    ttl = TTL_BY_QUERY.get(req.query_type.upper(), 300)
    cache.set(key, result, ttl=ttl)

    # Metricas del miss
    latency_ms = (time.perf_counter() - t_total_start) * 1000

    asyncio.create_task(_send_metric({
        "event": "miss",
        "query_type": req.query_type.upper(),
        "key": key,
        "latency_ms": latency_ms,
        "lookup_ms": t_lookup_ms,
        "compute_ms": compute_ms,
        "ttl": ttl,
        "ts": time.time(),
    }))

    return {
        "result": result,
        "cache": "MISS",
        "latency_ms": latency_ms,
        "compute_ms": compute_ms,
        "key": key,
        "ttl": ttl,
    }
