"""
Servicio de Cache - Intercepta consultas y las resuelve usando Redis.
Si hay cache hit retorna directamente; si no, delega al Generador de Respuestas.
Registra todas las metricas en PostgreSQL.
"""
import os
import json
import time
import logging
import hashlib
import requests as http_requests
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Configuracion desde variables de entorno
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
RESPONSE_GEN_URL = os.environ.get("RESPONSE_GEN_URL", "http://generador_respuestas:5001")
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = int(os.environ.get("PG_PORT", 5432))
PG_DB = os.environ.get("PG_DB", "metricas")
PG_USER = os.environ.get("PG_USER", "admin")
PG_PASS = os.environ.get("PG_PASS", "admin123")
CACHE_TTL = int(os.environ.get("CACHE_TTL", 60))

# Conexion a Redis
redis_client = None

# Conexion a PostgreSQL
pg_conn = None


def get_redis():
    global redis_client
    if redis_client is None:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0,
                                   decode_responses=True)
    return redis_client


def get_pg():
    """Obtiene la conexion a PostgreSQL, reconectando si es necesario."""
    global pg_conn
    try:
        if pg_conn is None or pg_conn.closed:
            pg_conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                dbname=PG_DB, user=PG_USER, password=PG_PASS,
            )
            pg_conn.autocommit = True
    except Exception as e:
        logger.error(f"Error conectando a PostgreSQL: {e}")
        pg_conn = None
    return pg_conn


def build_cache_key(query_type, params):
    """Construye la cache key segun el tipo de consulta."""
    if query_type == "Q1":
        return f"count:{params.get('zone_id')}:conf={params.get('confidence_min', 0.0)}"
    elif query_type == "Q2":
        return f"area:{params.get('zone_id')}:conf={params.get('confidence_min', 0.0)}"
    elif query_type == "Q3":
        return f"density:{params.get('zone_id')}:conf={params.get('confidence_min', 0.0)}"
    elif query_type == "Q4":
        return (f"compare:density:{params.get('zone_a')}:{params.get('zone_b')}"
                f":conf={params.get('confidence_min', 0.0)}")
    elif query_type == "Q5":
        return f"confidence_dist:{params.get('zone_id')}:bins={params.get('bins', 5)}"
    else:
        # Fallback: hash de los parametros
        raw = json.dumps({"q": query_type, "p": params}, sort_keys=True)
        return f"query:{hashlib.md5(raw.encode()).hexdigest()}"


def log_metric(query_type, cache_key, hit, latency_ms, source):
    """Registra la metrica de la consulta en PostgreSQL."""
    conn = get_pg()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO query_metrics
                   (query_type, cache_key, cache_hit, latency_ms, source, created_at)
                   VALUES (%s, %s, %s, %s, %s, NOW())""",
                (query_type, cache_key, hit, latency_ms, source),
            )
    except Exception as e:
        logger.error(f"Error registrando metrica: {e}")


def log_eviction_event():
    """Registra un evento de eviccion consultando info de Redis."""
    conn = get_pg()
    if conn is None:
        return
    try:
        r = get_redis()
        info = r.info("stats")
        evicted = info.get("evicted_keys", 0)
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO eviction_metrics (evicted_keys, recorded_at)
                   VALUES (%s, NOW())""",
                (evicted,),
            )
    except Exception as e:
        logger.error(f"Error registrando eviccion: {e}")


@app.route("/health", methods=["GET"])
def health():
    try:
        r = get_redis()
        r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return jsonify({"status": "ok", "redis": redis_ok})


@app.route("/query", methods=["POST"])
def handle_query():
    """
    Endpoint principal. Recibe una consulta, revisa cache,
    y si no esta, la delega al generador de respuestas.
    """
    data = request.get_json()
    query_type = data.get("query_type")
    params = data.get("params", {})

    cache_key = build_cache_key(query_type, params)
    start_time = time.time()

    r = get_redis()

    # Intentar cache hit
    try:
        cached = r.get(cache_key)
    except Exception as e:
        logger.error(f"Error leyendo cache: {e}")
        cached = None

    if cached is not None:
        # CACHE HIT
        elapsed_ms = round((time.time() - start_time) * 1000, 2)
        result = json.loads(cached)
        log_metric(query_type, cache_key, True, elapsed_ms, "cache")

        return jsonify({
            "query_type": query_type,
            "params": params,
            "result": result,
            "cache_hit": True,
            "latency_ms": elapsed_ms,
        })

    # CACHE MISS -> delegar al generador de respuestas
    try:
        resp = http_requests.post(
            f"{RESPONSE_GEN_URL}/query",
            json={"query_type": query_type, "params": params},
            timeout=10,
        )
        resp.raise_for_status()
        response_data = resp.json()
        result = response_data.get("result")
        processing_time = response_data.get("processing_time_ms", 0)

    except Exception as e:
        elapsed_ms = round((time.time() - start_time) * 1000, 2)
        log_metric(query_type, cache_key, False, elapsed_ms, "error")
        logger.error(f"Error consultando generador de respuestas: {e}")
        return jsonify({"error": f"Error procesando consulta: {e}"}), 500

    # Guardar en cache con TTL
    try:
        r.setex(cache_key, CACHE_TTL, json.dumps(result))
    except Exception as e:
        logger.error(f"Error escribiendo en cache: {e}")

    elapsed_ms = round((time.time() - start_time) * 1000, 2)
    log_metric(query_type, cache_key, False, elapsed_ms, "db")

    # Registrar evictions periodicamente
    log_eviction_event()

    return jsonify({
        "query_type": query_type,
        "params": params,
        "result": result,
        "cache_hit": False,
        "latency_ms": elapsed_ms,
        "processing_time_ms": processing_time,
    })


@app.route("/stats", methods=["GET"])
def cache_stats():
    """Retorna estadisticas actuales del cache Redis."""
    try:
        r = get_redis()
        info = r.info()
        memory = r.info("memory")
        stats = r.info("stats")
        return jsonify({
            "used_memory_mb": round(memory.get("used_memory", 0) / (1024 * 1024), 2),
            "maxmemory_mb": round(memory.get("maxmemory", 0) / (1024 * 1024), 2),
            "eviction_policy": info.get("maxmemory_policy", "unknown"),
            "total_keys": r.dbsize(),
            "evicted_keys": stats.get("evicted_keys", 0),
            "keyspace_hits": stats.get("keyspace_hits", 0),
            "keyspace_misses": stats.get("keyspace_misses", 0),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/flush", methods=["POST"])
def flush_cache():
    """Limpia el cache completo (util para reiniciar experimentos)."""
    try:
        r = get_redis()
        r.flushdb()
        return jsonify({"status": "cache flushed"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
