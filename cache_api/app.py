"""
Servicio de Cache - Intercepta consultas y las resuelve usando Redis.
Si hay cache hit retorna directamente; si no, delega al Generador de Respuestas.
Registra todas las metricas enviandolas via HTTP al servicio de Metricas.
"""
import os
import json
import time
import logging
import hashlib
import requests as http_requests
import redis
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Configuracion
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
RESPONSE_GEN_URL = os.environ.get("RESPONSE_GEN_URL", "http://generador_respuestas:5001")
METRICAS_URL = os.environ.get("METRICAS_URL", "http://metricas:5002")
CACHE_TTL = int(os.environ.get("CACHE_TTL", 60))

redis_client = None


def get_redis():
    global redis_client
    if redis_client is None:
        redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True
        )
    return redis_client


def send_metric(query_type, cache_key, hit, latency_ms, source):
    """Envia una metrica al servicio de metricas via HTTP."""
    try:
        payload = {
            "query_type": query_type,
            "cache_key": cache_key,
            "cache_hit": hit,
            "latency_ms": latency_ms,
            "source": source,
            "timestamp": time.time(),
        }
        resp = http_requests.post(
            f"{METRICAS_URL}/record",
            json=payload,
            timeout=3,
        )
        if resp.status_code != 201:
            logger.warning(f"Metricas respondio {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Error enviando metrica: {e}")


def send_eviction_metric():
    """Envia snapshot de evicted_keys al servicio de metricas."""
    try:
        r = get_redis()
        info = r.info("stats")
        evicted = info.get("evicted_keys", 0)
        http_requests.post(
            f"{METRICAS_URL}/record_eviction",
            json={"evicted_keys": evicted},
            timeout=3,
        )
    except Exception as e:
        logger.error(f"Error enviando eviction: {e}")


def build_cache_key(query_type, params):
    """Construye la cache key segun el tipo de consulta."""
    if query_type == "Q1":
        return f"count:{params.get('zone_id')}:conf={params.get('confidence_min', 0.0)}"
    elif query_type == "Q2":
        return f"area:{params.get('zone_id')}:conf={params.get('confidence_min', 0.0)}"
    elif query_type == "Q3":
        return f"density:{params.get('zone_id')}:conf={params.get('confidence_min', 0.0)}"
    elif query_type == "Q4":
        return (
            f"compare:density:{params.get('zone_a')}:{params.get('zone_b')}"
            f":conf={params.get('confidence_min', 0.0)}"
        )
    elif query_type == "Q5":
        return f"confidence_dist:{params.get('zone_id')}:bins={params.get('bins', 5)}"
    else:
        raw = json.dumps({"q": query_type, "p": params}, sort_keys=True)
        return f"query:{hashlib.md5(raw.encode()).hexdigest()}"


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
    data = request.get_json()
    query_type = data.get("query_type")
    params = data.get("params", {})

    cache_key = build_cache_key(query_type, params)
    start_time = time.time()

    r = get_redis()

    try:
        cached = r.get(cache_key)
    except Exception as e:
        logger.error(f"Error leyendo cache: {e}")
        cached = None

    if cached is not None:
        elapsed_ms = round((time.time() - start_time) * 1000, 2)
        result = json.loads(cached)
        send_metric(query_type, cache_key, True, elapsed_ms, "cache")

        return jsonify({
            "query_type": query_type,
            "params": params,
            "result": result,
            "cache_hit": True,
            "latency_ms": elapsed_ms,
        })

    # CACHE MISS
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
        send_metric(query_type, cache_key, False, elapsed_ms, "error")
        logger.error(f"Error consultando generador: {e}")
        return jsonify({"error": f"Error procesando consulta: {e}"}), 500

    # Guardar en cache con TTL
    try:
        r.setex(cache_key, CACHE_TTL, json.dumps(result))
    except Exception as e:
        logger.error(f"Error escribiendo cache: {e}")

    elapsed_ms = round((time.time() - start_time) * 1000, 2)
    send_metric(query_type, cache_key, False, elapsed_ms, "db")
    send_eviction_metric()

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
    try:
        r = get_redis()
        r.flushdb()
        return jsonify({"status": "cache flushed"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
