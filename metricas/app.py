"""
Servicio de Almacenamiento de Metricas.
Recibe eventos del sistema de cache via HTTP y los mantiene en memoria (RAM).
Ofrece endpoints para consultar estadisticas agregadas y exportar resultados.
"""
import os
import time
import json
import csv
import io
import logging
from collections import defaultdict
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Metricas almacenadas en memoria (lista de diccionarios)
metrics = []
eviction_snapshots = []

# Contadores agregados para consultas rapidas
stats_counter = defaultdict(int)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "total_metrics": len(metrics)})


@app.route("/record", methods=["POST"])
def record_metric():
    """Recibe una metrica de consulta individual (hit/miss)."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body"}), 400

    entry = {
        "query_type": data.get("query_type"),
        "cache_key": data.get("cache_key"),
        "cache_hit": data.get("cache_hit"),
        "latency_ms": data.get("latency_ms"),
        "source": data.get("source"),
        "timestamp": data.get("timestamp", time.time()),
    }
    metrics.append(entry)

    # Actualizar contadores
    stats_counter["total"] += 1
    if entry["cache_hit"]:
        stats_counter["hits"] += 1
    else:
        stats_counter["misses"] += 1

    logger.debug(f"Metrica registrada: {entry['query_type']} hit={entry['cache_hit']}")
    return jsonify({"status": "recorded"}), 201


@app.route("/record_eviction", methods=["POST"])
def record_eviction():
    """Recibe un snapshot de evicted_keys de Redis."""
    data = request.get_json()
    eviction_snapshots.append({
        "evicted_keys": data.get("evicted_keys", 0),
        "timestamp": time.time(),
    })
    return jsonify({"status": "recorded"}), 201


@app.route("/stats", methods=["GET"])
def get_stats():
    """Retorna estadisticas agregadas del sistema."""
    total = stats_counter["total"]
    hits = stats_counter["hits"]
    misses = stats_counter["misses"]

    if total == 0:
        return jsonify({"message": "Sin metricas registradas aun"}), 200

    hit_rate = hits / total if total else 0.0
    miss_rate = misses / total if total else 0.0

    # Latencias
    latencies = [m["latency_ms"] for m in metrics if m.get("latency_ms") is not None]
    lat_sorted = sorted(latencies)
    n = len(lat_sorted)
    p50 = lat_sorted[n // 2] if n else 0.0
    p95 = lat_sorted[int(n * 0.95)] if n else 0.0
    avg_latency = sum(latencies) / n if n else 0.0

    # Throughput: consultas / tiempo transcurrido
    if len(metrics) >= 2:
        t0 = metrics[0]["timestamp"]
        t1 = metrics[-1]["timestamp"]
        elapsed = t1 - t0 if t1 > t0 else 1.0
        throughput = total / elapsed
    else:
        throughput = 0.0

    # Eviction rate (evicciones por minuto)
    evicted_now = eviction_snapshots[-1]["evicted_keys"] if eviction_snapshots else 0
    evicted_start = eviction_snapshots[0]["evicted_keys"] if len(eviction_snapshots) > 1 else 0
    evicted_delta = evicted_now - evicted_start
    if len(eviction_snapshots) >= 2:
        ev_time = eviction_snapshots[-1]["timestamp"] - eviction_snapshots[0]["timestamp"]
        eviction_rate = (evicted_delta / ev_time) * 60.0 if ev_time > 0 else 0.0
    else:
        eviction_rate = 0.0

    # Cache efficiency (hits * t_cache - misses * t_db) / total
    t_cache = avg_latency if hits else 0.0
    miss_latencies = [m["latency_ms"] for m in metrics if not m.get("cache_hit") and m.get("latency_ms")]
    t_db = sum(miss_latencies) / len(miss_latencies) if miss_latencies else 0.0
    efficiency = (hits * t_cache - misses * t_db) / total if total else 0.0

    return jsonify({
        "total_queries": total,
        "hits": hits,
        "misses": misses,
        "hit_rate": round(hit_rate, 4),
        "miss_rate": round(miss_rate, 4),
        "throughput_qps": round(throughput, 2),
        "avg_latency_ms": round(avg_latency, 2),
        "latency_p50_ms": round(p50, 2),
        "latency_p95_ms": round(p95, 2),
        "evicted_keys": evicted_now,
        "eviction_rate_per_min": round(eviction_rate, 2),
        "cache_efficiency": round(efficiency, 2),
    })


@app.route("/query_breakdown", methods=["GET"])
def query_breakdown():
    """Desglose por tipo de consulta."""
    breakdown = defaultdict(lambda: {"total": 0, "hits": 0, "misses": 0})
    for m in metrics:
        qt = m.get("query_type", "unknown")
        breakdown[qt]["total"] += 1
        if m.get("cache_hit"):
            breakdown[qt]["hits"] += 1
        else:
            breakdown[qt]["misses"] += 1

    result = {}
    for qt, vals in breakdown.items():
        result[qt] = {
            "total": vals["total"],
            "hits": vals["hits"],
            "misses": vals["misses"],
            "hit_rate": round(vals["hits"] / vals["total"], 4) if vals["total"] else 0.0,
        }
    return jsonify(result)


@app.route("/export/csv", methods=["GET"])
def export_csv():
    """Exporta todas las metricas a CSV para analisis offline."""
    if not metrics:
        return jsonify({"error": "No hay metricas para exportar"}), 404

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "timestamp", "query_type", "cache_key", "cache_hit", "latency_ms", "source"
    ])
    writer.writeheader()
    for m in metrics:
        writer.writerow({
            "timestamp": m["timestamp"],
            "query_type": m["query_type"],
            "cache_key": m.get("cache_key", ""),
            "cache_hit": m["cache_hit"],
            "latency_ms": m["latency_ms"],
            "source": m.get("source", ""),
        })

    return output.getvalue(), 200, {"Content-Type": "text/csv"}


@app.route("/export/json", methods=["GET"])
def export_json():
    """Exporta todas las metricas a JSON."""
    return jsonify({"metrics": metrics, "eviction_snapshots": eviction_snapshots})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5002))
    app.run(host="0.0.0.0", port=port)
