"""
Generador de Trafico - Simula consultas de empresas de reparto
usando distribuciones Zipf (ley de potencia) y Uniforme.
"""
import os
import json
import time
import random
import logging
import threading
import requests as http_requests
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Config
CACHE_URL = os.environ.get("CACHE_URL", "http://cache:5000")
DISTRIBUTION = os.environ.get("DISTRIBUTION", "zipf")  # "zipf" o "uniform"
TOTAL_QUERIES = int(os.environ.get("TOTAL_QUERIES", 1000))
QUERIES_PER_SECOND = float(os.environ.get("QUERIES_PER_SECOND", 10))
NUM_THREADS = int(os.environ.get("NUM_THREADS", 4))

# Zonas y consultas disponibles
ZONE_IDS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERY_TYPES = ["Q1", "Q2", "Q3", "Q4", "Q5"]
CONFIDENCE_VALUES = [0.0, 0.3, 0.5, 0.7, 0.9]
BINS_OPTIONS = [3, 5, 10]


def generate_zipf_distribution(n_items, n_samples, s=1.5):
    """
    Genera indices siguiendo una distribucion Zipf.
    Las primeras zonas/consultas tendran mas probabilidad.
    """
    weights = np.array([1.0 / (i ** s) for i in range(1, n_items + 1)])
    weights /= weights.sum()
    return np.random.choice(n_items, size=n_samples, p=weights)


def generate_query(distribution, query_idx=None):
    """Genera una consulta sintetica aleatoria."""
    if distribution == "zipf":
        # Zipf para zonas (algunas zonas se consultan mucho mas)
        zone_weights = np.array([1.0 / (i ** 1.5) for i in range(1, len(ZONE_IDS) + 1)])
        zone_weights /= zone_weights.sum()
        zone_idx = np.random.choice(len(ZONE_IDS), p=zone_weights)

        # Zipf para tipo de consulta (Q1 es la mas comun)
        qt_weights = np.array([1.0 / (i ** 1.2) for i in range(1, len(QUERY_TYPES) + 1)])
        qt_weights /= qt_weights.sum()
        qt_idx = np.random.choice(len(QUERY_TYPES), p=qt_weights)
    else:
        # Distribucion uniforme
        zone_idx = random.randint(0, len(ZONE_IDS) - 1)
        qt_idx = random.randint(0, len(QUERY_TYPES) - 1)

    zone_id = ZONE_IDS[zone_idx]
    query_type = QUERY_TYPES[qt_idx]
    confidence_min = random.choice(CONFIDENCE_VALUES)

    # Armar los parametros segun el tipo de consulta
    params = {}
    if query_type in ("Q1", "Q2", "Q3"):
        params = {"zone_id": zone_id, "confidence_min": confidence_min}
    elif query_type == "Q4":
        zone_b_idx = random.randint(0, len(ZONE_IDS) - 1)
        while zone_b_idx == zone_idx:
            zone_b_idx = random.randint(0, len(ZONE_IDS) - 1)
        params = {
            "zone_a": zone_id,
            "zone_b": ZONE_IDS[zone_b_idx],
            "confidence_min": confidence_min,
        }
    elif query_type == "Q5":
        params = {"zone_id": zone_id, "bins": random.choice(BINS_OPTIONS)}

    return {"query_type": query_type, "params": params}


def send_query(query, query_num):
    """Envia una consulta al servicio de cache."""
    try:
        start = time.time()
        resp = http_requests.post(
            f"{CACHE_URL}/query",
            json=query,
            timeout=15,
        )
        elapsed = time.time() - start
        status = resp.status_code

        if status == 200:
            data = resp.json()
            hit = data.get("cache_hit", False)
            latency = data.get("latency_ms", elapsed * 1000)
            logger.info(f"[{query_num}] {query['query_type']} -> "
                        f"{'HIT' if hit else 'MISS'} | {latency:.1f}ms")
        else:
            logger.warning(f"[{query_num}] {query['query_type']} -> HTTP {status}")
        return resp

    except Exception as e:
        logger.error(f"[{query_num}] Error: {e}")
        return None


def wait_for_services():
    """Espera a que el servicio de cache este disponible."""
    logger.info("Esperando a que los servicios esten listos...")
    max_retries = 30
    for i in range(max_retries):
        try:
            resp = http_requests.get(f"{CACHE_URL}/health", timeout=5)
            if resp.status_code == 200:
                logger.info("Servicios listos!")
                return True
        except Exception:
            pass
        logger.info(f"  Reintentando... ({i + 1}/{max_retries})")
        time.sleep(2)

    logger.error("Los servicios no respondieron a tiempo")
    return False


def run_traffic():
    """Ejecuta la generacion de trafico."""
    np.random.seed(42)
    random.seed(42)

    if not wait_for_services():
        return

    logger.info(f"Iniciando generador de trafico")
    logger.info(f"  Distribucion: {DISTRIBUTION}")
    logger.info(f"  Total consultas: {TOTAL_QUERIES}")
    logger.info(f"  Tasa: {QUERIES_PER_SECOND} consultas/segundo")

    interval = 1.0 / QUERIES_PER_SECOND
    results = {"total": 0, "success": 0, "errors": 0, "hits": 0, "misses": 0}

    start_time = time.time()

    for i in range(TOTAL_QUERIES):
        query = generate_query(DISTRIBUTION)
        resp = send_query(query, i + 1)

        if resp and resp.status_code == 200:
            results["success"] += 1
            data = resp.json()
            if data.get("cache_hit"):
                results["hits"] += 1
            else:
                results["misses"] += 1
        else:
            results["errors"] += 1

        results["total"] += 1

        # Control de tasa
        elapsed = time.time() - start_time
        expected = (i + 1) * interval
        if elapsed < expected:
            time.sleep(expected - elapsed)

    total_time = time.time() - start_time

    # Resumen final
    logger.info("=" * 60)
    logger.info("RESUMEN DE LA EJECUCION")
    logger.info("=" * 60)
    logger.info(f"  Distribucion: {DISTRIBUTION}")
    logger.info(f"  Total consultas: {results['total']}")
    logger.info(f"  Exitosas: {results['success']}")
    logger.info(f"  Errores: {results['errors']}")
    logger.info(f"  Cache hits: {results['hits']}")
    logger.info(f"  Cache misses: {results['misses']}")
    if results["success"] > 0:
        hit_rate = results["hits"] / results["success"] * 100
        logger.info(f"  Hit rate: {hit_rate:.1f}%")
    logger.info(f"  Tiempo total: {total_time:.2f}s")
    logger.info(f"  Throughput: {results['total'] / total_time:.1f} q/s")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_traffic()
