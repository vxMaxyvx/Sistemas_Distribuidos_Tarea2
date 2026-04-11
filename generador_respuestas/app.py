"""
Generador de Respuestas - Servicio que procesa consultas Q1-Q5
sobre el dataset de edificaciones de Santiago precargado en memoria.
"""
import os
import csv
import time
import logging
from statistics import mean
from flask import Flask, request, jsonify
import numpy as np

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Estructura en memoria con los datos por zona
zone_data = {}

# Areas precalculadas de cada bounding box en km2
zone_area_km2 = {}

# Configuracion de zonas
ZONES = {
    "Z1": {"name": "Providencia", "lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600},
    "Z2": {"name": "Las Condes", "lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550},
    "Z3": {"name": "Maipu", "lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740},
    "Z4": {"name": "Santiago Centro", "lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630},
    "Z5": {"name": "Pudahuel", "lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760},
}

DATA_PATH = os.environ.get("DATA_PATH", "/app/data/buildings_rm.csv")


def calc_bbox_area_km2(lat_min, lat_max, lon_min, lon_max):
    """Calcula el area aproximada de un bounding box en km2."""
    import math
    lat_mid = (lat_min + lat_max) / 2.0
    # 1 grado de latitud ~ 111 km
    height_km = abs(lat_max - lat_min) * 111.0
    # 1 grado de longitud ~ 111 * cos(lat) km
    width_km = abs(lon_max - lon_min) * 111.0 * math.cos(math.radians(lat_mid))
    return height_km * width_km


def load_dataset():
    """Carga el CSV en memoria y clasifica por zona."""
    global zone_data, zone_area_km2

    logger.info(f"Cargando dataset desde {DATA_PATH}...")

    # Inicializar listas por zona
    for zid in ZONES:
        zone_data[zid] = []

    count = 0
    with open(DATA_PATH, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lat = float(row["latitude"])
            lon = float(row["longitude"])
            area = float(row["area_in_meters"])
            conf = float(row["confidence"])

            record = {"latitude": lat, "longitude": lon,
                      "area": area, "confidence": conf}

            # Asignar a la zona correspondiente
            for zid, z in ZONES.items():
                if (z["lat_min"] <= lat <= z["lat_max"] and
                        z["lon_min"] <= lon <= z["lon_max"]):
                    zone_data[zid].append(record)
                    break
            count += 1

    # Precalcular areas de bounding boxes
    for zid, z in ZONES.items():
        zone_area_km2[zid] = calc_bbox_area_km2(
            z["lat_min"], z["lat_max"], z["lon_min"], z["lon_max"]
        )

    logger.info(f"Dataset cargado: {count} registros totales")
    for zid in ZONES:
        logger.info(f"  {zid} ({ZONES[zid]['name']}): {len(zone_data[zid])} edificaciones, "
                     f"area bbox: {zone_area_km2[zid]:.2f} km2")


# --- Consultas Q1 a Q5 ---

def q1_count(zone_id, confidence_min=0.0):
    """Q1: Conteo de edificios en una zona."""
    records = zone_data.get(zone_id, [])
    return sum(1 for r in records if r["confidence"] >= confidence_min)


def q2_area(zone_id, confidence_min=0.0):
    """Q2: Area promedio y total de edificaciones."""
    areas = [r["area"] for r in zone_data.get(zone_id, [])
             if r["confidence"] >= confidence_min]
    if not areas:
        return {"avg_area": 0.0, "total_area": 0.0, "n": 0}
    return {
        "avg_area": round(mean(areas), 2),
        "total_area": round(sum(areas), 2),
        "n": len(areas),
    }


def q3_density(zone_id, confidence_min=0.0):
    """Q3: Densidad de edificaciones por km2."""
    count = q1_count(zone_id, confidence_min)
    area_km2 = zone_area_km2.get(zone_id, 1.0)
    return round(count / area_km2, 2)


def q4_compare(zone_a, zone_b, confidence_min=0.0):
    """Q4: Comparacion de densidad entre dos zonas."""
    da = q3_density(zone_a, confidence_min)
    db = q3_density(zone_b, confidence_min)
    return {
        "zone_a": {"id": zone_a, "density": da},
        "zone_b": {"id": zone_b, "density": db},
        "winner": zone_a if da > db else zone_b,
    }


def q5_confidence_dist(zone_id, bins=5):
    """Q5: Distribucion de confianza en una zona."""
    scores = [r["confidence"] for r in zone_data.get(zone_id, [])]
    if not scores:
        return []
    counts, edges = np.histogram(scores, bins=bins, range=(0, 1))
    result = []
    for i in range(bins):
        result.append({
            "bucket": i,
            "min": round(float(edges[i]), 4),
            "max": round(float(edges[i + 1]), 4),
            "count": int(counts[i]),
        })
    return result


# --- Endpoints HTTP ---

@app.route("/health", methods=["GET"])
def health():
    total = sum(len(zone_data[z]) for z in zone_data)
    return jsonify({"status": "ok", "total_records": total})


@app.route("/query", methods=["POST"])
def handle_query():
    """Endpoint principal para procesar consultas."""
    data = request.get_json()
    query_type = data.get("query_type")
    params = data.get("params", {})

    start = time.time()

    try:
        if query_type == "Q1":
            result = q1_count(params["zone_id"], params.get("confidence_min", 0.0))
        elif query_type == "Q2":
            result = q2_area(params["zone_id"], params.get("confidence_min", 0.0))
        elif query_type == "Q3":
            result = q3_density(params["zone_id"], params.get("confidence_min", 0.0))
        elif query_type == "Q4":
            result = q4_compare(params["zone_a"], params["zone_b"],
                                params.get("confidence_min", 0.0))
        elif query_type == "Q5":
            result = q5_confidence_dist(params["zone_id"], params.get("bins", 5))
        else:
            return jsonify({"error": f"Tipo de consulta desconocido: {query_type}"}), 400

        elapsed = time.time() - start
        return jsonify({
            "query_type": query_type,
            "params": params,
            "result": result,
            "processing_time_ms": round(elapsed * 1000, 2),
        })

    except KeyError as e:
        return jsonify({"error": f"Parametro faltante: {e}"}), 400
    except Exception as e:
        logger.error(f"Error procesando consulta: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    load_dataset()
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port)
