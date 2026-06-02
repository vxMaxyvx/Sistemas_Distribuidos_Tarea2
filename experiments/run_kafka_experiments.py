"""
run_kafka_experiments.py
Bateria de experimentos automatizados para la Tarea 2.
Controla Docker Compose (escala y variables de entorno), inyecta fallas temporales
y genera los snapshots de resultados en results/.
"""
import os
import json
import time
import subprocess
import urllib.request
import urllib.error
from pathlib import Path

# URLs de los servicios en el host
TRAFFIC = "http://localhost:5003"
CACHE = "http://localhost:5000"
METRICS = "http://localhost:5002"
RESPONSE_GEN = "http://localhost:5001"

RESULTS_DIR = Path(__file__).parent.parent / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def post(url, body=None, timeout=30):
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"Error en POST a {url}: {e}")
        return None


def get(url, timeout=10):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"Error en GET a {url}: {e}")
        return None


def run_cmd(cmd):
    """Ejecuta un comando del sistema."""
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"Error ejecutando: {cmd}\nStdout: {res.stdout}\nStderr: {res.stderr}")
    return res.stdout.strip()


def restart_services(use_kafka=False, scale_consumers=1):
    """Reinicia y escala los servicios segun el modo del experimento."""
    print(f"\n[docker] Reiniciando servicios con USE_KAFKA={use_kafka} y scale={scale_consumers}...", flush=True)
    
    # Detener contenedores
    run_cmd("docker compose down")
    time.sleep(2)
    
    # Configurar variable de entorno para la sesion
    env = os.environ.copy()
    env["USE_KAFKA"] = "true" if use_kafka else "false"
    
    # Iniciar servicios con escala
    cmd = f"docker compose up -d --build --scale cache_api={scale_consumers}"
    print(f"  Ejecutando: {cmd}")
    subprocess.run(cmd, shell=True, env=env)
    
    # Esperar a que esten listos
    wait_for_services()


def wait_for_services(retries=30, interval=2.0):
    """Espera a que los servicios esten saludables."""
    for svc, url in [("metricas", METRICS), ("respuestas", RESPONSE_GEN), ("cache", CACHE), ("trafico", TRAFFIC)]:
        print(f"  Esperando a {svc}...", end="", flush=True)
        for _ in range(retries):
            try:
                res = get(f"{url}/health", timeout=2)
                if res and res.get("status") == "ok":
                    print(" OK", flush=True)
                    break
            except Exception:
                pass
            time.sleep(interval)
        else:
            print(" FAILED", flush=True)
            raise RuntimeError(f"El servicio {svc} no respondio.")


def run_experiment(label, dist, duration, rate, use_kafka, scale, simulated_failure=False, extra_config=None):
    """Ejecuta un experimento individual y toma snapshot."""
    print(f"\n>>> INICIANDO EXPERIMENTO: {label} (USE_KAFKA={use_kafka}, scale={scale})", flush=True)
    
    # Inicializar servicios
    restart_services(use_kafka=use_kafka, scale_consumers=scale)
    
    # Limpiar cache y resetear metricas
    post(f"{METRICS}/reset")
    post(f"{CACHE}/flush")
    
    cfg = {
        "distribution": dist,
        "rate_qps": float(rate),
        "duration_sec": float(duration),
        "zipf_s": 1.2,
        "concurrency": 16,
        "seed": 42,
        "label": label,
    }
    
    # Lanzar trafico
    print(f"  Lanzando trafico: {rate} QPS por {duration}s...", flush=True)
    post(f"{TRAFFIC}/run", cfg)
    
    start_time = time.time()
    
    # Monitorear ejecucion y opcionalmente inyectar falla
    failure_triggered = False
    failure_restored = False
    
    deadline = start_time + duration + 30
    
    backlog_history = []
    
    while time.time() < deadline:
        # Monitorear estado
        status = get(f"{TRAFFIC}/status")
        if not status or not status.get("running"):
            # Si el trafico termino y el backlog en Kafka es 0, terminamos!
            if use_kafka:
                summary_data = get(f"{METRICS}/summary")
                lag = summary_data.get("backlog_size", 0) if summary_data else 0
                if lag == 0:
                    break
            else:
                break
                
        # Guardar historial de backlog si estamos en Kafka
        if use_kafka:
            summary_data = get(f"{METRICS}/summary")
            lag = summary_data.get("backlog_size", 0) if summary_data else 0
            backlog_history.append({"time_offset": round(time.time() - start_time, 1), "backlog": lag})
            
        # Simular falla temporal en el escenario 4
        elapsed = time.time() - start_time
        if simulated_failure:
            # Inyectar falla a los 5 segundos
            if elapsed >= 5.0 and not failure_triggered:
                print("\n  [FALLA] Inyectando falla temporal en el Generador de Respuestas! (HTTP 503)", flush=True)
                post(f"{RESPONSE_GEN}/toggle_failure", {"enabled": True})
                failure_triggered = True
            
            # Restaurar servicio a los 15 segundos (10 segundos de caida)
            if elapsed >= 15.0 and not failure_restored:
                print("\n  [FALLA] Restaurando Generador de Respuestas! Comienza recuperacion...", flush=True)
                post(f"{RESPONSE_GEN}/toggle_failure", {"enabled": False})
                failure_restored = True
                
        time.sleep(1.0)
        
    time.sleep(2.0)
    
    # Guardar snapshot de metricas
    snap_body = {
        "label": label,
        "extra": {
            "use_kafka": use_kafka,
            "scale": scale,
            "simulated_failure": simulated_failure,
            "backlog_history": backlog_history,
            **(extra_config or {})
        }
    }
    
    snap = post(f"{METRICS}/snapshot", snap_body)
    if snap:
        # Guardar localmente en results/
        out_path = RESULTS_DIR / f"snap_{label}.json"
        with open(out_path, "w") as f:
            json.dump(snap, f, indent=2)
        print(f"  Snapshot guardado exitosamente en: {out_path}", flush=True)
    else:
        print("  ERROR: No se pudo capturar el snapshot.", flush=True)


def run_all_scenarios():
    print("="*70)
    print("SISTEMAS DISTRIBUIDOS - BATERIA DE EXPERIMENTOS TAREA 2")
    print("="*70)
    
    t_start = time.time()
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 1: Sistema Base (Sincrono, sin Kafka)
    # Referencia para comparar contra la arquitectura asincrona.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="1_sync_base",
        dist="zipf",
        duration=30,
        rate=50,
        use_kafka=False,
        scale=1
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 2: Kafka + 1 Consumidor
    # Procesamiento asincrono basico con un solo consumer.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="2_kafka_1_consumer",
        dist="zipf",
        duration=30,
        rate=50,
        use_kafka=True,
        scale=1
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 3a: Kafka + 3 Consumidores
    # Escalamiento horizontal: evaluar impacto de multiples consumers.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="3a_kafka_3_consumers",
        dist="zipf",
        duration=30,
        rate=50,
        use_kafka=True,
        scale=3
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 3b: Kafka + 5 Consumidores
    # Mas consumidores para mostrar tendencia de escalamiento.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="3b_kafka_5_consumers",
        dist="zipf",
        duration=30,
        rate=50,
        use_kafka=True,
        scale=5
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 4: Falla Temporal con Kafka (Caida de 10s)
    # Demuestra reintentos y recuperacion via colas Kafka.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="4_kafka_transient_failure",
        dist="zipf",
        duration=40,
        rate=40,
        use_kafka=True,
        scale=1,
        simulated_failure=True,
        extra_config={"description": "Caida de 10s del Response Gen con reintentos Kafka (1 consumer)"}
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 5: Falla Temporal Sincrona (sin Kafka)
    # Comparacion: las consultas se pierden inmediatamente (HTTP 502).
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="5_sync_transient_failure",
        dist="zipf",
        duration=40,
        rate=40,
        use_kafka=False,
        scale=1,
        simulated_failure=True,
        extra_config={"description": "Caida de 10s del Response Gen en arquitectura sincrona sin colas"}
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 6: Spike de Trafico
    # Alta carga de 120 QPS para saturar colas y medir backlog.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="6_kafka_traffic_spike",
        dist="zipf",
        duration=20,
        rate=120,
        use_kafka=True,
        scale=1,
        extra_config={"description": "Spike de trafico a 120 QPS para medir acumulacion de backlog"}
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 7: Recuperacion ante Fallos con Escalamiento
    # Falla prolongada de 15s con 3 consumidores para evaluar recovery
    # time y capacidad de vaciado del backlog post-falla.
    # Comparar con Escenario 5 (sincrono) para medir perdida vs recovery.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="7_kafka_recovery_scaled",
        dist="zipf",
        duration=50,
        rate=50,
        use_kafka=True,
        scale=3,
        simulated_failure=True,
        extra_config={"description": "Falla de 10s con 3 consumers para evaluar recovery time y vaciado de backlog"}
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 8: Kafka con distribucion Uniforme
    # Comparar patron de trafico uniforme vs Zipf sobre colas Kafka.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="8_kafka_uniform",
        dist="uniform",
        duration=30,
        rate=50,
        use_kafka=True,
        scale=1,
        extra_config={"description": "Distribucion uniforme con Kafka para comparar contra Zipf"}
    )
    
    print("\n" + "="*70)
    print(f"BATERIA COMPLETA TERMINADA EN {(time.time() - t_start)/60:.1f} MINUTOS")
    print(f"Resultados en results/ -> graficar con build_kafka_figures.py")
    print("="*70)


if __name__ == "__main__":
    run_all_scenarios()

