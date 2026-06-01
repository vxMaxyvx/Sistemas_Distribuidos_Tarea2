"""
build_kafka_figures.py
Genera 4 graficos comparativos premium para el informe de la Tarea 2.
Lee los snapshots persistidos en results/ y guarda los graficos en informe/figs/.
"""
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

matplotlib.rcParams.update({
    "font.size": 11,
    "axes.titlesize": 12,
    "axes.labelsize": 11,
    "legend.fontsize": 10,
    "figure.dpi": 110,
    "savefig.dpi": 180,
    "savefig.bbox": "tight",
    "axes.spines.top": False,
    "axes.spines.right": False,
})

ROOT = Path(__file__).parent.parent
RES = ROOT / "results"
FIG = ROOT / "informe" / "figs"
FIG.mkdir(parents=True, exist_ok=True)

C = {
    "sync": "#c1453b",      # Rojo elegante
    "kafka1": "#2c6dd6",    # Azul
    "kafka3": "#0a8754",    # Verde
    "failure": "#c1453b",
    "recovery": "#0a8754"
}


def load_snapshot(label):
    p = RES / f"snap_{label}.json"
    if not p.exists():
        print(f"Advertencia: No se encontro el archivo de resultados {p}")
        return None
    with open(p, "r") as f:
        d = json.load(open(p))
    return d.get("summary", d)


def save(fig, name):
    for ext in ("pdf", "png"):
        fig.savefig(FIG / f"{name}.{ext}")
    plt.close()
    print(f"[fig-t2] Grafico generado: {name}")


def fig1_throughput():
    """Comparativa de Throughput sostenido."""
    sync = load_snapshot("1_sync_base")
    k1 = load_snapshot("2_kafka_1_consumer")
    k3 = load_snapshot("3_kafka_3_consumers")
    
    if not all([sync, k1, k3]):
        return
        
    labels = ["Sincrono Base\n(1 Thread)", "Kafka\n(1 Consumidor)", "Kafka Escaldo\n(3 Consumidores)"]
    thrs = [
        sync.get("throughput_qps_total", 0),
        k1.get("throughput_qps_total", 0),
        k3.get("throughput_qps_total", 0)
    ]
    colors = [C["sync"], C["kafka1"], C["kafka3"]]
    
    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    bars = ax.bar(labels, thrs, color=colors, width=0.5, zorder=2)
    for b, t in zip(bars, thrs):
        ax.text(b.get_x() + b.get_width() / 2, t + 1,
                f"{t:.1f} QPS", ha="center", fontsize=10, fontweight="bold")
                
    ax.set_ylabel("Throughput de exito (consultas/segundo)")
    ax.set_ylim(0, max(thrs) * 1.25)
    ax.set_title("Capacidad del Sistema: Throughput de Exito Sostenido", pad=12)
    ax.grid(axis="y", alpha=0.3, zorder=0)
    fig.tight_layout()
    save(fig, "fig1_throughput_comparison")


def fig2_latencies():
    """Percentiles de latencia hit/miss/all."""
    sync = load_snapshot("1_sync_base")
    k1 = load_snapshot("2_kafka_1_consumer")
    k3 = load_snapshot("3_kafka_3_consumers")
    
    if not all([sync, k1, k3]):
        return
        
    p50_hit = sync.get("latency_ms_hit", {}).get("p50") or 0
    p95_hit = sync.get("latency_ms_hit", {}).get("p95") or 0
    p50_miss = sync.get("latency_ms_miss", {}).get("p50") or 0
    p95_miss = sync.get("latency_ms_miss", {}).get("p95") or 0
    
    # Para Kafka all es lo representativo (ya que incluye la cola)
    k1_p50 = k1.get("latency_ms_all", {}).get("p50") or 0
    k1_p95 = k1.get("latency_ms_all", {}).get("p95") or 0
    k3_p50 = k3.get("latency_ms_all", {}).get("p50") or 0
    k3_p95 = k3.get("latency_ms_all", {}).get("p95") or 0

    categories = ["Sincrono (Hit)", "Sincrono (Miss)", "Kafka (1 Cons)", "Kafka (3 Cons)"]
    p50s = [p50_hit, p50_miss, k1_p50, k3_p50]
    p95s = [p95_hit, p95_miss, k1_p95, k3_p95]
    
    x = np.arange(len(categories))
    w = 0.35
    
    fig, ax = plt.subplots(figsize=(8.5, 5))
    b1 = ax.bar(x - w/2, p50s, w, label="Latencia p50 (Mediana)", color="#4682B4", zorder=2)
    b2 = ax.bar(x + w/2, p95s, w, label="Latencia p95 (Cola)", color="#B0C4DE", zorder=2)
    
    # Agregar valores sobre las barras
    for b in b1:
        h = b.get_height()
        ax.text(b.get_x() + b.get_width()/2, h + 2, f"{int(h)}ms", ha="center", fontsize=8)
    for b in b2:
        h = b.get_height()
        ax.text(b.get_x() + b.get_width()/2, h + 2, f"{int(h)}ms", ha="center", fontsize=8)

    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.set_ylabel("Tiempo de respuesta (ms)")
    ax.set_yscale("log")
    ax.set_title("Latencias de Respuesta p50 y p95 (Escala Logaritmica)", pad=12)
    ax.legend(loc="upper left")
    ax.grid(axis="y", which="both", alpha=0.3, zorder=0)
    fig.tight_layout()
    save(fig, "fig2_latency_comparison")


def fig3_reliability():
    """Tolerancia a fallos: Consultas perdidas vs Recuperadas en caida de 10s."""
    sync_fail = load_snapshot("5_sync_transient_failure")
    kafka_fail = load_snapshot("4_kafka_transient_failure")
    
    if not all([sync_fail, kafka_fail]):
        return
        
    sync_totals = sync_fail.get("totals", {})
    kafka_totals = kafka_fail.get("totals", {})
    
    sync_errors = sync_totals.get("errors", 0)
    sync_ok = sync_totals.get("hits", 0) + sync_totals.get("misses", 0)
    
    kafka_recovered = kafka_totals.get("recoveries", 0)
    kafka_ok = kafka_totals.get("hits", 0) + kafka_totals.get("misses", 0)
    kafka_dlq = kafka_totals.get("dlq", 0)
    
    labels = ["Sincrono (Caida 10s)", "Kafka (Caida 10s)"]
    successful = [sync_ok, kafka_ok + kafka_recovered]
    recovered = [0, kafka_recovered]
    errors = [sync_errors, kafka_dlq]
    
    fig, ax = plt.subplots(figsize=(8, 5))
    
    # Stacked bars
    w = 0.45
    ax.bar(labels, successful, w, label="Completadas Exitosamente", color="#0a8754", alpha=0.85, zorder=2)
    ax.bar(labels, errors, w, bottom=successful, label="Errores / Perdidas", color="#c1453b", alpha=0.85, zorder=2)
    
    # Colocar etiquetas en las barras
    ax.text(0, sync_ok / 2, f"OK: {sync_ok}", ha="center", color="white", fontweight="bold")
    if sync_errors > 0:
        ax.text(0, sync_ok + sync_errors / 2, f"FALLAS: {sync_errors}", ha="center", color="white", fontweight="bold")
        
    ax.text(1, (kafka_ok + kafka_recovered) / 2, f"OK & REC: {kafka_ok + kafka_recovered}", ha="center", color="white", fontweight="bold")
    if kafka_dlq > 0:
        ax.text(1, kafka_ok + kafka_recovered + kafka_dlq / 2, f"DLQ: {kafka_dlq}", ha="center", color="white", fontweight="bold")

    ax.set_ylabel("Cantidad total de consultas")
    ax.set_title("Confiabilidad ante Fallos Temporales (Caida de 10s)", pad=12)
    ax.legend(loc="lower center")
    ax.grid(axis="y", alpha=0.3, zorder=0)
    fig.tight_layout()
    save(fig, "fig3_reliability_comparison")


def fig4_backlog_timeline():
    """Linea de tiempo de acumulacion de backlog y vaciado."""
    kafka_fail = load_snapshot("4_kafka_transient_failure")
    if not kafka_fail:
        return
        
    extra = kafka_fail.get("extra", {})
    history = extra.get("backlog_history", [])
    
    if not history:
        print("Advertencia: No se encontro historial de backlog en el snapshot.")
        return
        
    times = [h["time_offset"] for h in history]
    backlogs = [h["backlog"] for h in history]
    
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.plot(times, backlogs, color="#2c6dd6", linewidth=2.5, marker="o", markersize=4, label="Mensajes Pendientes (Lag)")
    ax.fill_between(times, backlogs, color="#2c6dd6", alpha=0.15)
    
    # Anotar caida
    ax.axvspan(5.0, 15.0, color="#c1453b", alpha=0.12, label="Ventana de Falla (Response Gen Caido)")
    ax.axvline(15.0, color="red", linestyle="--", alpha=0.6)
    ax.text(15.2, max(backlogs) * 0.7, "Servicio Restaurado", color="red", fontsize=9, fontweight="bold")
    
    # Calcular Recovery Time
    recovery_start = 15.0
    recovery_end = times[-1]
    for t, b in zip(times, backlogs):
        if t >= 15.0 and b == 0:
            recovery_end = t
            break
    rec_time = recovery_end - recovery_start
    ax.text(recovery_end - 1, 10, f"Recovery Time: {rec_time:.1f}s", color="green", fontsize=9, fontweight="bold", ha="right")
    
    ax.set_xlabel("Tiempo transcurrido desde inicio del experimento (s)")
    ax.set_ylabel("Mensajes en backlog Kafka")
    ax.set_title("Evolucion Temporal del Backlog de Kafka ante Falla y Recuperacion", pad=12)
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    save(fig, "fig4_backlog_evolution")


if __name__ == "__main__":
    fig1_throughput()
    fig2_latencies()
    fig3_reliability()
    fig4_backlog_timeline()
    print("\n[figs-t2] 4 figuras de la Tarea 2 generadas con éxito.")
