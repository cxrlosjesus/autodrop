"""
AutoPulse Panamá — Orquestación con Prefect

Flows principales:
    - autopulse_daily_pipeline  : Scraping + dbt. Corre cada 24h.
    - run_spider                : Ejecuta un spider individual.
    - run_dbt                   : Ejecuta el pipeline dbt.
    - health_check              : Verifica que todos los spiders funcionen.

Cómo deployar:
    prefect deploy flows/main_flow.py:autopulse_daily_pipeline \
        --name "AutoPulse Daily" \
        --pool autopulse-pool \
        --cron "0 6 * * *"         # Todos los días a las 6am hora Panamá
"""
import subprocess
import os
from datetime import datetime, timezone
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect.task_runners import ConcurrentTaskRunner


# ──────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────

SCRAPERS_DIR = Path(os.getenv("SCRAPERS_DIR", "/scrapers"))
DBT_DIR      = Path(os.getenv("DBT_DIR", "/dbt"))

# Spiders disponibles (en orden de ejecución)
SPIDERS = [
    "encuentra24",   # HTTP puro, más rápido
    "carspot",       # Playwright
    "automarket",    # Playwright
    "champion",      # Playwright
]


# ──────────────────────────────────────────
# TASKS
# ──────────────────────────────────────────

@task(
    name="run-spider",
    retries=2,
    retry_delay_seconds=300,    # 5 min entre reintentos
    timeout_seconds=7200,        # 2 horas máximo por spider
    tags=["scraping"]
)
def run_spider(spider_name: str) -> dict:
    """Ejecuta un spider de Scrapy y retorna métricas del run."""
    logger = get_run_logger()
    logger.info(f"🕷️ Iniciando spider: {spider_name}")

    start = datetime.now(timezone.utc)
    log_path = Path(f"/logs/{spider_name}_{start.strftime('%Y%m%d_%H%M')}.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Escribir al log en tiempo real (permite tail -f durante la ejecución)
    with open(log_path, "w", encoding="utf-8") as log_file:
        result = subprocess.run(
            ["scrapy", "crawl", spider_name],
            stdout=log_file,
            stderr=log_file,
            cwd=str(SCRAPERS_DIR),
            timeout=7000
        )

    duration = (datetime.now(timezone.utc) - start).total_seconds()
    output = log_path.read_text(encoding="utf-8")

    if result.returncode != 0:
        logger.error(f"❌ Spider {spider_name} falló: {output[-500:]}")
        raise RuntimeError(f"Spider {spider_name} terminó con error code {result.returncode}")

    # Parsear métricas del log escrito
    metrics = _parse_scrapy_stats(output)
    metrics["spider"] = spider_name
    metrics["duration_seconds"] = round(duration, 1)

    items = metrics.get("item_scraped_count", 0)
    logger.info(
        f"✅ Spider {spider_name} completado | "
        f"items={items} | "
        f"duración={duration:.0f}s"
    )

    # Si scrapeó 0 items y hay errores 403, reintenta (proxy IP rotation)
    if items == 0 and "Gave up retrying" in output:
        raise RuntimeError(f"Spider {spider_name}: 0 items scrapeados — todas las IPs bloqueadas")

    return metrics


@task(
    name="run-dbt",
    retries=1,
    retry_delay_seconds=120,
    timeout_seconds=1800,        # 30 minutos máximo
    tags=["dbt", "transform"]
)
def run_dbt(command: str = "run") -> dict:
    """
    Ejecuta un comando dbt.
    Comandos útiles: 'run', 'test', 'run --select silver', 'run --select gold'
    """
    logger = get_run_logger()
    logger.info(f"🔧 dbt {command}")

    result = subprocess.run(
        ["dbt", command, "--profiles-dir", str(DBT_DIR)],
        capture_output=True,
        text=True,
        cwd=str(DBT_DIR),
        timeout=1700
    )

    if result.returncode != 0:
        logger.error(f"❌ dbt {command} falló:\n{result.stdout[-1000:]}")
        raise RuntimeError(f"dbt {command} falló con código {result.returncode}")

    logger.info(f"✅ dbt {command} completado")
    return {"command": command, "output": result.stdout[-500:]}


@task(
    name="dbt-seed",
    tags=["dbt"]
)
def run_dbt_seed() -> None:
    """Carga los seeds (brand_lookup, etc.) en la DB."""
    logger = get_run_logger()
    result = subprocess.run(
        ["dbt", "seed", "--profiles-dir", str(DBT_DIR)],
        capture_output=True, text=True, cwd=str(DBT_DIR), timeout=300
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt seed falló: {result.stdout}")
    logger.info("✅ dbt seed completado")


@task(name="health-check", tags=["monitoring"])
def check_spider_health(spider_name: str) -> dict:
    """Verifica que un spider puede conectar al sitio objetivo."""
    logger = get_run_logger()

    result = subprocess.run(
        ["scrapy", "fetch", f"--spider={spider_name}", "--nolog",
         _get_spider_test_url(spider_name)],
        capture_output=True, text=True,
        cwd=str(SCRAPERS_DIR), timeout=30
    )

    is_healthy = result.returncode == 0 and len(result.stdout) > 100

    status = "ok" if is_healthy else "error"
    logger.info(f"Health check {spider_name}: {status}")

    return {"spider": spider_name, "status": status}


# ──────────────────────────────────────────
# FLOWS PRINCIPALES
# ──────────────────────────────────────────

@flow(
    name="AutoPulse Daily Pipeline",
    description="Pipeline completo: scraping todos los sitios + transformación dbt",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True,
)
def autopulse_daily_pipeline():
    """
    Flow principal. Corre todos los días a las 6am.
    
    1. Ejecuta todos los spiders en paralelo
    2. Espera a que terminen todos
    3. Ejecuta dbt run (Bronze → Silver → Gold)
    4. Ejecuta dbt test (valida calidad de datos)
    """
    logger = get_run_logger()
    logger.info("🚀 AutoPulse Daily Pipeline iniciado")

    # Ejecutar spiders secuencialmente para no saturar RAM con Playwright
    spider_results = []
    for spider_name in SPIDERS:
        try:
            result = run_spider(spider_name)
            spider_results.append(result)
        except Exception as e:
            logger.warning(f"⚠️ Spider {spider_name} falló pero el pipeline continúa: {e}")

    if not spider_results:
        logger.error("❌ Todos los spiders fallaron. Abortando pipeline.")
        return

    total_items = sum(r.get("item_scraped_count", 0) for r in spider_results)
    logger.info(f"📊 Scraping completado: {total_items} items en total")

    # Ejecutar dbt: Bronze → Silver → Gold
    run_dbt("run")

    # Validar calidad de datos
    run_dbt("test")

    logger.info("✅ AutoPulse Daily Pipeline completado exitosamente")
    return {
        "spiders_run": len(spider_results),
        "total_items": total_items,
        "completed_at": datetime.now(timezone.utc).isoformat()
    }


@flow(name="AutoPulse Health Check", log_prints=True)
def health_check_flow():
    """
    Verifica que todos los spiders puedan alcanzar sus sitios objetivo.
    Corre cada hora para detectar bloqueos rápidamente.
    """
    results = [
        check_spider_health.submit(spider)
        for spider in SPIDERS
    ]

    statuses = [r.result() for r in results]
    failed = [s for s in statuses if s["status"] == "error"]

    if failed:
        get_run_logger().warning(
            f"⚠️ {len(failed)} spider(s) con problemas: "
            f"{[s['spider'] for s in failed]}"
        )
    else:
        get_run_logger().info(f"✅ Todos los spiders saludables")

    return statuses


@flow(name="AutoPulse Setup", log_prints=True)
def setup_flow():
    """
    Flow de inicialización. Ejecutar una sola vez al comenzar el proyecto.
    Carga los seeds de dbt (brand_lookup, etc.)
    """
    logger = get_run_logger()
    logger.info("🔧 Ejecutando setup inicial de AutoPulse")

    run_dbt_seed()
    run_dbt("run --select bronze")  # Verificar que dbt puede leer Bronze

    logger.info("✅ Setup completado. Ya puedes ejecutar autopulse_daily_pipeline.")


# ──────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────

def _parse_scrapy_stats(output: str) -> dict:
    """Extrae estadísticas del output de Scrapy."""
    import re
    stats = {}
    patterns = {
        "item_scraped_count": r"'item_scraped_count': (\d+)",
        "response_error_count": r"'response_error_count': (\d+)",
        "downloader_request_count": r"'downloader/request_count': (\d+)",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, output)
        if match:
            stats[key] = int(match.group(1))
    return stats


def _get_spider_test_url(spider_name: str) -> str:
    """Retorna una URL de prueba para el health check de cada spider."""
    urls = {
        "encuentra24": "https://encuentra24.com/panama-es/autos",
        "clasificar":  "https://clasificar.com/pa",
        "carspot":     "https://carspotpanama.com",
        "automarket":  "https://automarketpanama.com",
        "champion":    "https://championmotorspanama.com",
    }
    return urls.get(spider_name, "https://encuentra24.com")


if __name__ == "__main__":
    # Para correr manualmente: python flows/main_flow.py
    autopulse_daily_pipeline()
