"""
AutoPulse Panamá — Orquestación con Prefect

Flows disponibles:
    - autopulse_daily_pipeline   : Orquestador principal — spiders en paralelo → dbt
    - spider_encuentra24_flow    : Spider Encuentra24, deployable individualmente
    - spider_carspot_flow        : Spider CarSpot, deployable individualmente
    - spider_automarket_flow     : Spider AutoMarket, deployable individualmente
    - spider_champion_flow       : Spider Champion, deployable individualmente
    - dbt_pipeline_flow          : dbt run + test, deployable individualmente
    - health_check_flow          : Verifica conectividad de todos los spiders

Cómo deployar cada spider por separado:
    prefect deploy flows/main_flow.py:spider_encuentra24_flow \\
        --name "Spider Encuentra24" --pool autopulse-pool --cron "0 6 * * *"

    prefect deploy flows/main_flow.py:dbt_pipeline_flow \\
        --name "DBT Pipeline" --pool autopulse-pool --cron "0 8 * * *"

    prefect deploy flows/main_flow.py:autopulse_daily_pipeline \\
        --name "AutoPulse Daily" --pool autopulse-pool --cron "0 6 * * *"
"""
import subprocess
import os
from datetime import datetime, timezone
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner


# ──────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────

SCRAPERS_DIR = Path(os.getenv("SCRAPERS_DIR", "/scrapers"))
DBT_DIR      = Path(os.getenv("DBT_DIR", "/dbt"))

SPIDERS = ["encuentra24", "carspot", "automarket", "champion"]

# Timeout del subprocess por spider (segundos).
# encuentra24 es HTTP puro — si está bloqueado falla rápido.
# Playwright spiders necesitan más tiempo para cargar y renderizar.
SPIDER_TIMEOUTS = {
    "encuentra24": 1800,   # 30 min
    "carspot":     3600,   # 60 min
    "automarket":  3600,   # 60 min
    "champion":    3600,   # 60 min
}


# ──────────────────────────────────────────
# TASKS
# ──────────────────────────────────────────

@task(
    name="run-spider",
    retries=1,
    retry_delay_seconds=120,
    # Task timeout = spider subprocess timeout + 5 min margen para escritura de logs
    timeout_seconds=4200,
    tags=["scraping"],
)
def run_spider(spider_name: str) -> dict:
    """Ejecuta un spider de Scrapy y retorna métricas del run."""
    logger = get_run_logger()
    logger.info(f"🕷️ Iniciando spider: {spider_name}")

    start    = datetime.now(timezone.utc)
    log_path = Path(f"/logs/{spider_name}_{start.strftime('%Y%m%d_%H%M')}.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)

    spider_timeout = SPIDER_TIMEOUTS.get(spider_name, 3600)

    with open(log_path, "w", encoding="utf-8") as log_file:
        result = subprocess.run(
            ["scrapy", "crawl", spider_name],
            stdout=log_file,
            stderr=log_file,
            cwd=str(SCRAPERS_DIR),
            timeout=spider_timeout,
        )

    duration = (datetime.now(timezone.utc) - start).total_seconds()
    output   = log_path.read_text(encoding="utf-8")

    if result.returncode != 0:
        logger.error(f"❌ Spider {spider_name} falló (código {result.returncode}): {output[-500:]}")
        raise RuntimeError(f"Spider {spider_name} terminó con error code {result.returncode}")

    metrics = _parse_scrapy_stats(output)
    metrics["spider"]           = spider_name
    metrics["duration_seconds"] = round(duration, 1)

    items = metrics.get("item_scraped_count", 0)
    logger.info(f"✅ Spider {spider_name} | items={items} | duración={duration:.0f}s")

    blocked_signals = ["Gave up retrying", "closespider_timeout", "closespider_errorcount"]
    if items == 0 and any(sig in output for sig in blocked_signals):
        raise RuntimeError(f"Spider {spider_name}: 0 items — IPs bloqueadas o timeout")

    return metrics


@task(
    name="run-dbt",
    retries=1,
    retry_delay_seconds=120,
    timeout_seconds=1800,
    tags=["dbt", "transform"],
)
def run_dbt(command: str = "run") -> dict:
    """
    Ejecuta un comando dbt.
    Acepta comandos simples ('run', 'test') y compuestos ('run --select silver').
    """
    logger = get_run_logger()
    logger.info(f"🔧 dbt {command}")

    result = subprocess.run(
        ["dbt", *command.split(), "--profiles-dir", str(DBT_DIR)],
        capture_output=True,
        text=True,
        cwd=str(DBT_DIR),
        timeout=1700,
    )

    if result.returncode != 0:
        logger.error(f"❌ dbt {command} falló:\n{result.stdout[-1000:]}")
        raise RuntimeError(f"dbt {command} falló con código {result.returncode}")

    logger.info(f"✅ dbt {command} completado")
    return {"command": command, "output": result.stdout[-500:]}


@task(name="dbt-seed", tags=["dbt"])
def run_dbt_seed() -> None:
    logger = get_run_logger()
    result = subprocess.run(
        ["dbt", "seed", "--profiles-dir", str(DBT_DIR)],
        capture_output=True, text=True, cwd=str(DBT_DIR), timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt seed falló: {result.stdout}")
    logger.info("✅ dbt seed completado")


@task(name="health-check", tags=["monitoring"])
def check_spider_health(spider_name: str) -> dict:
    logger = get_run_logger()
    result = subprocess.run(
        ["scrapy", "fetch", f"--spider={spider_name}", "--nolog",
         _get_spider_test_url(spider_name)],
        capture_output=True, text=True,
        cwd=str(SCRAPERS_DIR), timeout=30,
    )
    is_healthy = result.returncode == 0 and len(result.stdout) > 100
    status     = "ok" if is_healthy else "error"
    logger.info(f"Health check {spider_name}: {status}")
    return {"spider": spider_name, "status": status}


# ──────────────────────────────────────────
# SPIDER FLOWS — cada uno deployable solo
# ──────────────────────────────────────────

@flow(name="Spider: Encuentra24", log_prints=True)
def spider_encuentra24_flow() -> dict:
    return run_spider("encuentra24")


@flow(name="Spider: Carspot", log_prints=True)
def spider_carspot_flow() -> dict:
    return run_spider("carspot")


@flow(name="Spider: Automarket", log_prints=True)
def spider_automarket_flow() -> dict:
    return run_spider("automarket")


@flow(name="Spider: Champion", log_prints=True)
def spider_champion_flow() -> dict:
    return run_spider("champion")


# ──────────────────────────────────────────
# DBT FLOW — deployable solo
# ──────────────────────────────────────────

@flow(name="AutoPulse DBT Pipeline", log_prints=True)
def dbt_pipeline_flow() -> dict:
    """dbt run + test. Corre a las 5am después de que los spiders terminaron."""
    run_dbt("run --select silver_listings gold_market_summary gold_dealer_comparison gold_seller_analysis")
    run_dbt("test")
    return {"completed_at": datetime.now(timezone.utc).isoformat()}


# ──────────────────────────────────────────
# ORQUESTADOR PRINCIPAL
# ──────────────────────────────────────────

@flow(
    name="AutoPulse Daily Pipeline",
    description=(
        "Lanza los 4 spiders en paralelo. "
        "Cuando todos terminan (fallen o no), corre dbt si hubo al menos 1 item."
    ),
    task_runner=ConcurrentTaskRunner(),
    log_prints=True,
)
def autopulse_daily_pipeline():
    """
    Comportamiento ante fallos:
    - Un spider que falla NO detiene a los demás (corren en paralelo).
    - dbt corre si total_items > 0, incluso si algunos spiders fallaron.
    - Si todos los spiders retornan 0 items, dbt se cancela.

    Nota de recursos: los 3 spiders Playwright (carspot, automarket, champion)
    abren Chromium simultáneamente. En un servidor con <4 GB RAM considera
    desplegar cada spider individualmente con su propio schedule.
    """
    logger = get_run_logger()
    logger.info("🚀 AutoPulse Daily Pipeline iniciado")

    # Lanzar los 4 spiders en paralelo — .submit() no bloquea
    futures = {spider: run_spider.submit(spider) for spider in SPIDERS}

    # Recolectar resultados esperando a TODOS antes de continuar con dbt
    results = []
    for spider_name, future in futures.items():
        try:
            results.append(future.result())
        except Exception as e:
            logger.warning(f"⚠️ Spider {spider_name} falló: {e}")

    total_items = sum(r.get("item_scraped_count", 0) for r in results)
    logger.info(
        f"📊 Spiders: {len(results)}/{len(SPIDERS)} exitosos | {total_items} items totales"
    )

    if total_items == 0:
        logger.error("❌ Todos los spiders retornaron 0 items. Abortando dbt.")
        return {"spiders_ok": 0, "total_items": 0}

    run_dbt("run")
    run_dbt("test")

    logger.info("✅ AutoPulse Daily Pipeline completado")
    return {
        "spiders_ok":   len(results),
        "total_items":  total_items,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }


# ──────────────────────────────────────────
# OTROS FLOWS
# ──────────────────────────────────────────

@flow(name="AutoPulse Health Check", log_prints=True)
def health_check_flow():
    """Verifica que todos los spiders puedan alcanzar sus sitios. Corre cada hora."""
    results = [check_spider_health.submit(s) for s in SPIDERS]
    statuses = [r.result() for r in results]
    failed   = [s for s in statuses if s["status"] == "error"]
    if failed:
        get_run_logger().warning(
            f"⚠️ {len(failed)} spider(s) con problemas: {[s['spider'] for s in failed]}"
        )
    return statuses


@flow(name="AutoPulse Setup", log_prints=True)
def setup_flow():
    """Inicialización única al comenzar el proyecto."""
    logger = get_run_logger()
    run_dbt_seed()
    run_dbt("run --select bronze")
    logger.info("✅ Setup completado. Ya puedes ejecutar autopulse_daily_pipeline.")


# ──────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────

def _parse_scrapy_stats(output: str) -> dict:
    import re
    stats    = {}
    patterns = {
        "item_scraped_count":       r"'item_scraped_count': (\d+)",
        "response_error_count":     r"'response_error_count': (\d+)",
        "downloader_request_count": r"'downloader/request_count': (\d+)",
    }
    for key, pattern in patterns.items():
        m = re.search(pattern, output)
        if m:
            stats[key] = int(m.group(1))
    return stats


def _get_spider_test_url(spider_name: str) -> str:
    return {
        "encuentra24": "https://www.encuentra24.com/panama-es/autos",
        "carspot":     "https://www.carspotpanama.com",
        "automarket":  "https://automarketpanama.com",
        "champion":    "https://championmotors.com.pa",
    }.get(spider_name, "https://www.encuentra24.com")


if __name__ == "__main__":
    autopulse_daily_pipeline()
