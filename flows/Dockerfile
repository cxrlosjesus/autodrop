FROM prefecthq/prefect:2-latest

# Dependencias del sistema para playwright y psycopg2
RUN apt-get update && apt-get install -y \
    gcc libpq-dev wget curl \
    && rm -rf /var/lib/apt/lists/*

# Scrapy + dependencias del scraper
RUN pip install --no-cache-dir \
    scrapy==2.11.1 \
    scrapy-playwright==0.0.40 \
    playwright==1.44.0 \
    psycopg2-binary==2.9.9 \
    sqlalchemy==2.0.30 \
    python-dotenv==1.0.1 \
    itemadapter==0.9.0 \
    w3lib==2.1.2 \
    fake-useragent==1.5.1 \
    tenacity==8.3.0 \
    loguru==0.7.2 \
    pydantic==2.7.1 \
    playwright-stealth==1.0.6 \
    brotli==1.1.0

# dbt para transformaciones
RUN pip install --no-cache-dir dbt-postgres==1.7.*

# Instalar Chromium para scrapy-playwright
# playwright install-deps falla en Debian Trixie por paquetes renombrados;
# instalamos las dependencias directamente con los nombres correctos.
RUN playwright install chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libnspr4 libatk1.0-0t64 libatk-bridge2.0-0t64 \
    libcups2t64 libxkbcommon0 libxcomposite1 libxdamage1 \
    libxfixes3 libxrandr2 libgbm1 libasound2t64 \
    libpango-1.0-0 libcairo2 fonts-unifont \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
