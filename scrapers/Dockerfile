FROM python:3.11-slim

WORKDIR /app

# Dependencias del sistema para Playwright
RUN apt-get update && apt-get install -y \
    wget curl gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instalar navegadores de Playwright
RUN playwright install chromium
# playwright install-deps falla en Debian Trixie por paquetes renombrados
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libnspr4 libatk1.0-0t64 libatk-bridge2.0-0t64 \
    libcups2t64 libxkbcommon0 libxcomposite1 libxdamage1 \
    libxfixes3 libxrandr2 libgbm1 libasound2t64 \
    libpango-1.0-0 libcairo2 fonts-unifont \
    && rm -rf /var/lib/apt/lists/*

COPY . .

CMD ["scrapy", "version"]
