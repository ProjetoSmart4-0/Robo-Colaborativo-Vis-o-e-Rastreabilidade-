# Imagem base do Python
FROM python:3.10-slim

# Instala dependências de sistema para o OpenCV operar em modo headless
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Diretório de trabalho na imagem
WORKDIR /app

# Copia e instala as dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código da aplicação
COPY . .

# Expõe a porta interna
EXPOSE 8080

# Inicia a API FastAPI usando a variável de porta dinâmica do Render
CMD ["sh", "-c", "uvicorn cobot_service_app:app --host 0.0.0.0 --port ${PORT:-8080}"]