FROM python:3.9-slim

# Instala dependências do sistema: Java e utilitários mínimos
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
        wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copia requirements.txt para o container e instala dependências Python
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Baixa o driver JDBC do PostgreSQL
RUN mkdir -p /opt/spark/jars && \
    wget -P /opt/spark/jars https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Define o diretório de trabalho
WORKDIR /app/notebooks

# Expõe a porta do JupyterLab
EXPOSE 8888

# Comando de inicialização
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
