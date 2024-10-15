# Base Image de Python 3.9
FROM python:3.9-slim

# Instalar dependencias: Java, curl, y otras utilidades necesarias
RUN apt-get update && apt-get install -y openjdk-17-jre-headless curl bash git && \
    apt-get clean

# Definir variables de entorno para Java y Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Descargar e instalar Apache Spark desde un mirror confiable
RUN curl -fSL https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz -o spark.tgz && \
    tar -xvzf spark.tgz -C /opt/ && \
    mv /opt/spark-3.3.1-bin-hadoop3 /opt/spark && \
    rm spark.tgz

# Instalar Poetry
RUN pip install poetry

# Definir directorio de trabajo
WORKDIR /app

# Copiar archivos de Poetry
COPY pyproject.toml poetry.lock* /app/

# Instalar dependencias de Poetry
RUN poetry install --no-root

# Copiar el código fuente y datos al contenedor
COPY . /app/

# Instalar pre-commit
RUN poetry run pre-commit install

# Exponer puerto para el Web UI de PySpark (opcional)
EXPOSE 4040

# Definir el comando condicional en Dockerfile
CMD if [ "$MODE" = "test" ] ; then \
      if [ -n "$TEST_FILE" ]; then \
        poetry run pytest $TEST_FILE ; \
      else \
        poetry run pytest -v ; \
      fi \
    else \
      poetry run python run.py ; \
    fi

