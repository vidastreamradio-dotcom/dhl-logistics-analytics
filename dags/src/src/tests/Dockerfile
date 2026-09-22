# Base Python Image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    AIRFLOW_HOME=/opt/airflow

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/airflow

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline project code
COPY dags/ ${AIRFLOW_HOME}/dags/
COPY src/ ${AIRFLOW_HOME}/src/
COPY tests/ ${AIRFLOW_HOME}/tests/

# Expose Airflow Webserver port
EXPOSE 8080

# Run unit tests prior to launch
RUN pytest tests/

CMD ["airflow", "standalone"]
