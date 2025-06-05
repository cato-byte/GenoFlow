FROM bitnami/spark:latest

USER root
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN apt-get update && apt-get install -y python3-pip && pip3 install -r requirements.txt

# Copy project code
COPY . /app

# Entry point (uses Spark submit)
CMD ["spark-submit", "run_fasta_clean.py"]