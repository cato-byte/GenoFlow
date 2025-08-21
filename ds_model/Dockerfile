FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
ENV PIP_DEFAULT_TIMEOUT=300 \
    PIP_RETRIES=10 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

CMD ["python", "run_model.py"]