FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    QUEUEWORKS_DB_PATH=/var/lib/queueworks/queueworks.sqlite3 \
    QUEUEWORKS_DATA_DIR=/data

WORKDIR /app
COPY pyproject.toml requirements-lock.txt README.md LICENSE ./
COPY src ./src
RUN python -m pip install --no-cache-dir -c requirements-lock.txt .

EXPOSE 8000
CMD ["queueworks", "serve", "--host", "0.0.0.0", "--port", "8000"]
