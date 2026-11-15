# Nightly prediction job (ECS Fargate, arm64). Build context: the repository root.
#   docker buildx build --platform linux/arm64 -f prediction/Dockerfile -t <repo>:<tag> --push .
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY prediction/requirements.txt prediction/requirements.txt
RUN pip install --no-cache-dir -r prediction/requirements.txt
# the file selection of the API is shared (compacted file wins over its inputs)
COPY api/__init__.py api/config.py api/files.py api/
COPY prediction/ prediction/
ENV PYTHONPATH=/app TZ=Europe/Oslo PYTHONUNBUFFERED=1
CMD ["python", "-m", "prediction.nightly"]
