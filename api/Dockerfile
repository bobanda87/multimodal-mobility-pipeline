# Build from the repository root: docker build -f api/Dockerfile .
FROM public.ecr.aws/lambda/python:3.12

COPY api/requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# The DuckDB httpfs extension is not baked into the image: that step hung under x86_64 emulation on
# Apple Silicon. api/db.py installs it on first use (into /tmp), which costs a download per cold start.

COPY api ${LAMBDA_TASK_ROOT}/api
CMD ["api.main.handler"]
