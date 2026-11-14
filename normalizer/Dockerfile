# Build from the repository root: docker build -f normalizer/Dockerfile .
FROM public.ecr.aws/lambda/python:3.12

COPY normalizer/requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY utils ${LAMBDA_TASK_ROOT}/utils
COPY analysis/__init__.py analysis/normalize_data.py analysis/compact_normalized.py ${LAMBDA_TASK_ROOT}/analysis/
COPY normalizer ${LAMBDA_TASK_ROOT}/normalizer
CMD ["normalizer.handler.normalize_handler"]
