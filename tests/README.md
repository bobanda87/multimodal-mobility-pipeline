# Tests of the real-time pipeline

Plain scripts, no framework: each prints what it checks and ends with "... passed", or fails an assert.
They use fake S3 clients and synthetic data, so they run offline and write nothing.

    for t in tests/test_*.py; do PYTHONPATH=. POLARS_SKIP_CPU_CHECK=1 .venv/bin/python $t || break; done

`test_api_file_selection.py` needs the API dependencies (`pip install -r api/requirements.txt`, e.g. in a venv).

`test_prediction_*.py` cover the prediction tables (no look-ahead in any feature), the time split, the metrics and a
training run on synthetic data; they need `duckdb`, `lightgbm`, `shap` and `scikit-learn` in the `.venv`.
