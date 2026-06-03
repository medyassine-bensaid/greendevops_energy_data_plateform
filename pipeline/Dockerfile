FROM python:3.12-slim
RUN pip install --no-cache-dir duckdb pandas pyarrow
WORKDIR /app
COPY csv2parquet.py .
CMD ["python", "-u", "csv2parquet.py"]
