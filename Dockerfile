FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml ./
RUN pip install --no-cache-dir uv && uv pip install --system .

# Install local embedding deps with CPU-only torch (lighter than GPU build)
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu && \
    pip install --no-cache-dir sentence-transformers chromadb

# Pre-download the multilingual embedding model into the image
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')"

COPY app ./app
COPY apps_sdk ./apps_sdk
COPY raw_data ./raw_data
COPY README.md ./

ENV LISTINGS_RAW_DATA_DIR=/app/raw_data
ENV LISTINGS_DB_PATH=/data/listings.db

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
