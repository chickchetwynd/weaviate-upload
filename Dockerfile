FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY 1-extract_from_bq.py .
COPY 2-populate.py .

COPY key.json .
COPY .env .

CMD ["sh", "-c", "python 1-extract_from_bq.py && python 2-populate.py"]