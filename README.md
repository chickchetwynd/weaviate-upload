# Mentra Candidate/Weaviate Data Pipeline

A data pipeline that extracts candidate data from BigQuery and loads it into Weaviate for semantic search capabilities.

## Overview

This pipeline consists of two main steps:
1. Extract transformed candidate data from BigQuery (`1-extract_from_bq.py`) - the data extracted is already transformed using DBT ready for ingestion into Weaviate
2. Populate Weaviate with the extracted data (`2-populate.py`)


## Local Dev Setup

1. Create and activate virtual environment:
```bash
# Create venv
python -m venv venv
source venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure local credentials:
    * rename `.env.template` to `.env`
```bash
     mv .env.template .env
```
    * Add your credentials to the `.env` file
    * Create a file called `key.json`
```bash
touch key.json
```
    * Add your BigQuery service account credentials to `key.json`

4. Run the pipeline:
```bash
python 1-extract_from_bq.py && python 2-populate.py
```

### Local Execution

```bash
python 1-extract_from_bq.py  # Extract data from BigQuery
python 2-populate.py         # Load data into Weaviate
```

### Running in Production/Docker

Use the Dockerfile to build and run the pipeline in production:

```bash
docker build -t mentra-pipeline .
docker run mentra-pipeline
```