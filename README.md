# ETL Pipeline with Airflow

A beginner-friendly ETL pipeline using Apache Airflow and Docker.

## What This Does

Extracts sample customer data, transforms it (adds tax, categories), and loads it into a database.

## Quick Start

### 1. Setup
```bash
mkdir airflow-etl-pipeline
cd airflow-etl-pipeline
mkdir -p dags logs plugins data
```

### 2. Add Files
- Put `extract_transform_load.py` in `dags/` folder
- Put `docker-compose.yml`, `requirements.txt`, `.env`, `.gitignore`, `README.md` in project root

### 3. Start Airflow
```bash
docker-compose up -d
```

### 4. Access UI
- Open: http://localhost:8080
- Login: `airflow` / `airflow`

### 5. Run Pipeline
1. Toggle DAG to ON
2. Click Play button
3. Select "Trigger DAG"

## View Results

**See data in CSV:**
```bash
cat data/transformed_data.csv
```

**See data in database:**
```bash
sqlite3 data/etl_database.db
SELECT * FROM customer_purchases;
.exit
```

## Stop Airflow
```bash
docker-compose down
```

## Need Help?
- Check logs: `docker-compose logs airflow-scheduler`
- Restart: `docker-compose restart`