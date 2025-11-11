
# Bridge Monitoring — PySpark Data Pipeline

## Project overview
This repository contains a small end-to-end PySpark data-pipeline project for simulated bridge sensor telemetry. It demonstrates a simple **bronze → silver → gold** data lake pipeline pattern, streaming data ingestion, enrichment and aggregation. The sample dataset and generator produce sensor events for temperature, vibration and tilt for multiple bridges.

Key components:
- `data_generator/` — Python script that simulates and writes JSON event batches (streams) for temperature, vibration, and tilt sensors.
- `streams/` — Example streaming batches (JSON) organized by sensor type and partitioned by date.
- `pipelines/` — PySpark pipeline scripts for Bronze ingestion (streaming), Silver enrichment (parquet enrichment & joins with metadata), and Gold aggregation (daily aggregated metrics).
- `metadata/` — Static metadata about bridges (CSV) used for enrichment and lookups.
- `silver/` and other content directories — Intermediate Parquet outputs created by the pipelines.

This repo is educational and intended to be run locally in a notebook or a small Spark environment (e.g., Google Colab with PySpark or local Spark).

---

## Repository structure (top-level)
```
bridge-monitoring-pyspark/
├─ data_generator/
│  └─ data_generator.py
├─ metadata/
│  └─ bridges.csv
├─ notebooks/
├─ pipelines/
│  ├─ bronze-ingest.py
│  ├─ silver-enrichment.py
│  └─ gold-aggregation.py
├─ scripts/
├─ streams/                     # pre-generated JSON batches
├─ silver/                      # enriched parquet outputs (by sensor)
└─ ... 
```

---

## Requirements & prerequisites

### Software
- Python 3.8+ (recommended)
- Java JDK 8 or 11 (required by Spark)
- Apache Spark 3.x (PySpark)
- Optional: Docker (to containerize) or Google Colab (for quick experiments)

### Python packages
Install required Python packages (you can use a virtual environment):

```bash
python -m venv venv
source venv/bin/activate          # Linux/macOS
venv\Scripts\activate             # Windows

pip install pyspark pandas
# Optional: install jupyterlab if you want to run notebooks
pip install jupyterlab
```

---

## Quickstart — generate sample streaming data

The `data_generator.py` script writes JSON batch files into `streams/` by default. Example usage:

```bash
# run generator (writes to streams/ by default)
python bridge-monitoring-pyspark/data_generator/data_generator.py --output_dir bridge-monitoring-pyspark/streams --num_bridges 5 --rate 1 --batches 10
```

Key options:
- `--output_dir` : where JSON batch files will be written
- `--num_bridges` : number of unique bridge IDs to simulate
- `--rate` : events per second per bridge (approx)
- `--batches` : number of batch files to write (per sensor/date partition)

After running, check the `streams/` folder; you should see JSON files partitioned by sensor type and date (e.g., `streams/bridge_temperature/date=YYYY-MM-DD/batch_0_YYYYMMDD_HHMMSS.json`).

---

## Bronze: streaming ingestion (bronze-ingest.py)

The `pipelines/bronze-ingest.py` script reads the JSON files using Spark Structured Streaming with a declared schema and prints to the console (or can write to bronze Parquet). To run it locally:

```bash
# Example (runs streaming read and prints to console)
python bridge-monitoring-pyspark/pipelines/bronze-ingest.py
```

Notes:
- The script uses `spark.readStream.schema(RAW_SCHEMA).csv(path)` to treat incoming JSON/CSV-like files as a stream source.
- For a production setup, modify writeStream to write Parquet to a `bronze/` path (checkpointing recommended).

---

## Silver: enrichment (silver-enrichment.py)

This step reads bronze parquet files, performs joins with `metadata/bridges.csv` and adds enrichment columns (e.g., partition_date, enriched_date). It then writes out Parquet files to `silver/` folders per sensor.

Run:

```bash
python bridge-monitoring-pyspark/pipelines/silver-enrichment.py
```

Confirm the `silver/bridge_temperature`, `silver/bridge_vibration`, and `silver/bridge_tilt` folders are populated with Parquet files.

---

## Gold: aggregation (gold-aggregation.py)

The gold pipeline reads the Silver parquet data and computes aggregated daily metrics (e.g., average temperature, avg vibration, avg tilt) per bridge and partition date. It writes results to `gold/aggregated_metrics` as Parquet.

Run:

```bash
python bridge-monitoring-pyspark/pipelines/gold-aggregation.py
```

---

## Example data schema

**Raw records (per event):**
```json
{
  "event_time": "2025-11-08T17:08:50.123456",
  "bridge_id": "BRIDGE_001",
  "sensor_type": "temperature",
  "value": 22.345,
  "ingest_time": "2025-11-08T17:08:50.654321"
}
```

**Metadata (bridges.csv)**
- `id` — Bridge identifier (BRIDGE_001)
- `name` — Bridge name
- `latitude`, `longitude` — coordinates
- `location` — city or region
- other descriptive attributes

---

## Notebooks
The `notebooks/` folder contains Jupyter notebooks for interactive exploration (if present). Open with JupyterLab or Jupyter Notebook:

```bash
jupyter lab
# then open the notebook under bridge-monitoring-pyspark/notebooks/
```

---

## Running in Google Colab
If you prefer Colab:
1. Mount your drive or upload the repo.
2. Install PySpark:
```python
!apt-get install -y openjdk-11
!pip install pyspark
```
3. Run pipeline scripts or copy logic into notebook cells.

---

## Troubleshooting & tips

- **Spark versions / Java:** Ensure Java version matches Spark requirements. Mismatched Java can cause JVM errors when launching PySpark.
- **Permissions:** Make sure the output directories (`streams/`, `silver/`, `gold/`) exist and are writable.
- **Small local testing:** Use a small number of batches and `--rate 1` when developing.
- **Schema mismatches:** When reading JSON/CSV files as streaming sources, declare the schema to avoid type inference issues.

---

## Extending the project

Possible improvements:
- Add checkpointing and continuous streaming writes to Parquet for robust ingestion.
- Use Delta Lake instead of raw Parquet for ACID operations and efficient upserts.
- Add anomaly detection on aggregated metrics (e.g., alerts when vibration crosses thresholds).
- Visualize gold metrics using a simple dashboard (Plotly, Streamlit or Power BI).

---

## Contact & License
- Author: (project contributor)
- License: MIT (adjust as needed)

---

