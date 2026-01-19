# 🏥 Medical Telegram Data Warehouse & AI Pipeline

[![Medical Data CI](https://github.com/HermonaDev/medical-telegram-warehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/medical-telegram-warehouse/actions)
![Python](https://img.shields.io/badge/python-3.9+-blue.svg)
![dbt](https://img.shields.io/badge/dbt-1.11.2-orange.svg)
![YOLOv8](https://img.shields.io/badge/AI-YOLOv8-green.svg)

An end-to-end data engineering pipeline designed to scrape, process, and analyze medical product data from Telegram channels. This project utilizes AI-driven object detection (YOLO), professional data modeling (dbt), and a robust orchestration layer (Dagster).

## 📊 Pipeline Overview
1.  **Extraction**: Python-based scraper using Telethon to collect messages and images from specified medical Telegram channels.
2.  **AI Processing**: YOLOv8 model processes scraped images to detect and label medical products.
3.  **Data Warehouse**: PostgreSQL database structured using a **Star Schema** (Dimensions and Fact tables).
4.  **Transformation**: dbt (data build tool) manages the transformation from raw staging data to analytical marts.
5.  **Orchestration**: Dagster handles the entire pipeline lineage and scheduling.
6.  **Data Access**: FastAPI provides endpoints for real-time analytics on channel activity and product trends.
---

## 🛠 Tech Stack

* **Language:** Python 3.11
* **Orchestration:** Dagster
* **Transformation:** dbt (Postgres adapter)
* **Database:** PostgreSQL
* **AI/ML:** YOLOv8 (Ultralytics)
* **API:** FastAPI & Uvicorn
* **Scraping:** Telethon (Telegram API)

## 📂 Project Structure
```
├── api/                # API Endpoints (FastAPI)
├── data/               # Raw and processed medical data
├── docs/               # Documentation & Contribution guidelines
├── kara_dbt/           # dbt Transformation project
├── logs/               # Centralized pipeline logs
├── notebooks/          # Data Exploration & AI Model Analysis
├── scripts/            # Python ETL, Scraper, and YOLO Inference
├── orchestration/         # Dagster definitions and assets
├── tests/              # Unit and Integration tests
├── .env                # Environment variables (DB_URL, API_ID, etc.)
├── README.md
├── docker-compose.yml  # Warehouse environment (PostgreSQL)
└── requirements.txt    # Project dependencies
```


## 🚀 Key Achievements

* **Ingested**: 482 messages from 7 medical channels.
* **Processed**: 468 objects detected in images (Bottles, Equipment, Labels).
* **Warehouse**: Fully operational PostgreSQL DB with automated dbt transformations.
* **CI/CD**: Integrated GitHub Actions for automated build validation.

## 🛠 Setup & Usage

### 1. Environment Setup

```bash
python -m venv venv
source venv/scripts/activate
pip install -r requirements.txt

```

### 2. Database & Warehouse

```bash
docker-compose up -d

```

### 3. Transformation (dbt)

```bash
cd kara_dbt
dbt deps
dbt run
dbt test

```

## 📈 Sample Results

| Channel | Message Text | Detected Item | Confidence |
| --- | --- | --- | --- |
| cafimadEt | Riluzole... | Bottle | 0.74 |
| CheMed123 | Azithromycin... | Person | 0.86 |

---
### 4. Environment Setup

Create a `.env` file in the root directory to store your credentials:

```env
DATABASE_URL=postgresql://kara_admin:karapassword@localhost:5433/medical_data
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash

```

### 5. Run the Pipeline (Dagster)

The entire pipeline is automated. Launch the Dagster UI to visualize and run all steps:

```bash
dagster dev -f orchestration/definitions.py

```

*Navigate to `http://localhost:3000`, go to the Asset Graph, and click **Materialize All**.*

### 6. Start the API

Once the pipeline finishes, serve the data via the API:

```bash
uvicorn api.main:app --reload

```

*Interactive Documentation: Open `http://127.0.0.1:8000/docs` to test the endpoints.*

---

## 📊 Analytical Endpoints

The API provides several endpoints to gain insights from the warehouse:

* **`GET /analytics/top-products`**: Returns the most frequently detected medical items (labels) from the YOLO analysis.
* **`GET /analytics/channel-activity`**: Provides a breakdown of message volume by Telegram channel, joining fact tables with channel dimensions.
* **`GET /debug/raw-check`**: A diagnostic tool that lists all tables within the `staging` and `marts` schemas.

---

## 🛡 Quality Assurance

* **Data Integrity**: dbt models utilize primary key tests and relationship tests to ensure no orphan records exist in the warehouse.
* **Orchestration Safety**: Dagster prevents "downstream" failures; the YOLO model will not attempt to process images if the scraper fails to download them.
* **Error Handling**: The FastAPI layer includes custom exception handlers to manage database connection issues gracefully.


## ⚖️ Code Best Practices

* **Modular SQL**: Used dbt `ref()` and `source()` for dynamic lineage.
* **Scalability**: JSONB storage for flexible schema evolution.
* **Automation**: GitHub Actions for Continuous Integration.
Here is the complete project documentation in Markdown format. You can copy the entire block below and save it as `README.md` in your project's root directory.

