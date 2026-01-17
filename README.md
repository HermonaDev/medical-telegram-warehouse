# 🏥 Medical Telegram Data Warehouse & AI Pipeline

[![Medical Data CI](https://github.com/YOUR_USERNAME/medical-telegram-warehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/medical-telegram-warehouse/actions)
![Python](https://img.shields.io/badge/python-3.9+-blue.svg)
![dbt](https://img.shields.io/badge/dbt-1.11.2-orange.svg)
![YOLOv8](https://img.shields.io/badge/AI-YOLOv8-green.svg)

An end-to-end Data Engineering pipeline that extracts medical data from Telegram channels, transforms it into a Star Schema warehouse, and enriches it using Computer Vision (YOLOv8).

## 📊 Pipeline Overview
1. **Extract**: Multi-channel Telegram scraping using `Telethon`.
2. **Load**: Raw data ingestion into a `PostgreSQL` JSONB landing zone (Dockerized).
3. **Transform**: Modular data modeling with `dbt` (Staging -> Marts).
4. **Enrich**: Object detection on medical images using `YOLOv8`.

---

## 🏗 Project Architecture
```text
├── .github/workflows/   # CI/CD (GitHub Actions)
├── data/
│   ├── raw/             # Scraped JSON and Images
│   └── yolo_results.csv # CV Inference output
├── kara_dbt/            # dbt Project
│   ├── models/
│   │   ├── staging/     # Cleaning & Type Casting
│   │   └── marts/       # Star Schema (Fact/Dim tables)
│   └── schema.yml       # Data Documentation & Tests
├── scripts/             # Python ETL & AI Scripts
├── docker-compose.yml   # PostgreSQL Warehouse Setup
└── requirements.txt     # Dependency Management
```


## 🚀 Key Achievements (Interim Report - Jan 17)

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

## ⚖️ Code Best Practices

* **Modular SQL**: Used dbt `ref()` and `source()` for dynamic lineage.
* **Scalability**: JSONB storage for flexible schema evolution.
* **Automation**: GitHub Actions for Continuous Integration.
