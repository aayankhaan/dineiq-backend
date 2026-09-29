# DineIQ Analytics Backend

Backend and data-science pipeline for **DineIQ Analytics**, a restaurant intelligence platform designed to process restaurant data and generate analytical insights, predictions, comparisons, and recommendations.

The project uses FastAPI for the API layer, PostgreSQL/SQLModel for application data, Apache Spark/PySpark for big-data processing, and an independent Python machine-learning pipeline for model comparison.

## Tech Stack

- Python 3.14+
- FastAPI
- SQLModel
- PostgreSQL
- Apache Spark / PySpark
- Spark SQL
- Spark MLlib
- Pandas / NumPy
- Scikit-learn
- PyArrow / Parquet
- Uvicorn
- uv

## Main Features

The backend provides processing and analytics for:

- Menu profitability and performance classification
- Customer segmentation and RFM analysis
- Market-basket analysis and bundle recommendations
- Peak-period and ordering-channel analysis
- Demand forecasting and forecast evaluation
- Wastage analysis and wastage-risk prediction
- Pricing and price-sensitivity analysis
- Promotion effectiveness and promotion-trap detection
- Rating and sales anomaly detection
- Multi-location restaurant intelligence
- Customer churn-risk analysis
- Spark and Python model comparison
- Evidence-based recommendations and priority assignment
- What-if scenario and impact analysis
- Authentication, role-based access and audit logging
- Analytical result exports and API access

## Prerequisites

Before running the project, make sure the following are installed:

- Python 3.14 or later
- `uv`
- PostgreSQL
- Java (required by Apache Spark / PySpark)

## Installation

Clone the repository and open the project directory:

```bash
git clone <repository-url>
cd dineiq-backend
```

Install the project dependencies:

```bash
uv sync
```

## Environment Variables

Create a `.env` file in the project root.

Example:

```env
DATABASE_URL=postgresql+psycopg://username:password@localhost:5432/dineiq
SECRET_KEY=your-secret-key

RESEND_API_KEY=your-resend-api-key
EMAIL_FROM=your-email@example.com
FRONTEND_URL=http://localhost:5173
```

Replace the example values with your local configuration.

## Create Administrator

Create the first administrator account before signing in:

```bash
uv run python -m app.create_admin
```

The command will ask for the administrator's name, email, and password.

## Run the API

Start the FastAPI development server:

```bash
uv run uvicorn app.main:app --reload --port 8000
```

The API will be available at:

```text
http://127.0.0.1:8000
```

Health check:

```text
http://127.0.0.1:8000/health
```

FastAPI interactive documentation:

```text
http://127.0.0.1:8000/docs
```

## Run the Analytics Pipeline

Run the complete data, analytics, machine-learning, recommendation, and validation pipeline:

```bash
uv run python run_pipeline.py
```

List all available pipeline steps without running them:

```bash
uv run python run_pipeline.py --list
```

Resume the pipeline from a particular step:

```bash
uv run python run_pipeline.py --from-step <step-name>
```

For example:

```bash
uv run python run_pipeline.py --from-step customer_segmentation
```

## Project Structure

```text
dineiq-backend/
├── app/
│   ├── main.py
│   ├── analytics_api.py
│   ├── analytics_service.py
│   ├── create_admin.py
│   ├── database.py
│   ├── email.py
│   ├── models.py
│   ├── results.py
│   ├── user.py
│   └── utils.py
│
├── analytics/
│   ├── anomalies/
│   ├── channels/
│   ├── customer/
│   ├── locations/
│   ├── market_basket/
│   ├── menu/
│   ├── peak_period/
│   ├── pricing/
│   ├── promotions/
│   ├── ratings/
│   ├── recommendations/
│   ├── scenarios/
│   └── wastage/
│
├── ml/
│   ├── comparison/
│   ├── forecasting/
│   ├── python/
│   ├── spark/
│   └── wastage/
│
├── spark/
│   ├── ingest.py
│   ├── integration.py
│   ├── quality.py
│   ├── schemas.py
│   ├── sql_queries.py
│   └── validate_integration.py
│
├── preprocessing/
├── feature_engineering/
├── eda/
├── data_generator/
├── config/
│   └── settings.py
│
├── data/
├── models/
├── run_pipeline.py
├── pyproject.toml
├── requirements.txt
├── uv.lock
├── .env
└── README.md
```

## Directory Overview

`app/` contains the FastAPI application, authentication, database models, analytics API routes, result services, exports, and user management.

`data_generator/` creates the interconnected restaurant dataset and injects controlled data-quality issues used by the analytics pipeline.

`spark/` handles Spark ingestion, schemas, data-quality assessment, dataset integration, and Spark SQL processing.

`preprocessing/` contains data-cleaning and cleaned-data validation logic.

`feature_engineering/` creates and validates the analytical features used by later analytics and machine-learning stages.

`eda/` contains exploratory data analysis and its validation.

`analytics/` contains restaurant intelligence modules covering menu, customers, locations, pricing, promotions, wastage, anomalies, recommendations, scenarios, and related analysis.

`ml/` contains the independent Spark and Python machine-learning pipelines, forecasting, wastage-risk prediction, and dual-pipeline comparison.

`data/` stores generated and processed datasets, analytics results, ML outputs, reports, and pipeline execution information.

`models/` stores generated machine-learning model artifacts.

`run_pipeline.py` runs the complete DineIQ processing workflow in the required order and records pipeline execution history.

## Data Flow

The main processing flow is:

```text
Dataset Generation
        ↓
Data Quality Assessment
        ↓
Spark Ingestion
        ↓
Data Cleaning
        ↓
Data Integration
        ↓
Spark SQL Processing
        ↓
Feature Engineering
        ↓
Exploratory Data Analysis
        ↓
Restaurant Analytics
        ↓
Spark + Python ML Pipelines
        ↓
Dual-Pipeline Comparison
        ↓
Forecasting / Wastage Prediction
        ↓
Recommendations & What-If Analysis
        ↓
FastAPI / Dashboard Results
```

## API Authentication

DineIQ uses authenticated user accounts with role-based access. Login creates an access token that is also stored as an HTTP-only cookie.

Main authentication routes include:

```text
POST /api/auth/login
GET  /api/auth/me
POST /api/auth/logout
POST /api/auth/change-password
```

Additional analytics and management endpoints are available through the FastAPI documentation at `/docs`.

## Data Directories

The application organizes generated output into separate directories configured in `config/settings.py`, including:

```text
data/raw
data/processed
data/integrated
data/features
data/analytics
data/eda
data/ml
data/reports
models/
```

These directories are used by different stages of the data and machine-learning pipeline.
