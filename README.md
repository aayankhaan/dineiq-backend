# DineIQ Analytics

DineIQ is a FastAPI and React restaurant-intelligence application backed by saved Apache Spark, Spark MLlib, and independent Python data-science results. The application covers menu profitability, customer segmentation, RFM, market baskets, peak periods, forecasting, wastage, pricing, promotions, anomalies, recommendations, what-if analysis, and Spark/Python comparison.

## Technology

- Backend: FastAPI, SQLModel, PostgreSQL or SQLite
- Frontend: React 18, Vite, Recharts
- Big data: Apache Spark, PySpark, Spark SQL, Parquet
- Models: Spark MLlib and scikit-learn

## Run the application

Create `.env` in the project root with `DATABASE_URL` and `SECRET_KEY`, then run:

```powershell
uv sync
uv run python -m app.create_admin
uv run uvicorn app.main:app --reload --port 8000
```

In another terminal:

```powershell
cd 'Dine IQ - Front End'
npm install
npm run dev
```

Open `http://127.0.0.1:5173` and sign in with an account from the configured database. The frontend calls `/api` through the Vite proxy.

## Validate the application

```powershell
uv run python -m unittest discover -s tests -v
cd 'Dine IQ - Front End'
npm run build
```

The integration tests use an isolated in-memory account database. They do not modify configured PostgreSQL users.

## Important folders

- `data_generator/`: interconnected restaurant dataset generation
- `preprocessing/`: quality assessment, cleaning, and integration
- `spark/`: Spark ingestion, transformations, and SQL work
- `feature_engineering/`: analytical feature creation
- `analytics/`: restaurant-intelligence analyses
- `ml/`: Spark, Python, forecasting, wastage-risk, and comparison pipelines
- `data/`: raw, processed, integrated, analytical, model, and report outputs
- `app/`: authenticated API, exports, RBAC, audit events, and saved-result adapters
- `Dine IQ - Front End/`: React dashboard
- `tests/`: API integration and permission checks

## Current limits

- Saved forecasts support 1 to 30 days.
- Independent Spark/Python comparison is implemented for menu classification.
- Analytics are read from saved pipeline outputs. Adding a managed restaurant location requires a pipeline rerun before it has analytical results.
- The Jobs page reports saved output availability because the batch scripts do not persist live Spark progress.