# API contract

Base URL = `VITE_API_URL` (default `/api`). The client sends the bearer token and
includes cookies. GET requests also receive active global filters.

Live routes are in `app/analytics_api.py`; response mappings are in
`app/analytics_service.py`. `mock.js` contains demo examples only.

| Method | Path | Result |
|---|---|---|
| POST | `/auth/login` `{email,password}` | `{token,user}` |
| GET | `/meta/filters` | Filter options |
| GET | `/kpis/executive` | KPIs, trend, channel mix and heatmap |
| GET | `/menu/items` | Menu performance rows |
| GET | `/menu/slow-moving` | Slow-moving dishes |
| GET | `/menu/items/:id/locations` | Per-location item performance |
| GET | `/customers/segments`, `/customers/rfm`, `/customers/churn` | Customer analysis |
| GET | `/basket/rules?minSupport&minLift` | Association rules |
| GET | `/peak` | Hour, day, month, season, location and channel peaks |
| GET | `/forecast?level&horizon` | Chronological forecast result |
| GET | `/wastage` | KPIs, breakdowns, trend and risk |
| GET | `/pricing/sensitivity`, `/pricing/history?item=` | Price analysis |
| GET | `/promotions` | Promotion effectiveness and traps |
| GET | `/ratings`, `/anomalies` | Rating and anomaly results |
| GET | `/locations`, `/channels` | Location and channel comparisons |
| GET | `/models/comparison?task=menu_class` | Independent model comparison |
| GET | `/recommendations?priority=` | Evidence-backed recommendations |
| POST | `/whatif` `{scenario,itemId,value}` | Estimated scenario impact |
| GET | `/reports/:kind/download?format=csv|xlsx` | Exported file |
| GET | `/jobs` | Saved output and data-quality evidence |
| GET | `/audit`, `/admin/users`, `/admin/locations` | Administration data |

Errors use `{detail: "message"}` with an appropriate HTTP status.

Live mode is the default. Forecasts support 1-30 saved days. Only menu
classification has an independent Spark/Python comparison. Unsupported filter
dimensions return a clear error. Recommendation impacts include `impactUnit` and
`impactBasis`; they are not all currency savings. See the root `INTEGRATION.md`.
