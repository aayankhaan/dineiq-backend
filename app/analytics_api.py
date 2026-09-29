"""HTTP routes for the existing frontend. All business results come from local pipelines."""

from io import BytesIO
from typing import Literal
from datetime import datetime, timezone

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field
from sqlmodel import Session, select

from app import analytics_service as service
from app.database import get_session
from app.models import User, UserRole, AuditEvent, ManagedLocation
from app.user import UserCreate, create_user
from app.utils import get_current_user, require_admin
from app.results import DATA, read, analytics, mapped, records, ratio

router = APIRouter(dependencies=[Depends(get_current_user)])


def log(db, user, area, action):
    db.add(AuditEvent(user=user.email, area=area, action=action))
    db.commit()


def require_team(user: User = Depends(get_current_user)):
    if user.role == UserRole.restaurant_manager:
        raise HTTPException(
            403, "This action requires an analyst, regional manager or administrator"
        )
    return user


@router.get("/meta/filters")
def meta():
    return service.metadata()


@router.get("/kpis/executive")
def executive(request: Request):
    return service.executive(dict(request.query_params))


@router.get("/menu/items")
def menu(request: Request):
    return service.menu(dict(request.query_params))


@router.get("/menu/slow-moving")
def slow(request: Request):
    return service.slow(dict(request.query_params))


@router.get("/menu/items/{item_id}/locations")
def item_locations(item_id: int, request: Request):
    return service.item_locations(item_id, dict(request.query_params))


@router.get("/customers/{kind}")
def customers(kind: Literal["segments", "rfm", "churn"], request: Request):
    return service.customers(kind, dict(request.query_params))


@router.get("/basket/rules")
def basket(request: Request):
    return service.basket(dict(request.query_params))


@router.get("/peak")
def peak(request: Request):
    return service.peak(dict(request.query_params))


@router.get("/forecast")
def forecast(request: Request):
    return service.forecast(dict(request.query_params))


@router.get("/wastage")
def wastage(request: Request):
    return service.wastage(dict(request.query_params))


@router.get("/pricing/{kind}")
def pricing(kind: Literal["sensitivity", "history"], request: Request):
    return service.pricing(dict(request.query_params), history=kind == "history")


@router.get("/promotions")
def promotions(request: Request):
    return service.promotions(dict(request.query_params))


@router.get("/ratings")
def ratings(request: Request):
    return service.ratings(dict(request.query_params))


@router.get("/anomalies")
def anomalies(request: Request):
    return service.anomalies(dict(request.query_params))


@router.get("/locations", dependencies=[Depends(require_team)])
def locations(request: Request):
    return service.locations(dict(request.query_params))


@router.get("/channels", dependencies=[Depends(require_team)])
def channels(request: Request):
    return service.channels(dict(request.query_params))


@router.get("/models/comparison", dependencies=[Depends(require_team)])
def comparison(request: Request):
    return service.comparison(dict(request.query_params))


@router.get("/recommendations")
def recommendations(request: Request):
    return service.recommendations(dict(request.query_params))


class Scenario(BaseModel):
    scenario: Literal[
        "price", "discount", "promo_freq", "remove", "prep", "demand", "waste"
    ]
    itemId: int = Field(gt=0)
    value: float = Field(allow_inf_nan=False)


@router.post("/whatif")
def what_if(
    data: Scenario,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_session),
):
    bounds = {
        "price": (-50, 50),
        "discount": (0, 90),
        "promo_freq": (0, 100),
        "remove": (0, 0),
        "prep": (-100, 100),
        "demand": (-100, 100),
        "waste": (0, 95),
    }
    lower, upper = bounds[data.scenario]
    if not lower <= data.value <= upper:
        raise HTTPException(422, f"{data.scenario} must be between {lower} and {upper}")
    rows = service.menu({"item": str(data.itemId)})
    if not rows:
        raise HTTPException(404, "Menu item not found")
    item = rows[0]
    history = read("ml/forecasting/historical_menu_item_demand")
    history = history[history.item_id == data.itemId]
    months = max(pd.to_datetime(history.order_date).dt.to_period("M").nunique(), 1)
    demand0 = item["qty"] / months
    price0, unit_cost = item["price"], item["unitCost"]
    # Use the same observed elasticity source as the batch scenario pipeline.
    sensitivity = analytics("price_sensitivity/item_price_sensitivity")
    sensitivity = sensitivity[sensitivity.item_id == data.itemId]
    elasticity = (
        float(sensitivity.iloc[0].median_capped_absolute_elasticity)
        if len(sensitivity)
        else float("nan")
    )
    if data.scenario in ("price", "discount") and not pd.notna(elasticity):
        raise HTTPException(
            422, "This item has insufficient price-change evidence for this scenario"
        )
    waste_rate = min(item["wastePct"], 95) / 100
    waste_units0 = demand0 * waste_rate / max(1 - waste_rate, 0.05)
    prep0 = demand0 + waste_units0
    demand, price, waste_units = demand0, price0, waste_units0
    v = data.value / 100
    notes = [
        f"Baseline is the monthly average across {months} months of saved history.",
        "Contribution margin excludes wastage; profit subtracts estimated wastage cost.",
    ]
    if data.scenario in ("price", "discount"):
        change = v if data.scenario == "price" else -v
        price *= 1 + change
        demand *= min(2, max(0, 1 - elasticity * change))
        waste_units = demand * waste_rate / max(1 - waste_rate, 0.05)
        notes.append(
            f"Uses historical absolute elasticity {elasticity:.3f}; demand response is capped between 0 and 2 times baseline. Discount applies to the observed average selling price."
        )
    elif data.scenario == "promo_freq":
        share = item["promoDep"]
        if not share:
            raise HTTPException(
                422, "This item has no observed promotion demand to extend"
            )
        demand *= 1 + share * v
        waste_units *= 1 + share * v
        notes.append(
            "Scales the observed promoted share of sales by the requested increase in promotion-active days, at unchanged average price and margin. No causal uplift is assumed."
        )
    elif data.scenario == "remove":
        demand = waste_units = 0
        notes.append("Assumes no sales transfer to other dishes.")
    elif data.scenario == "prep":
        prep = prep0 * (1 + v)
        demand = min(demand0, prep)
        waste_units = max(0, prep - demand)
        notes.append(
            "Demand is capped by preparation; remaining prepared units are treated as waste."
        )
    elif data.scenario == "demand":
        demand *= 1 + v
        waste_units *= 1 + v
        notes.append(
            "Demand and preparation scale together at unchanged price and wastage rate."
        )
    elif data.scenario == "waste":
        waste_units = demand * v / (1 - v)
        notes.append(
            "Applies the selected wastage share of preparation at unchanged demand."
        )

    def calculate(qty, price, waste):
        revenue = qty * price
        contribution = revenue - qty * unit_cost
        waste_cost = waste * unit_cost
        return {
            "revenue": revenue,
            "contribution": contribution,
            "demand": qty,
            "wastage": waste_cost,
            "profit": contribution - waste_cost,
        }

    log(
        db,
        user,
        "Scenario",
        f"Estimated {data.scenario} for item {data.itemId}; value={data.value}",
    )
    return {
        "estimate": True,
        "item": item["name"],
        "period": "monthly historical average",
        "baseline": calculate(demand0, price0, waste_units0),
        "simulated": calculate(demand, price, waste_units),
        "notes": notes,
    }


def model_comparison_report(params):
    result = service.comparison(params)
    return [
        {
            **row,
            "sparkModel": result["models"]["spark"],
            "pythonModel": result["models"]["python"],
            "sparkVersion": result["versions"]["spark"],
            "pythonVersion": result["versions"]["python"],
        }
        for row in result["rows"]
    ]


REPORTS = {
    "menu-performance": service.menu,
    "profitability": service.menu,
    "customer-segmentation": lambda p: service.customers("segments", p),
    "market-basket": service.basket,
    "demand-forecast": lambda p: service.forecast(p)["series"],
    "wastage": lambda p: service.wastage(p)["byItem"],
    "promotions": service.promotions,
    "pricing": service.pricing,
    "location-performance": service.locations,
    "anomalies": lambda p: sum(service.anomalies(p).values(), []),
    "recommendations": service.recommendations,
    "model-comparison": model_comparison_report,
    "peak-period": lambda p: [
        *[
            {"view": "hour", "period": f"{row['hour']}:00", "orders": row["orders"]}
            for row in service.peak(p)["byHour"]
        ],
        *[
            {"view": "weekday", "period": row["day"], "orders": row["orders"]}
            for row in service.peak(p)["byDay"]
        ],
        *[
            {
                "view": "location",
                "period": row["location"],
                "peak_hour": row["peakHour"],
                "peak_day": row["peakDay"],
            }
            for row in service.peak(p)["byLocation"]
        ],
    ],
}


@router.get("/reports/{kind}/download")
def report(
    kind: str,
    request: Request,
    format: Literal["csv", "xlsx"] = "csv",
    user: User = Depends(require_team),
    db: Session = Depends(get_session),
):
    if kind not in REPORTS:
        raise HTTPException(404, "Unknown report")
    frame = pd.DataFrame(REPORTS[kind](dict(request.query_params)))
    for column in frame:
        frame[column] = frame[column].map(
            lambda v: "; ".join(map(str, v)) if isinstance(v, list) else v
        )
        # Prevent spreadsheet software from evaluating exported text as a formula.
        frame[column] = frame[column].map(
            lambda v: (
                "'" + v
                if isinstance(v, str) and v.lstrip().startswith(("=", "+", "-", "@"))
                else v
            )
        )
    if format == "csv":
        content = frame.to_csv(index=False).encode("utf-8-sig")
        media = "text/csv"
    else:
        output = BytesIO()
        frame.to_excel(output, index=False, engine="openpyxl")
        content = output.getvalue()
        media = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    log(db, user, "Export", f"Downloaded {kind}.{format}")
    return Response(
        content,
        media_type=media,
        headers={"Content-Disposition": f'attachment; filename="{kind}.{format}"'},
    )


@router.get("/jobs", dependencies=[Depends(require_team)])
def jobs():
    # Existing scripts do not store execution events. Show output evidence only.
    entries = []
    for folder in ("processed", "integrated", "features", "analytics", "ml"):
        markers = list((DATA / folder).rglob("_SUCCESS"))
        if markers:
            modified = max(p.stat().st_mtime for p in markers)
            entries.append(
                {
                    "id": folder,
                    "name": folder,
                    "engine": "Saved pipeline outputs",
                    "status": "Outputs available",
                    "progress": None,
                    "rows": None,
                    "duration": "Not recorded",
                    "started": datetime.fromtimestamp(
                        modified, timezone.utc
                    ).isoformat(),
                }
            )
    path = DATA / "reports/cleaning_report.csv"
    quality_path = DATA / "reports/data_quality_report.csv"
    dq = []
    if path.exists():
        cleaning = pd.read_csv(path).fillna("")
        for i, row in cleaning.iterrows():
            dq.append(
                {
                    "rule": f"C-{i + 1}",
                    "issue": f"{row.dataset}: {row.issue}",
                    "found": int(row.affected_rows),
                    "action": f"{row.action}: {row.reason}",
                }
            )
    if quality_path.exists():
        quality = pd.read_csv(quality_path).fillna("")
        for i, row in quality[quality.issue_count > 0].iterrows():
            dq.append(
                {
                    "rule": f"Q-{i + 1}",
                    "issue": f"{row.dataset}: {row['check']} ({row['column']})",
                    "found": int(row.issue_count),
                    "action": "Assessment finding; see cleaning rows for actions",
                }
            )
    return {
        "jobs": entries,
        "dq": dq,
        "note": "Saved output availability only. Existing scripts do not record live job status, progress or execution duration.",
    }


@router.get("/audit", dependencies=[Depends(require_admin)])
def audit(db: Session = Depends(get_session)):
    return db.exec(select(AuditEvent).order_by(AuditEvent.id.desc()).limit(500)).all()


@router.get("/admin/users", dependencies=[Depends(require_admin)])
def users(db: Session = Depends(get_session)):
    return [
        {
            "id": u.id,
            "username": u.email,
            "name": u.name,
            "role": "manager"
            if u.role == UserRole.restaurant_manager
            else u.role.value,
            "is_active": u.is_active,
            "location": "All",
        }
        for u in db.exec(select(User)).all()
    ]


class AdminUser(BaseModel):
    username: str = Field(min_length=3)
    name: str = Field(min_length=1)
    password: str = Field(min_length=8)
    role: Literal["admin", "analyst", "regional_manager", "manager"]


@router.post("/admin/users")
def add_user(
    data: AdminUser,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_session),
):
    role = "restaurant_manager" if data.role == "manager" else data.role
    user = create_user(
        UserCreate(
            name=data.name, email=data.username, password=data.password, role=role
        ),
        db,
    )
    log(db, admin, "Admin", f"Created user {user.id}")
    return {"id": user.id, "name": user.name, "username": user.email, "role": data.role}


def location_rows(db: Session):
    base = records(read("processed/restaurants"))
    managed = {row.id: row for row in db.exec(select(ManagedLocation)).all()}
    rows = []
    for row in base:
        override = managed.pop(int(row["restaurant_id"]), None)
        rows.append(
            {
                "id": int(row["restaurant_id"]),
                "name": override.name if override else row["name"],
                "city": override.city if override else row["city"],
                "area": override.area if override else row["area"],
                "status": override.status if override else row["status"],
                "opening_date": override.opening_date if override else str(row["opening_date"]),
                "source": "Managed" if override else "Pipeline",
            }
        )
    rows.extend(
        {
            "id": row.id,
            "name": row.name,
            "city": row.city,
            "area": row.area,
            "status": row.status,
            "opening_date": row.opening_date,
            "source": "Managed",
        }
        for row in managed.values()
    )
    return sorted(rows, key=lambda row: int(row["id"]))


class LocationInput(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    city: str = Field(min_length=2, max_length=80)
    area: str = Field(min_length=1, max_length=80)
    status: Literal["Active", "Inactive"] = "Active"
    opening_date: str | None = None


@router.get("/admin/locations", dependencies=[Depends(require_admin)])
def admin_locations(db: Session = Depends(get_session)):
    return location_rows(db)


@router.post("/admin/locations")
def add_location(
    data: LocationInput,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_session),
):
    rows = location_rows(db)
    if any(row["name"].strip().lower() == data.name.strip().lower() for row in rows):
        raise HTTPException(400, "A restaurant location with this name already exists")
    location = ManagedLocation(
        id=max((int(row["id"]) for row in rows), default=0) + 1,
        **data.model_dump(),
    )
    db.add(location)
    db.commit()
    db.refresh(location)
    log(db, admin, "Admin", f"Created restaurant location {location.id}")
    return next(row for row in location_rows(db) if row["id"] == location.id)


@router.patch("/admin/locations/{location_id}")
def update_location(
    location_id: int,
    data: LocationInput,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_session),
):
    rows = location_rows(db)
    current = next((row for row in rows if int(row["id"]) == location_id), None)
    if not current:
        raise HTTPException(404, "Restaurant location not found")
    duplicate = next(
        (
            row for row in rows
            if int(row["id"]) != location_id
            and row["name"].strip().lower() == data.name.strip().lower()
        ),
        None,
    )
    if duplicate:
        raise HTTPException(400, "A restaurant location with this name already exists")
    location = db.get(ManagedLocation, location_id) or ManagedLocation(id=location_id, **data.model_dump())
    location.sqlmodel_update(data.model_dump())
    location.updated_at = datetime.now(timezone.utc)
    db.add(location)
    db.commit()
    log(db, admin, "Admin", f"Updated restaurant location {location_id}")
    return next(row for row in location_rows(db) if int(row["id"]) == location_id)
