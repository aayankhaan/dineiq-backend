"""Small adapters from pipeline column names to the React API contract."""

import pandas as pd
from fastapi import HTTPException
from app.results import (
    analytics as a,
    read,
    mapped,
    records,
    filtered,
    allow,
    ratio,
    strings,
)

ITEM_FILTERS = {"item": "item_id", "category": "category_name"}
LOCATION_FILTERS = {**ITEM_FILTERS, "location": "restaurant_id"}


def metadata():
    categories = read("processed/menu_categories")
    return {
        "locations": mapped(
            read("processed/restaurants"), {"restaurant_id": "id", "name": "name"}
        ),
        "items": mapped(
            read("processed/menu_items"), {"menu_items_id": "id", "name": "name"}
        ),
        "categories": categories.name.tolist(),
        "segments": a(
            "customer_segmentation/segment_summary"
        ).customer_segment.tolist(),
        "channels": a(
            "ordering_channel_analysis/channel_summary"
        ).ordering_channel.tolist(),
        "promotions": mapped(
            read("processed/promotions"), {"promotion_id": "id", "name": "name"}
        ),
        "classes": sorted(a("menu_classification").performance_class.unique().tolist()),
    }


def menu(params):
    if params.get("location"):
        df = a("location_menu_performance/location_menu_performance")
        df = df.rename(
            columns={
                "repeat_purchase_rate_pct": "repeat_purchase_rate",
                "promotion_dependency_pct": "promotion_dependency",
                "sales_trend_pct": "sales_trend",
            }
        )
        df["cost"] = df.item_revenue - df.contribution_margin
        df["tags"] = [[] for _ in range(len(df))]
    else:
        df = a("tricky_menu_cases")
        master = read("processed/menu_items").merge(
            read("processed/menu_categories"), left_on="cat_id", right_on="category_id"
        )
        df = df.merge(
            master[["menu_items_id", "name_y"]].rename(
                columns={"menu_items_id": "item_id", "name_y": "category_name"}
            ),
            on="item_id",
        )
        flags = [
            "high_selling_loss_making",
            "highly_profitable_rarely_purchased",
            "popular_excessive_wastage",
            "highly_rated_poor_profitability",
            "low_rated_high_sales",
            "promotion_dependent",
            "different_across_locations",
            "weekend_performer",
            "seasonal_item",
            "insufficient_history",
        ]
        df["tags"] = df.apply(
            lambda row: [f.replace("_", " ").capitalize() for f in flags if row[f]],
            axis=1,
        )
    df["price"] = df.item_revenue.div(df.quantity_sold.replace(0, float("nan")))
    df["unitCost"] = df.cost.div(df.quantity_sold.replace(0, float("nan")))
    columns = {
        **ITEM_FILTERS,
        "cls": "performance_class",
        "priceMin": "price",
        "priceMax": "price",
        "ratingMin": "average_rating",
        "wasteMax": "wastage_percentage",
    }
    if "restaurant_id" in df:
        columns["location"] = "restaurant_id"
    df = filtered(df, params, columns)
    df["repeat_purchase_rate"] /= 100
    df["promotion_dependency"] /= 100
    return mapped(
        df,
        {
            "item_id": "id",
            "item_name": "name",
            "category_name": "category",
            "quantity_sold": "qty",
            "item_revenue": "revenue",
            "cost": "cost",
            "contribution_margin": "profit",
            "profit_percentage": "marginPct",
            "average_rating": "rating",
            "repeat_purchase_rate": "repeatRate",
            "wastage_percentage": "wastePct",
            "promotion_dependency": "promoDep",
            "sales_trend": "trend",
            "performance_class": "cls",
            "price": "price",
            "unitCost": "unitCost",
            "tags": "tags",
        },
    )


def slow(params):
    df = filtered(a("slow_moving_dishes/slow_moving_dishes"), params, LOCATION_FILTERS)
    df = df[df.slow_moving_dish].copy()
    df["repeat_purchase_rate_pct"] /= 100
    df["slowScore"] = df.slow_signal_count / 7
    df["wastePct"] = (
        100
        * (df.estimated_preparation_quantity - df.wastage_analysis_demand).clip(lower=0)
        / df.estimated_preparation_quantity.replace(0, float("nan"))
    )
    df["id"] = df.restaurant_id.astype(str) + ":" + df.item_id.astype(str)
    return mapped(
        df,
        {
            "id": "id",
            "item_name": "name",
            "restaurant_name": "location",
            "quantity_sold": "qty",
            "purchase_frequency": "orderFreq",
            "days_since_last_purchase": "daysSinceLast",
            "repeat_purchase_rate_pct": "repeatRate",
            "wastePct": "wastePct",
            "profit_percentage": "marginPct",
            "sales_trend_pct": "trend",
            "slowScore": "slowScore",
        },
    )


def item_locations(item_id, params):
    df = filtered(
        a("location_menu_performance/location_menu_performance"),
        {**params, "item": item_id},
        LOCATION_FILTERS,
    )
    return mapped(
        df,
        {
            "restaurant_name": "location",
            "quantity_sold": "qty",
            "profit_percentage": "marginPct",
            "wastage_percentage": "wastePct",
            "performance_class": "cls",
        },
    )


def customers(kind, params):
    columns = {"segment": "customer_segment"}
    if kind == "segments":
        df = a("customer_segmentation/segment_summary").merge(
            a("customer_segmentation/segment_definitions")[
                ["customer_segment", "recommended_strategy"]
            ],
            on="customer_segment",
        )
        return mapped(
            filtered(df, params, columns),
            {
                "customer_segment": "name",
                "customer_count": "count",
                "average_order_value": "avgOrder",
                "average_recency": "recencyDays",
                "average_frequency": "frequency",
                "average_monetary_value": "monetary",
                "recommended_strategy": "strategy",
            },
        )
    if kind == "rfm":
        df = filtered(a("rfm_analysis/customer_rfm"), params, columns)
        bands = [
            {
                "score": i,
                "recency": int((df.recency_score == i).sum()),
                "frequency": int((df.frequency_score == i).sum()),
                "monetary": int((df.monetary_score == i).sum()),
            }
            for i in range(1, 6)
        ]
        top = df.nlargest(20, "monetary_value").merge(
            a("customer_segmentation/customer_segments")[
                ["customer_id", "favorite_category", "channel_preference"]
            ],
            on="customer_id",
        )
        return {
            "bands": bands,
            "top": mapped(
                top,
                {
                    "customer_id": "id",
                    "customer_segment": "segment",
                    "recency": "recency",
                    "frequency": "frequency",
                    "monetary_value": "monetary",
                    "favorite_category": "favCategory",
                    "channel_preference": "channel",
                },
            ),
        }
    df = filtered(
        a("churn_risk/customer_churn_risk"),
        params,
        {**columns, "channel": "channel_preference"},
    )
    df = df[df.churn_risk_flag].copy()
    df["risk"] = df.risk_signal_count / 6
    df["reasons"] = df.risk_reason.apply(strings)
    return mapped(
        df.sort_values("risk", ascending=False),
        {
            "customer_id": "id",
            "risk": "risk",
            "days_since_last_order": "recency",
            "frequency_change_pct": "frequencyChange",
            "monetary_change_pct": "spendChange",
            "category_diversity_change_pct": "categoryDiversityChange",
            "reasons": "reasons",
        },
    )


def basket(params):
    allow(params, {"item"})
    df = a("market_basket/association_rules")
    if params.get("item"):
        df = df[
            (df.antecedent_item_id.astype(str) == str(params["item"]))
            | (df.consequent_item_id.astype(str) == str(params["item"]))
        ]
    try:
        support, lift = (
            float(params.get("minSupport", 0)),
            float(params.get("minLift", 0)),
        )
        if not 0 <= support <= 1 or not 0 <= lift < float("inf"):
            raise ValueError()
    except ValueError:
        raise HTTPException(
            422, "Support must be between 0 and 1, and lift must be nonnegative"
        )
    df = df[(df.support >= support) & (df.lift >= lift)].sort_values(
        "lift", ascending=False
    )
    return mapped(
        df,
        {
            "antecedent_item_name": "antecedent",
            "consequent_item_name": "consequent",
            "antecedent_item_id": "aId",
            "consequent_item_id": "bId",
            "support": "support",
            "confidence": "confidence",
            "lift": "lift",
        },
    )


def orders(params):
    return filtered(
        read("processed/orders"),
        params,
        {
            "dateFrom": "order_datetime",
            "dateTo": "order_datetime",
            "location": "restaurant_id",
            "channel": "ordering_channel",
            "promotion": "promotion_id",
        },
    )


def peak(params):
    df = orders(params)
    df["date"] = pd.to_datetime(df.order_datetime)
    df["hour"] = df.date.dt.hour
    df["day"] = df.date.dt.dayofweek
    df["month"] = df.date.dt.strftime("%Y-%m")
    heat = (
        pd.crosstab(df.day, df.hour)
        .reindex(index=range(7), columns=range(24), fill_value=0)
        .values.tolist()
    )
    counts = df.groupby("ordering_channel").size()
    hour_channel = pd.crosstab(df.hour, df.ordering_channel).reindex(
        range(24), fill_value=0
    )
    dine = [c for c in hour_channel if "dine" in c.lower()]
    delivery = [c for c in hour_channel if "delivery" in c.lower()]
    by_location = []
    names = read("processed/restaurants").set_index("restaurant_id")["name"].to_dict()
    for loc, group in df.groupby("restaurant_id"):
        by_location.append(
            {
                "location": names.get(loc, str(loc)),
                "peakHour": f"{int(group.hour.mode().iloc[0]):02}:00",
                "peakDay": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][
                    int(group.day.mode().iloc[0])
                ],
            }
        )
    season_names = {
        12: "Winter",
        1: "Winter",
        2: "Winter",
        3: "Spring",
        4: "Spring",
        5: "Spring",
        6: "Summer",
        7: "Summer",
        8: "Summer",
        9: "Autumn",
        10: "Autumn",
        11: "Autumn",
    }
    df["season"] = df.date.dt.month.map(season_names)
    seasonal = df.groupby("season").size()
    return {
        "heat": heat,
        "byHour": [
            {"hour": h, "orders": sum(row[h] for row in heat)} for h in range(24)
        ],
        "byDay": [
            {"day": d, "orders": sum(heat[i])}
            for i, d in enumerate(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])
        ],
        "monthly": [
            {"month": k, "orders": int(v)}
            for k, v in df.groupby("month").size().items()
        ],
        "seasonal": [
            {"season": k, "index": ratio(v, seasonal.mean())}
            for k, v in seasonal.items()
        ],
        "byLocation": by_location,
        "dineVsDelivery": [
            {
                "hour": h,
                "dineIn": int(hour_channel.loc[h, dine].sum()),
                "delivery": int(hour_channel.loc[h, delivery].sum()),
            }
            for h in range(24)
        ],
        "weekendRatio": ratio((df.day >= 5).sum(), len(df)),
    }


def locations(params):
    df = a("multi_location_intelligence/location_comparison")
    ranks = [c for c in df if c.endswith("_rank")]
    df["health"] = 100 * (1 - (df[ranks].mean(axis=1) - 1) / max(len(df) - 1, 1))
    df = filtered(df, params, {"location": "restaurant_id"})
    df["repeat_purchase_rate_pct"] /= 100
    return mapped(
        df,
        {
            "restaurant_id": "id",
            "restaurant_name": "name",
            "revenue": "revenue",
            "contribution_margin": "profit",
            "average_order_value": "aov",
            "customer_count": "customers",
            "repeat_purchase_rate_pct": "repeatRate",
            "wastage_cost_pct_of_item_revenue": "wastePct",
            "average_rating": "rating",
            "promotion_effective_rate_pct": "promoEffectiveness",
            "health": "health",
        },
    )


def channels(params):
    df = filtered(
        a("ordering_channel_analysis/channel_summary"),
        params,
        {"channel": "ordering_channel"},
    )
    return mapped(
        df,
        {
            "ordering_channel": "name",
            "order_count": "orders",
            "average_basket_size": "basket",
            "average_order_value": "aov",
            "average_discount_pct": "discountPct",
            "promotion_order_rate_pct": "promoShare",
            "profit_percentage": "marginPct",
            "peak_order_hour": "peak",
            "top_category_name": "topCategory",
        },
    )


def pricing(params, history=False):
    if history:
        df = filtered(
            a("price_intelligence/price_change_events"),
            params,
            {
                **LOCATION_FILTERS,
                "dateFrom": "effective_date",
                "dateTo": "effective_date",
            },
        )
        return mapped(
            df.sort_values("effective_date"),
            {
                "effective_date": "month",
                "new_price": "price",
                "post_daily_demand": "demand",
                "restaurant_name": "location",
            },
        )
    df = a("price_sensitivity/item_price_sensitivity")
    events = a("price_intelligence/price_change_events")
    events = (
        events[events.evaluable_change]
        .groupby("item_id")[
            [
                "price_change_pct",
                "demand_change_pct",
                "price_elasticity_proxy",
                "new_price",
            ]
        ]
        .mean()
        .reset_index()
    )
    df = df.merge(events, on="item_id", how="left")
    df = filtered(df, params, ITEM_FILTERS)
    return mapped(
        df,
        {
            "item_id": "id",
            "item_name": "name",
            "category_name": "category",
            "new_price": "price",
            "price_change_pct": "priceChangePct",
            "demand_change_pct": "demandChangePct",
            "price_elasticity_proxy": "elasticity",
            "price_sensitivity_class": "sensitivity",
        },
    )


def promotions(params):
    df = filtered(
        a("promotion_traps/promotion_traps"),
        params,
        {**LOCATION_FILTERS, "promotion": "promotion_id"},
    )
    df["period"] = df.start_date.astype(str) + " to " + df.end_date.astype(str)
    df["flags"] = df.trap_types.apply(strings)
    df["verdict"] = df.apply(
        lambda r: (
            "Promotion trap" if r.promotion_trap_detected else r.promotion_assessment
        ),
        axis=1,
    )
    df["repeat_purchase_rate_pct"] /= 100
    df["postPromoDrop"] = (
        1 - df.post_daily_demand.div(df.during_daily_demand.replace(0, float("nan")))
    ) * 100
    return mapped(
        df,
        {
            "promotion_id": "id",
            "promotion_name": "name",
            "period": "period",
            "demand_lift_pct": "orderUplift",
            "revenue_lift_pct": "revenueChange",
            "average_margin_per_order_change_pct": "marginChange",
            "margin_lift_pct": "profitChange",
            "acquired_customers": "newCustomers",
            "repeat_purchase_rate_pct": "repeatRate",
            "aov_change_pct": "aovChange",
            "wastage_change_pct": "wasteChange",
            "postPromoDrop": "postPromoDrop",
            "flags": "flags",
            "verdict": "verdict",
        },
    )


def ratings(params):
    allow(params, set())
    df = a("rating_satisfaction/item_rating_performance").sort_values(
        "average_rating", ascending=False
    )
    df["repeat_purchase_rate_pct"] /= 100
    trend = a("rating_satisfaction/rating_time_promotion")
    rows = []
    for month, group in trend.groupby("rating_month"):
        total = group.rating_count.sum()
        promoted = group[
            group.promotion_status.str.lower().str.contains("promot")
            & ~group.promotion_status.str.lower().str.contains("non|no ")
        ]
        rows.append(
            {
                "week": str(month),
                "rating": ratio(
                    (group.average_rating * group.rating_count).sum(), total
                ),
                "promo": ratio(
                    (promoted.average_rating * promoted.rating_count).sum(),
                    promoted.rating_count.sum(),
                )
                if len(promoted)
                else None,
            }
        )
    return {
        "byItem": mapped(
            df,
            {
                "item_id": "id",
                "item_name": "name",
                "average_rating": "rating",
                "profit_percentage": "marginPct",
                "quantity_sold": "qty",
                "repeat_purchase_rate_pct": "repeatRate",
            },
        ),
        "byLocation": mapped(
            a("rating_satisfaction/location_rating_performance"),
            {"restaurant_name": "name", "average_rating": "rating"},
        ),
        "trend": rows,
    }


def anomalies(params):
    sales = filtered(
        a("sales_anomalies/sales_anomaly_events"),
        params,
        {
            "location": "restaurant_id",
            "item": "item_id",
            "dateFrom": "event_date",
            "dateTo": "event_date",
        },
    )
    rating = filtered(
        a("rating_anomalies/rating_anomaly_windows"),
        params,
        {
            "location": "restaurant_id",
            "item": "item_id",
            "dateFrom": "rating_week",
            "dateTo": "rating_week",
        },
    )
    rating = rating[rating.rating_anomaly_detected].copy()
    rating["detail"] = rating.apply(
        lambda r: (
            f"{r.rating_count} ratings; average {r.average_rating:.2f}; historical average {r.historical_average_rating:.2f}"
        ),
        axis=1,
    )
    rating["anomaly_types"] = rating.anomaly_types.apply(
        lambda x: "; ".join(strings(x))
    )
    result = {
        "sales": mapped(
            sales,
            {
                "event_date": "date",
                "restaurant_name": "entity",
                "anomaly_type": "type",
                "evidence": "detail",
                "severity": "severity",
            },
        ),
        "ratings": mapped(
            rating,
            {
                "rating_week": "date",
                "item_name": "entity",
                "anomaly_types": "type",
                "detail": "detail",
                "anomaly_severity": "severity",
            },
        ),
    }
    for kind, rows in result.items():
        rows.sort(key=lambda row: row["date"] or "", reverse=True)
        for i, row in enumerate(rows):
            row["id"] = f"{kind}-{i}"
    return result


def recommendations(params):
    df = filtered(
        a("recommendation_priority/prioritized_recommendations"),
        params,
        {
            "location": "restaurant_id",
            "item": "item_id",
            "segment": "customer_segment",
            "promotion": "promotion_id",
        },
    )
    if params.get("priority"):
        df = df[df.priority == params["priority"]]
    evidence = a("recommendation_evidence/recommendation_evidence").sort_values(
        "evidence_order"
    )
    groups = (
        evidence.groupby("recommendation_id").evidence_statement.apply(list).to_dict()
    )
    df["evidence"] = df.recommendation_id.map(groups).apply(
        lambda v: v if isinstance(v, list) else []
    )
    return mapped(
        df.sort_values("priority_score", ascending=False),
        {
            "recommendation_id": "id",
            "priority": "priority",
            "recommendation_type": "type",
            "recommended_action": "action",
            "impact_value": "impact",
            "impact_unit": "impactUnit",
            "impact_basis": "impactBasis",
            "evidence": "evidence",
        },
    )


def wastage(params):
    df = filtered(
        a("wastage/item_daily_wastage"),
        params,
        {**LOCATION_FILTERS, "dateFrom": "analysis_date", "dateTo": "analysis_date"},
    )

    def grouped(keys):
        return df.groupby(keys, as_index=False)[
            [
                "estimated_wastage_cost",
                "estimated_wasted_servings",
                "estimated_preparation_quantity",
            ]
        ].sum()

    items = grouped(["item_id", "item_name", "category_name"])
    items["wastePct"] = (
        100
        * items.estimated_wasted_servings
        / items.estimated_preparation_quantity.replace(0, float("nan"))
    )
    locs = grouped(["restaurant_id", "restaurant_name"])
    locs["pct"] = (
        100
        * locs.estimated_wasted_servings
        / locs.estimated_preparation_quantity.replace(0, float("nan"))
    )
    allocation = filtered(
        a("wastage/wastage_item_allocation"),
        params,
        {**LOCATION_FILTERS, "dateFrom": "wastage_date", "dateTo": "wastage_date"},
    )
    reasons = allocation.groupby("reason", as_index=False).allocated_wastage_cost.sum()
    total_reason_cost = reasons.allocated_wastage_cost.sum()
    reasons["allocated_wastage_cost"] = (
        reasons.allocated_wastage_cost / total_reason_cost * 100
        if total_reason_cost
        else 0
    )
    reasons = reasons.sort_values("allocated_wastage_cost", ascending=False)
    risk_params = {k: v for k, v in params.items() if k not in ("dateFrom", "dateTo")}
    risk = filtered(
        read("ml/wastage_risk/high_risk_predictions"), risk_params, LOCATION_FILTERS
    )
    if params.get("dateFrom") or params.get("dateTo"):
        risk = filtered(
            risk,
            {k: v for k, v in params.items() if k in ("dateFrom", "dateTo")},
            {"dateFrom": "target_date", "dateTo": "target_date"},
        )
    risk = risk.sort_values("risk_probability", ascending=False).head(100)
    return {
        "kpis": {
            "totalUnits": float(df.estimated_wasted_servings.sum()),
            "cost": float(df.estimated_wastage_cost.sum()),
            "pct": ratio(
                df.estimated_wasted_servings.sum(),
                df.estimated_preparation_quantity.sum(),
                100,
            ),
        },
        "byItem": mapped(
            items.sort_values("estimated_wastage_cost", ascending=False),
            {
                "item_id": "id",
                "item_name": "name",
                "category_name": "category",
                "wastePct": "wastePct",
                "estimated_wasted_servings": "units",
                "estimated_wastage_cost": "cost",
            },
        ),
        "byLocation": mapped(
            locs,
            {"restaurant_name": "name", "pct": "pct", "estimated_wastage_cost": "cost"},
        ),
        "byReason": mapped(
            reasons, {"reason": "name", "allocated_wastage_cost": "value"}
        ),
        "trend": mapped(
            grouped(["analysis_date"]),
            {"analysis_date": "date", "estimated_wastage_cost": "cost"},
        ),
        "risk": mapped(
            risk,
            {
                "item_id": "itemId",
                "item_name": "item",
                "restaurant_name": "location",
                "target_date": "day",
                "risk_probability": "risk",
                "lag_1_preparation_quantity": "prepQty",
                "forecast_demand_proxy": "forecastDemand",
                "historical_average_wastage_cost": "expectedCost",
            },
        ),
    }


def executive(params):
    df = read(
        "integrated/transactions",
        columns=[
            "restaurant_id",
            "item_id",
            "category_name",
            "order_datetime",
            "ordering_channel",
            "promotion_id",
            "line_total",
            "quantity",
            "item_cost",
            "order_id",
            "customer_id",
        ],
    )
    columns = {
        **LOCATION_FILTERS,
        "dateFrom": "order_datetime",
        "dateTo": "order_datetime",
        "channel": "ordering_channel",
        "promotion": "promotion_id",
    }
    df = filtered(df, params, columns)
    df["profit"] = df.line_total - df.quantity * df.item_cost
    df["date"] = pd.to_datetime(df.order_datetime).dt.strftime("%Y-%m-%d")
    daily = df.groupby("date", as_index=False).agg(
        revenue=("line_total", "sum"),
        profit=("profit", "sum"),
        orders=("order_id", "nunique"),
    )
    distinct = df.drop_duplicates("order_id")
    customer_orders = distinct.groupby("customer_id").size()
    dates = pd.to_datetime(distinct.order_datetime)
    heat = (
        pd.crosstab(dates.dt.dayofweek, dates.dt.hour)
        .reindex(index=range(7), columns=range(24), fill_value=0)
        .values.tolist()
    )
    mix = distinct.groupby("ordering_channel").size()
    waste = None
    forecast_demand = None
    if not params.get("channel") and not params.get("promotion"):
        waste_df = filtered(
            a("wastage/item_daily_wastage"),
            params,
            {
                **LOCATION_FILTERS,
                "dateFrom": "analysis_date",
                "dateTo": "analysis_date",
            },
        )
        waste = float(waste_df.estimated_wastage_cost.sum())
        forecast_params = {
            k: v
            for k, v in params.items()
            if k in ("item", "category", "location") and v
        }
        if len(forecast_params) <= 1:
            level = (
                "item"
                if params.get("item")
                else "location"
                if params.get("location")
                else "category"
            )
            future = forecast({**forecast_params, "level": level, "horizon": "14"})
            forecast_demand = sum(
                row["predicted"] for row in future["series"] if row["phase"] == "future"
            )
    revenue = float(df.line_total.sum())
    return {
        "kpis": {
            "revenue": revenue,
            "profit": float(df.profit.sum()),
            "orders": len(distinct),
            "aov": ratio(revenue, len(distinct)),
            "activeCustomers": len(customer_orders),
            "repeatCustomers": int((customer_orders > 1).sum()),
            "wastageCost": waste,
            "forecastDemand": forecast_demand,
        },
        "deltas": {},
        "trend": records(daily),
        "channelMix": [{"name": k, "value": int(v)} for k, v in mix.items()],
        "heat": heat,
    }


def forecast(params):
    levels = {
        "item": ("menu_item", "Menu Item", "item", "item_id"),
        "category": ("menu_category", "Menu Category", "category", "category_name"),
        "location": (
            "restaurant_location",
            "Restaurant Location",
            "location",
            "restaurant_id",
        ),
    }
    level = params.get("level", "category")
    if level not in levels:
        raise HTTPException(422, "Forecast level must be item, category or location")
    prefix, label, filter_key, column = levels[level]
    allow(params, {filter_key})
    meta = read("ml/forecasting/forecast_metadata").iloc[0]
    try:
        horizon = int(params.get("horizon", 14))
        if not 1 <= horizon <= int(meta.forecast_days):
            raise ValueError()
    except ValueError:
        raise HTTPException(
            422,
            f"Saved forecast supports 1 to {int(meta.forecast_days)} days. Regenerate forecasts for a longer horizon.",
        )
    select = {filter_key: column}
    hist = filtered(read(f"ml/forecasting/historical_{prefix}_demand"), params, select)
    future = filtered(read(f"ml/forecasting/{prefix}_forecast"), params, select)
    future = future[future.forecast_horizon_day <= horizon]
    back = filtered(
        read(f"ml/forecasting_validation/{prefix}_backtest"), params, select
    )
    split = records(read("ml/forecasting_validation/split_metadata"))[0]
    test = back[back.split == "test"]
    actual, predicted, baseline = (
        test.actual_demand,
        test.predicted_demand,
        test.baseline_prediction,
    )

    def errors(pred):
        if not len(actual):
            return {"MAE": None, "RMSE": None, "MAPE": None, "R2": None}
        err = actual - pred
        eligible = actual != 0
        denominator = ((actual - actual.mean()) ** 2).sum()
        return {
            "MAE": round(float(err.abs().mean()), 4),
            "RMSE": round(float((err.pow(2).mean()) ** 0.5), 4),
            "MAPE": f"{(err[eligible].abs() / actual[eligible]).mean() * 100:.2f}%"
            if eligible.any()
            else None,
            "R2": float(1 - err.pow(2).sum() / denominator) if denominator else None,
        }

    metrics, base = errors(predicted), errors(baseline)
    metrics.update(
        baselineMAE=base["MAE"],
        baselineMAPE=base["MAPE"],
        baselineName=split["baseline_method"],
    )
    history = (
        hist.groupby("order_date", as_index=False)
        .demand.sum()
        .rename(columns={"order_date": "date", "demand": "actual"})
    )
    tested = (
        back.groupby("forecast_date", as_index=False)
        .agg(predicted=("predicted_demand", "sum"), phase=("split", "first"))
        .rename(columns={"forecast_date": "date"})
    )
    history = history.merge(tested, on="date", how="left")
    history["phase"] = history.phase.fillna("train")
    future = (
        future.groupby("forecast_date", as_index=False)
        .predicted_demand.sum()
        .rename(columns={"forecast_date": "date", "predicted_demand": "predicted"})
    )
    future["phase"] = "future"
    future["actual"] = None
    rows = records(pd.concat([history.tail(90), future], ignore_index=True))
    for row in rows:
        row["date"] = row["date"][:10]
    return {
        "versions": {
            "spark": meta.model_version,
            "python": "Not independently modeled",
        },
        "modelName": meta.model_name,
        "maxHorizon": int(meta.forecast_days),
        "split": {
            "trainEnd": split["training_end_date"][:10],
            "testStart": split["test_start_date"][:10],
            "method": "Chronological training, validation, testing",
        },
        "series": rows,
        "metrics": metrics,
        "risk": [
            {
                "date": r["date"],
                "predicted": r["predicted"],
                "note": "Forecast exceeds the last 28-day average by more than 30%",
            }
            for r in rows
            if r["phase"] == "future"
            and r["predicted"] > history.actual.tail(28).mean() * 1.3
        ],
    }


def comparison(params):
    allow(params, set())
    if params.get("task", "menu_class") != "menu_class":
        raise HTTPException(
            422,
            "Independent Spark/Python comparison is available for menu classification only",
        )
    df = read("ml/comparison/dual_pipeline_menu_comparison.parquet")
    summary = read("ml/comparison/dual_pipeline_comparison_summary.parquet").iloc[0]
    df["match"] = df.spark_prediction == df.python_prediction
    from sklearn.metrics import accuracy_score, precision_recall_fscore_support

    def scores(column):
        precision, recall, f1, _ = precision_recall_fscore_support(
            df.actual_class, df[column], average="macro", zero_division=0
        )
        return {
            "Accuracy": float(accuracy_score(df.actual_class, df[column])),
            "Macro precision": float(precision),
            "Macro recall": float(recall),
            "Macro F1": float(f1),
        }

    candidates = read("ml/menu_classification_model_metrics")
    return {
        "versions": {
            "spark": summary.spark_model_version,
            "python": summary.python_model_version,
        },
        "total": len(df),
        "agreement": round(float(df.match.mean() * 100), 2),
        "disagreements": int((~df.match).sum()),
        "metrics": {
            "spark": scores("spark_prediction"),
            "python": scores("python_prediction"),
        },
        "sparkCandidates": mapped(candidates, {"model_name": "name", "f1": "score"}),
        "rows": mapped(
            df,
            {
                "item_id": "recordId",
                "actual_class": "actual",
                "spark_prediction": "spark",
                "python_prediction": "python",
                "spark_probability": "sparkConf",
                "python_probability": "pythonConf",
                "match": "match",
                "probability_difference": "diff",
                "disagreement_explanation": "explanation",
            },
        ),
    }
