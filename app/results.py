"""Read saved pipeline results. The API never trains Spark models on a request."""

from functools import lru_cache
from pathlib import Path
import json
import os

import pandas as pd
import pyarrow.parquet as pq
from fastapi import HTTPException


DATA = Path(
    os.getenv(
        "DINEIQ_DATA_DIR",
        Path(__file__).resolve().parents[1] / "data",
    )
)


@lru_cache(maxsize=48)
def _read(path, signature, columns):
    table = pq.read_table(
        path,
        columns=list(columns) if columns is not None else None,
    )

    # Spark decimal columns must become JSON numbers, not strings.
    for field in table.schema:
        if str(field.type).startswith("decimal"):
            table = table.set_column(
                table.schema.get_field_index(field.name),
                field.name,
                table[field.name].cast("float64"),
            )

    return table.to_pandas()


def read(name, columns=None):
    path = DATA / name

    files = sorted(path.glob("*.parquet")) if path.is_dir() else [path]

    if not files or not all(p.exists() for p in files):
        raise HTTPException(
            404,
            f"Saved result '{name}' is missing. Run its processing script first.",
        )

    try:
        signature = tuple(
            (p.name, p.stat().st_mtime_ns, p.stat().st_size)
            for p in files
        )

        return _read(
            str(path),
            signature,
            tuple(columns) if columns else None,
        ).copy()

    except (OSError, ValueError) as exc:
        raise HTTPException(
            503,
            f"Cannot read saved result '{name}'. Check the pipeline output.",
        ) from exc


def records(frame):
    # pandas handles NaN, dates, NumPy values and nested Parquet arrays.
    return json.loads(
        frame.to_json(
            orient="records",
            date_format="iso",
        )
    )


def mapped(frame, fields):
    return records(
        frame[list(fields)].rename(columns=fields)
    )


def analytics(name):
    return read("analytics/" + name)


FILTERS = {
    "dateFrom",
    "dateTo",
    "location",
    "item",
    "category",
    "segment",
    "channel",
    "promotion",
    "cls",
    "priceMin",
    "priceMax",
    "ratingMin",
    "wasteMax",
}


def allow(params, supported):
    unsupported = [
        key
        for key in FILTERS
        if params.get(key) and key not in supported
    ]

    if unsupported:
        raise HTTPException(
            400,
            "This saved analysis does not support these filters: "
            + ", ".join(sorted(unsupported))
            + ". Clear them to view the result.",
        )


def filtered(frame, params, columns):
    """Only advertise filters actually represented in the saved result."""
    allow(params, columns)

    for key, column in columns.items():
        value = params.get(key)

        if value in (None, ""):
            continue

        if key in (
            "priceMin",
            "priceMax",
            "ratingMin",
            "wasteMax",
        ):
            try:
                number = float(value)

                if not pd.notna(number) or abs(number) == float("inf"):
                    raise ValueError()

            except ValueError:
                raise HTTPException(
                    422,
                    f"{key} must be a finite number",
                )

            frame = (
                frame[frame[column] >= number]
                if key in ("priceMin", "ratingMin")
                else frame[frame[column] <= number]
            )

        elif key in ("dateFrom", "dateTo"):
            try:
                date = pd.Timestamp(value)
            except ValueError:
                raise HTTPException(
                    422,
                    f"{key} must be a valid date",
                )

            dates = pd.to_datetime(frame[column]).dt.normalize()

            frame = (
                frame[dates >= date]
                if key == "dateFrom"
                else frame[dates <= date]
            )

        else:
            frame = frame[
                frame[column].astype(str) == str(value)
            ]

    if (
        params.get("dateFrom")
        and params.get("dateTo")
        and params["dateFrom"] > params["dateTo"]
    ):
        raise HTTPException(
            422,
            "Start date must be before end date",
        )

    return frame


def ratio(a, b, scale=1):
    return float(a) / float(b) * scale if b else 0.0


def strings(value):
    if isinstance(value, str):
        return [
            s.strip()
            for s in value.split(";")
            if s.strip()
        ]

    return list(value) if value is not None else []