from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field


class TimeseriesItem(BaseModel):
    day: date
    n_focos: int


class TimeseriesResponse(BaseModel):
    items: list[TimeseriesItem]


class TopItem(BaseModel):
    key: str
    label: str
    n_focos: int


class TopResponse(BaseModel):
    group: Literal["uf", "bioma", "mun", "uc", "ti"]
    items: list[TopItem]
    note: str | None = None


class TotalsResponse(BaseModel):
    n_focos: int


class ChoroplethResponse(BaseModel):
    from_date: date = Field(alias="from")
    to: date
    features: dict[str, Any]


class SummaryFilters(BaseModel):
    uf: str | None = None
    bioma: str | None = None
    mun: str | None = None
    uc: str | None = None
    ti: str | None = None


class SummaryResponse(BaseModel):
    from_date: date = Field(alias="from")
    to: date
    filters: SummaryFilters
    total_n_focos: int
    mean_per_day: float
    days: int
    peak_day: date | None
    peak_n_focos: int


class ValidateResponse(BaseModel):
    from_date: date = Field(alias="from")
    to: date
    filters: SummaryFilters
    totals_n_focos: int
    timeseries_sum_n_focos: int
    choropleth_sum_n_focos: int
    consistent: bool
