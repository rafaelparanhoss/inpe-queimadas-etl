from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field


class TimeseriesItem(BaseModel):
    day: date
    n_focos: int


class TimeseriesResponse(BaseModel):
    granularity: Literal["day", "week", "month"]
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


class ChoroplethWithLegendResponse(BaseModel):
    from_date: date = Field(alias="from")
    to: date
    geojson: dict[str, Any]
    breaks: list[float]
    domain: list[float]
    method: Literal["quantile", "equal"]
    unit: Literal["focos"]
    zero_class: bool
    palette: list[str]
    note: str | None = None


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
    invalid_filter_state: bool
    break_monotonicity_ok: bool
    bounds_vs_geo_bbox_ratio: float | None = None
    bounds_consistent: bool | None = None
    points_endpoint_ok: bool | None = None
    points_returned_le_limit: bool | None = None


class MunicipalityLookupResponse(BaseModel):
    mun: str
    mun_nome: str
    uf: str
    uf_nome: str


class BoundsResponse(BaseModel):
    entity: Literal["uf", "mun", "bioma", "uc", "ti"]
    key: str
    bbox: list[float]
    center: list[float]


class GeoOverlayResponse(BaseModel):
    entity: Literal["uc", "ti"]
    key: str
    geojson: dict[str, Any]


class GeoOverlayQaResponse(BaseModel):
    entity: Literal["uc", "ti"]
    key: str
    label: str
    simplify_param_received: int
    tol_m_received: float
    simplify_applied: bool
    n_parts_before_union: int
    area_m2_union_before: float
    area_m2_union_after: float
    is_valid_before: bool
    is_valid_after: bool
    npoints_before_union: int
    npoints_out: int
    bbox_ratio: float | None = None
    warning_bbox_ratio: bool
    coords_hash: str
    bbox: list[float]


class PointItem(BaseModel):
    lon: float
    lat: float
    n: int


class PointsResponse(BaseModel):
    date: date
    bbox: list[float]
    returned: int
    limit: int
    truncated: bool
    points: list[PointItem]
