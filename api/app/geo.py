from __future__ import annotations

from typing import Any, List, Tuple, Union


Coord = Tuple[float, float]
Ring = List[Coord]
PolygonCoords = List[Ring]
MultiPolygonCoords = List[PolygonCoords]


def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float))


def _depth(x: Any) -> int:
    d = 0
    cur = x
    while isinstance(cur, list) and cur:
        d += 1
        cur = cur[0]
    return d


def _close_ring(ring: Ring) -> Ring:
    if len(ring) < 3:
        return ring
    if ring[0] != ring[-1]:
        return ring + [ring[0]]
    return ring


def _sanitize_ring(ring: Any) -> Ring:
    out: Ring = []
    if not isinstance(ring, list):
        return out
    for pt in ring:
        if (
            isinstance(pt, list)
            and len(pt) >= 2
            and _is_number(pt[0])
            and _is_number(pt[1])
        ):
            out.append((float(pt[0]), float(pt[1])))
    out = _close_ring(out)
    if len(out) >= 4:
        return out
    return []


def normalize_poly_coords(poly_coords: Any) -> tuple[str, Union[PolygonCoords, MultiPolygonCoords]]:
    d = _depth(poly_coords)

    if d == 2:
        ring = _sanitize_ring(poly_coords)
        coords: PolygonCoords = [ring] if ring else []
        return "Polygon", coords

    if d == 3:
        rings: PolygonCoords = []
        for r in (poly_coords or []):
            ring = _sanitize_ring(r)
            if ring:
                rings.append(ring)
        return "Polygon", rings

    if d == 4:
        polys: MultiPolygonCoords = []
        for p in (poly_coords or []):
            rings: PolygonCoords = []
            if not isinstance(p, list):
                continue
            for r in p:
                ring = _sanitize_ring(r)
                if ring:
                    rings.append(ring)
            if rings:
                polys.append(rings)
        return "MultiPolygon", polys

    if isinstance(poly_coords, list):
        rings: PolygonCoords = []
        for r in poly_coords:
            ring = _sanitize_ring(r)
            if ring:
                rings.append(ring)
        if rings:
            return "Polygon", rings

    return "Polygon", []


def to_feature(uf: str, n_focos: int, mean_per_day: float, poly_coords: Any) -> dict:
    gtype, coords = normalize_poly_coords(poly_coords)
    return {
        "type": "Feature",
        "properties": {
            "uf": uf,
            "n_focos": int(n_focos),
            "mean_per_day": float(mean_per_day),
        },
        "geometry": {
            "type": gtype,
            "coordinates": coords,
        },
    }
