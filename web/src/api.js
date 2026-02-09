const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000'

function qs(params) {
  const u = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) {
    if (v === null || v === undefined || v === '') continue
    u.set(k, v)
  }
  return u.toString()
}

async function fetchJson(path, params, signal) {
  const url = `${API_BASE}${path}?${qs(params)}`
  const res = await fetch(url, { signal })
  if (!res.ok) {
    const txt = await res.text().catch(() => '')
    throw new Error(`http ${res.status} ${txt}`)
  }
  return res.json()
}

function withFilters(from, to, filters, extra = {}) {
  return { from, to, ...(filters || {}), ...extra }
}

export const api = {
  choroplethUf: (from, to, filters, signal) =>
    fetchJson('/api/choropleth/uf', withFilters(from, to, filters), signal),

  choroplethMun: (from, to, filters, signal) =>
    fetchJson('/api/choropleth/mun', withFilters(from, to, filters), signal),

  top: (group, from, to, filters, limit, signal) =>
    fetchJson('/api/top', withFilters(from, to, filters, { group, limit }), signal),

  timeseriesTotal: (from, to, filters, signal) =>
    fetchJson('/api/timeseries/total', withFilters(from, to, filters), signal),

  totals: (from, to, filters, signal) =>
    fetchJson('/api/totals', withFilters(from, to, filters), signal),

  summary: (from, to, filters, signal) =>
    fetchJson('/api/summary', withFilters(from, to, filters), signal),

  validate: (from, to, filters, signal) =>
    fetchJson('/api/validate', withFilters(from, to, filters), signal),

  fetchBounds: (entity, key, filters, signal) =>
    fetchJson('/api/bounds', { entity, key, uf: filters?.uf }, signal),

  lookupMun: (key, signal) =>
    fetchJson('/api/lookup/mun', { key }, signal),
}
