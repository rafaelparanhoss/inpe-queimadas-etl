export const state = {
  from: null,
  to: null,
  uf: null,
  bioma: null,
  mun: null,
  uc: null,
  ti: null,
  ui: {
    showMunLayer: false,
  },
  abort: null,
}

export const FILTER_KEYS = ['uf', 'bioma', 'mun', 'uc', 'ti']

export function setState(patch) {
  Object.assign(state, patch)
}

export function pickFilters() {
  return {
    uf: state.uf,
    bioma: state.bioma,
    mun: state.mun,
    uc: state.uc,
    ti: state.ti,
  }
}

export function toggleFilter(key, value) {
  if (!FILTER_KEYS.includes(key)) return
  const nextValue = normalizeFilterValue(value)
  const next = state[key] === nextValue ? null : nextValue
  state[key] = next
}

export function clearDimensionFilters() {
  for (const key of FILTER_KEYS) {
    state[key] = null
  }
  state.ui.showMunLayer = false
}

function normalizeFilterValue(value) {
  if (value === null || value === undefined) return null
  const out = String(value).trim()
  return out || null
}

export function setMunLayerEnabled(enabled) {
  state.ui.showMunLayer = Boolean(enabled)
}
