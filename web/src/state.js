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

export function setFilters(patch) {
  const next = {}
  for (const key of FILTER_KEYS) {
    if (Object.prototype.hasOwnProperty.call(patch, key)) {
      next[key] = normalizeFilterValue(patch[key])
    }
  }
  Object.assign(state, next)
  ensureValidFilterState()
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
  ensureValidFilterState()
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

export function ensureValidFilterState() {
  if (!state.uf && state.mun) {
    state.mun = null
  }
  if (!state.uf) {
    state.ui.showMunLayer = false
  }
}
