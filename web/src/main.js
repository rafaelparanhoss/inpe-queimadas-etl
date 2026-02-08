import './styles.css'

import { state, setState, pickFilters, toggleFilter, clearDimensionFilters } from './state.js'
import { api } from './api.js'
import { initMap } from './map.js'
import { initCharts } from './charts.js'
import { initUi, debounce } from './ui.js'
import { sum, assertClose } from './validate.js'

function todayIso() {
  const d = new Date()
  const yyyy = d.getFullYear()
  const mm = String(d.getMonth() + 1).padStart(2, '0')
  const dd = String(d.getDate()).padStart(2, '0')
  return `${yyyy}-${mm}-${dd}`
}

function addDaysIso(iso, delta) {
  const d = new Date(`${iso}T00:00:00`)
  d.setDate(d.getDate() + delta)
  const yyyy = d.getFullYear()
  const mm = String(d.getMonth() + 1).padStart(2, '0')
  const dd = String(d.getDate()).padStart(2, '0')
  return `${yyyy}-${mm}-${dd}`
}

function defaultRangeLast30() {
  const today = todayIso()
  const to = addDaysIso(today, 1)
  const from = addDaysIso(to, -30)
  return { from, to }
}

function startRequestCycle() {
  if (state.abort) state.abort.abort()
  const abort = new AbortController()
  setState({ abort })
  return abort
}

function setFilterUi() {
  const filters = pickFilters()
  ui.setFilterLabels(filters)
  ui.setActiveChips(filters)
}

function toggleFilterAndRefresh(filterKey, value) {
  toggleFilter(filterKey, value)
  setFilterUi()
  refreshAllDebounced()
}

async function refreshAll() {
  const { from, to } = state
  if (!from || !to) return

  const filters = pickFilters()
  ui.setStatus('carregando...')
  ui.setFilterLabels(filters)
  ui.setActiveChips(filters)

  const abort = startRequestCycle()
  const signal = abort.signal

  try {
    const munLimit = filters.uf ? 20 : 10

    const [summary, choro, topUf, topBioma, topMun, topUc, topTi, ts, tot, qa] = await Promise.all([
      api.summary(from, to, filters, signal),
      api.choroplethUf(from, to, filters, signal),
      api.top('uf', from, to, filters, 10, signal),
      api.top('bioma', from, to, filters, 10, signal),
      api.top('mun', from, to, filters, munLimit, signal),
      api.top('uc', from, to, filters, 10, signal),
      api.top('ti', from, to, filters, 10, signal),
      api.timeseriesTotal(from, to, filters, signal),
      api.totals(from, to, filters, signal),
      api.validate(from, to, filters, signal).catch(() => null),
    ])

    mapCtl.setGeojson(choro.features, filters.uf)
    chartsCtl.setTopUf(topUf.items)
    chartsCtl.setTopBioma(topBioma.items)
    chartsCtl.setTopMun(topMun.items)
    chartsCtl.setTimeseries(ts.items)
    ui.setTopUc(topUc.items)
    ui.setTopTi(topTi.items)
    ui.setKpis(summary)
    ui.setTotal(tot.n_focos)
    ui.setMunGuardrail(topMun.note || '')

    const total = Number(tot.n_focos || 0)
    const tsSum = sum(ts.items || [], 'n_focos')
    const mapSum = (choro.features?.features || []).reduce(
      (acc, f) => acc + (Number(f.properties?.n_focos) || 0),
      0,
    )

    if (!assertClose(total, tsSum)) {
      ui.setStatus(`inconsistencia: totals(${total}) != tsSum(${tsSum})`)
      return
    }
    if (!assertClose(total, mapSum)) {
      ui.setStatus(`inconsistencia: totals(${total}) != mapSum(${mapSum})`)
      return
    }
    if (qa && qa.consistent === false) {
      ui.setStatus('inconsistencia em /api/validate')
      return
    }
    ui.setStatus('')
  } catch (err) {
    if (err?.name === 'AbortError') return
    ui.setStatus(String(err?.message || err))
  }
}

function applyInputs() {
  const { from, to } = ui.getInputs()
  setState({ from: from || null, to: to || null })
  void refreshAll()
}

function clearFilters() {
  const { from, to } = defaultRangeLast30()
  clearDimensionFilters()
  setState({ from, to })
  ui.setInputs({ from, to })
  setFilterUi()
  void refreshAll()
}

const refreshAllDebounced = debounce(() => {
  void refreshAll()
}, 120)

const ui = initUi()
const mapCtl = initMap((uf) => toggleFilterAndRefresh('uf', uf))
const chartsCtl = initCharts((filterKey, value) => toggleFilterAndRefresh(filterKey, value))

ui.onApply(applyInputs)
ui.onClear(clearFilters)
ui.onLast30(clearFilters)
ui.onChipRemove((filterKey) => {
  setState({ [filterKey]: null })
  setFilterUi()
  refreshAllDebounced()
})
ui.onTopTablePick((filterKey, value) => toggleFilterAndRefresh(filterKey, value))

{
  const { from, to } = defaultRangeLast30()
  clearDimensionFilters()
  setState({ from, to })
  ui.setInputs({ from, to })
  setFilterUi()
}

void refreshAll()
