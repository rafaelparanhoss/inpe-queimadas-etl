import './styles.css'

import { state, setState, pickFilters, toggleFilter, clearDimensionFilters, setMunLayerEnabled } from './state.js'
import { api } from './api.js'
import { initMap } from './map.js'
import { initCharts } from './charts.js'
import { debounce, initUi, toTitleCasePt } from './ui.js'
import { assertClose, sum } from './validate.js'

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

function entityForFilter(filterKey) {
  if (filterKey === 'uf') return 'uf'
  if (filterKey === 'mun') return 'mun'
  if (filterKey === 'bioma') return 'bioma'
  if (filterKey === 'uc') return 'uc'
  if (filterKey === 'ti') return 'ti'
  return null
}

function startRequestCycle() {
  if (state.abort) state.abort.abort()
  const abort = new AbortController()
  setState({ abort })
  return abort
}

function setFilterUi() {
  const filters = pickFilters()
  const ufSelected = Boolean(filters.uf)
  const showMunLayer = Boolean(state.ui?.showMunLayer && ufSelected)
  ui.setFilterLabels(filters)
  ui.setActiveChips(filters)
  ui.setMunLayerToggle({ enabled: ufSelected, checked: showMunLayer })

  if (!ufSelected) {
    ui.setMunLayerHint('Selecione uma UF para habilitar a camada municipal.')
  } else if (showMunLayer) {
    ui.setMunLayerHint('Camada municipal ativa para a UF selecionada.')
  } else {
    ui.setMunLayerHint('Ative a camada municipal para navegar por municipios.')
  }
}

async function fetchBoundsAndFit(filterKey, value) {
  const entity = entityForFilter(filterKey)
  if (!entity || !value) return
  try {
    const bounds = await api.fetchBounds(entity, value, pickFilters())
    mapCtl.fitToBbox(bounds.bbox)
  } catch (err) {
    const msg = String(err?.message || err)
    if (msg.includes('geometry source not configured')) {
      ui.setStatus('Fonte de geometria nao configurada para fit bounds.')
    }
  }
}

function normalizeTopItems(items, group) {
  return (items || []).map((item) => {
    const out = { ...item }
    if (group === 'uf') {
      out.label = String(item.label || item.key).toUpperCase()
    } else {
      out.label = toTitleCasePt(item.label || item.key)
    }
    return out
  })
}

function toggleFilterAndRefresh(filterKey, value, { withBounds = true } = {}) {
  const prevUf = state.uf
  toggleFilter(filterKey, value)
  if (filterKey === 'uf' && !state.uf) {
    setMunLayerEnabled(false)
    if (prevUf) mapCtl.fitBrazil()
  }
  setFilterUi()

  const activeValue = state[filterKey]
  if (withBounds && activeValue) {
    void fetchBoundsAndFit(filterKey, activeValue)
  }
  refreshAllDebounced()
}

async function refreshAll() {
  const { from, to } = state
  if (!from || !to) return

  const filters = pickFilters()
  const showMunLayer = Boolean(state.ui?.showMunLayer && filters.uf)
  ui.setStatus('Carregando...')
  setFilterUi()

  const abort = startRequestCycle()
  const signal = abort.signal

  try {
    const munLimit = filters.uf ? 20 : 10

    const [summary, choroUf, topUfRaw, topBiomaRaw, topMunRaw, topUcRaw, topTiRaw, ts, tot, qa] = await Promise.all([
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

    let layerPayload = choroUf
    let layerType = 'uf'
    let munLayerError = ''
    if (showMunLayer) {
      try {
        const choroMun = await api.choroplethMun(from, to, filters, signal)
        layerPayload = choroMun
        layerType = 'mun'
      } catch (err) {
        munLayerError = `Camada municipal indisponivel: ${String(err?.message || err)}`
        ui.setMunLayerHint(munLayerError)
      }
    }

    const topUf = { ...topUfRaw, items: normalizeTopItems(topUfRaw.items, 'uf') }
    const topBioma = { ...topBiomaRaw, items: normalizeTopItems(topBiomaRaw.items, 'bioma') }
    const topMun = { ...topMunRaw, items: normalizeTopItems(topMunRaw.items, 'mun') }
    const topUc = { ...topUcRaw, items: normalizeTopItems(topUcRaw.items, 'uc') }
    const topTi = { ...topTiRaw, items: normalizeTopItems(topTiRaw.items, 'ti') }

    mapCtl.setChoropleth(layerPayload, {
      layerType,
      selectedUf: filters.uf,
      selectedMun: filters.mun,
    })

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
    const mapSumUf = (choroUf.geojson?.features || []).reduce(
      (acc, f) => acc + (Number(f.properties?.n_focos) || 0),
      0,
    )

    if (!assertClose(total, tsSum)) {
      ui.setStatus(`Inconsistencia: totals(${total}) != tsSum(${tsSum})`)
      return
    }
    if (!assertClose(total, mapSumUf)) {
      ui.setStatus(`Inconsistencia: totals(${total}) != mapUfSum(${mapSumUf})`)
      return
    }
    if (qa && qa.consistent === false) {
      ui.setStatus('Inconsistencia em /api/validate')
      return
    }

    if (!ui.getInputs().from || !ui.getInputs().to) {
      ui.setStatus('Defina um range de datas valido.')
      return
    }

    if (munLayerError) {
      ui.setStatus(munLayerError)
      return
    }

    if (layerType === 'mun' && state.ui.showMunLayer) {
      if (!filters.uf) {
        ui.setStatus('Selecione uma UF para manter a camada municipal.')
      } else {
        ui.setStatus('')
      }
    } else {
      ui.setStatus('')
    }
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
  mapCtl.fitBrazil()
  void refreshAll()
}

const refreshAllDebounced = debounce(() => {
  void refreshAll()
}, 150)

const ui = initUi()
const mapCtl = initMap((filterKey, value) => {
  toggleFilterAndRefresh(filterKey, value, { withBounds: false })
})
const chartsCtl = initCharts((filterKey, item) => {
  const key = item?.key
  if (!key) return
  toggleFilterAndRefresh(filterKey, key, { withBounds: true })
})

ui.onApply(applyInputs)
ui.onClear(clearFilters)
ui.onLast30(clearFilters)
ui.onMunLayerToggle((checked) => {
  if (!state.uf) {
    setMunLayerEnabled(false)
    setFilterUi()
    return
  }
  setMunLayerEnabled(checked)
  setFilterUi()
  refreshAllDebounced()
})
ui.onChipRemove((filterKey) => {
  setState({ [filterKey]: null })
  if (filterKey === 'uf') {
    setMunLayerEnabled(false)
    mapCtl.fitBrazil()
  }
  setFilterUi()
  refreshAllDebounced()
})
ui.onTopTablePick((filterKey, value) => {
  toggleFilterAndRefresh(filterKey, value, { withBounds: true })
})

{
  const { from, to } = defaultRangeLast30()
  clearDimensionFilters()
  setState({ from, to })
  ui.setInputs({ from, to })
  setFilterUi()
}

void refreshAll()
