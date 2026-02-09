import './styles.css'

import {
  state,
  setState,
  setFilters,
  pickFilters,
  toggleFilter,
  clearDimensionFilters,
  setMunLayerEnabled,
  ensureValidFilterState,
} from './state.js'
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

function currentOverlaySelection(filters) {
  if (filters.ti) return { entity: 'ti', key: filters.ti }
  if (filters.uc) return { entity: 'uc', key: filters.uc }
  return null
}

function startRequestCycle() {
  if (state.abort) state.abort.abort()
  const abort = new AbortController()
  setState({ abort })
  return abort
}

let pointsAbort = null

function startPointsRequestCycle() {
  if (pointsAbort) pointsAbort.abort()
  pointsAbort = new AbortController()
  return pointsAbort
}

function resolvePointsDate(from, to) {
  if (!from) return { date: null, note: '' }
  const expectedTo = addDaysIso(from, 1)
  if (to === expectedTo) {
    return { date: from, note: '' }
  }
  return { date: from, note: 'MVP: pontos para 1 dia (from).' }
}

function setFilterUi() {
  const filters = pickFilters()
  const ufSelected = Boolean(filters.uf)
  const showMunLayer = Boolean(state.ui?.showMunLayer && ufSelected)
  const showPoints = Boolean(state.ui?.showPoints)
  ui.setUfSelect(filters.uf)
  ui.setFilterLabels(filters)
  ui.setActiveChips(filters)
  ui.setMunLayerToggle({ enabled: ufSelected, checked: showMunLayer })
  ui.setPointsToggle({ checked: showPoints })
  ui.setPointsHint('Pontos usam 1 dia (from) e bbox visivel do mapa.')

  if (!ufSelected) {
    ui.setMunLayerHint('Para municipios, selecione uma UF.')
  } else if (showMunLayer) {
    ui.setMunLayerHint('Camada municipal ativa para a UF selecionada.')
  } else {
    ui.setMunLayerHint('Ative a camada municipal para navegar por municipios.')
  }
}

async function applyMunicipalitySelection(munKey, { withBounds = true } = {}) {
  const key = String(munKey || '').trim()
  if (!key) return

  if (state.uf && state.mun === key) {
    setFilters({ mun: null })
    setFilterUi()
    if (withBounds && state.uf) {
      await fetchBoundsAndFit('uf', state.uf)
    }
    refreshAllDebounced()
    return
  }

  let derivedUf = state.uf ? String(state.uf).toUpperCase() : null

  try {
    const lookup = await api.lookupMun(key)
    if (lookup?.uf) {
      derivedUf = String(lookup.uf).toUpperCase()
    }
  } catch {
    if (!derivedUf) {
      ui.setStatus('Selecione uma UF para explorar municipios.')
      return
    }
  }

  if (!derivedUf) {
    ui.setStatus('Selecione uma UF para explorar municipios.')
    return
  }

  setFilters({ uf: derivedUf, mun: key })
  setMunLayerEnabled(true)
  setFilterUi()

  if (withBounds) {
    await fetchBoundsAndFit('mun', key)
  }

  refreshAllDebounced()
}

async function fetchBoundsAndFit(filterKey, value) {
  const entity = entityForFilter(filterKey)
  if (!entity || !value) return
  const filters = pickFilters()
  try {
    const bounds = await api.fetchBounds(entity, value, filters)
    mapCtl.fitToBbox(bounds.bbox)
  } catch (err) {
    const msg = String(err?.message || err)
    const fallbackUf = filters.uf && filterKey !== 'uf' ? filters.uf : null
    if (fallbackUf) {
      try {
        const ufBounds = await api.fetchBounds('uf', fallbackUf, filters)
        mapCtl.fitToBbox(ufBounds.bbox)
        ui.setStatus('Bounds da camada indisponivel; zoom aplicado no UF selecionado.')
        return
      } catch {
        mapCtl.fitBrazil()
      }
    } else {
      mapCtl.fitBrazil()
    }

    if (msg.includes('geometry source not configured')) {
      ui.setStatus('Fonte de geometria nao configurada para fit bounds.')
    } else if (msg.includes('geometry not found')) {
      ui.setStatus('Bounds nao encontrados para o item selecionado.')
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
  if (filterKey === 'mun') {
    void applyMunicipalitySelection(value, { withBounds })
    return
  }

  const prevUf = state.uf
  toggleFilter(filterKey, value)
  if (filterKey === 'uc' && state.uc) {
    setFilters({ ti: null })
  }
  if (filterKey === 'ti' && state.ti) {
    setFilters({ uc: null })
  }
  if ((filterKey === 'uc' || filterKey === 'ti') && withBounds) {
    const nextValue = state[filterKey]
    if (nextValue) {
      pendingOverlayFocus = { entity: filterKey, key: nextValue }
    } else {
      pendingOverlayFocus = null
      mapCtl.clearSelectionOverlay()
    }
  }
  if (filterKey === 'uf') {
    if (!state.uf) {
      setFilters({ mun: null })
      setMunLayerEnabled(false)
      if (prevUf) mapCtl.fitBrazil()
    } else if (prevUf && state.uf !== prevUf) {
      setFilters({ mun: null })
      setMunLayerEnabled(true)
    } else {
      setMunLayerEnabled(true)
    }
  }

  ensureValidFilterState()
  setFilterUi()

  const activeValue = state[filterKey]
  if (withBounds && activeValue && filterKey !== 'uc' && filterKey !== 'ti') {
    void fetchBoundsAndFit(filterKey, activeValue)
  }
  refreshAllDebounced()
}

async function refreshPointsLayer() {
  const showPoints = Boolean(state.ui?.showPoints)
  if (!showPoints) {
    mapCtl.clearPoints()
    ui.setPointsBadge(null)
    return ''
  }

  const { from, to } = state
  if (!from || !to) return ''
  const filters = pickFilters()
  const bbox = mapCtl.getViewportBbox()
  if (!bbox) return ''

  const { date, note } = resolvePointsDate(from, to)
  if (!date) return ''

  const abort = startPointsRequestCycle()
  const signal = abort.signal
  try {
    const payload = await api.points(date, bbox, filters, 20000, signal)
    mapCtl.setPointsData(payload)
    ui.setPointsBadge(payload)
    if (note) {
      ui.setPointsHint(note)
    } else {
      ui.setPointsHint('Pontos usam 1 dia (from) e bbox visivel do mapa.')
    }
  } catch (err) {
    if (err?.name === 'AbortError') return ''
    mapCtl.clearPoints()
    ui.setPointsBadge({ error: true })
    return `Pontos indisponiveis: ${String(err?.message || err)}`
  }
  return ''
}

async function refreshAll() {
  ensureValidFilterState()
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

    let overlayError = ''
    const overlaySelection = currentOverlaySelection(filters)
    if (overlaySelection) {
      const focusRequested = Boolean(
        pendingOverlayFocus
        && pendingOverlayFocus.entity === overlaySelection.entity
        && String(pendingOverlayFocus.key) === String(overlaySelection.key),
      )
      try {
        const overlayGeo = await api.fetchGeo(
          overlaySelection.entity,
          overlaySelection.key,
          from,
          to,
          filters,
          signal,
        )
        mapCtl.setSelectionOverlay(overlayGeo.geojson)
        if (focusRequested) {
          const fitted = mapCtl.fitToSelectionOverlay()
          if (!fitted) {
            await fetchBoundsAndFit(overlaySelection.entity, overlaySelection.key)
          }
          pendingOverlayFocus = null
        }
      } catch (err) {
        overlayError = `Destaque ${overlaySelection.entity.toUpperCase()} indisponivel: ${String(err?.message || err)}`
        mapCtl.clearSelectionOverlay()
        if (focusRequested) {
          pendingOverlayFocus = null
          await fetchBoundsAndFit(overlaySelection.entity, overlaySelection.key)
        }
      }
    } else {
      mapCtl.clearSelectionOverlay()
      pendingOverlayFocus = null
    }

    chartsCtl.setTopUf(topUf.items)
    chartsCtl.setTopBioma(topBioma.items)
    chartsCtl.setTopMun(topMun.items)
    chartsCtl.setTimeseries(ts.items, ts.granularity || 'day')
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

    const pointsError = await refreshPointsLayer()

    if (munLayerError) {
      ui.setStatus(munLayerError)
      return
    }
    if (overlayError) {
      ui.setStatus(overlayError)
      return
    }
    if (pointsError) {
      ui.setStatus(pointsError)
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
  if (pointsAbort) {
    pointsAbort.abort()
    pointsAbort = null
  }
  if (state.ui) {
    state.ui.showPoints = false
  }
  pendingOverlayFocus = null
  setState({ from, to })
  ui.setInputs({ from, to })
  mapCtl.clearSelectionOverlay()
  mapCtl.clearPoints()
  ui.setPointsBadge(null)
  setFilterUi()
  mapCtl.fitBrazil()
  void refreshAll()
}

let pendingOverlayFocus = null

const refreshAllDebounced = debounce(() => {
  void refreshAll()
}, 150)

const refreshPointsDebounced = debounce(() => {
  if (!state.ui?.showPoints) return
  void refreshPointsLayer()
}, 320)

const ui = initUi()
const mapCtl = initMap((filterKey, value) => {
  toggleFilterAndRefresh(filterKey, value, { withBounds: false })
})
mapCtl.onViewportChange(() => {
  refreshPointsDebounced()
})
const chartsCtl = initCharts((filterKey, item) => {
  const key = item?.key
  if (!key) return
  toggleFilterAndRefresh(filterKey, key, { withBounds: true })
})

ui.onApply(applyInputs)
ui.onClear(clearFilters)
ui.onLast30(clearFilters)
ui.onUfSelect((value) => {
  const uf = value ? String(value).trim().toUpperCase() : null
  if (!uf) {
    setFilters({ uf: null, mun: null })
    setMunLayerEnabled(false)
    setFilterUi()
    mapCtl.fitBrazil()
    refreshAllDebounced()
    return
  }

  const changedUf = state.uf !== uf
  setFilters({ uf, mun: changedUf ? null : state.mun })
  setMunLayerEnabled(true)
  setFilterUi()
  void fetchBoundsAndFit('uf', uf)
  refreshAllDebounced()
})
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
ui.onPointsToggle((checked) => {
  if (state.ui) {
    state.ui.showPoints = Boolean(checked)
  }
  setFilterUi()
  if (!checked) {
    if (pointsAbort) {
      pointsAbort.abort()
      pointsAbort = null
    }
    mapCtl.clearPoints()
    ui.setPointsBadge(null)
    return
  }
  void refreshPointsLayer()
})
ui.onChipRemove((filterKey) => {
  if (filterKey === 'uf') {
    setFilters({ uf: null, mun: null })
  } else {
    setFilters({ [filterKey]: null })
  }
  if (filterKey === 'uf') {
    setMunLayerEnabled(false)
    mapCtl.fitBrazil()
  }
  if (filterKey === 'uc' || filterKey === 'ti') {
    pendingOverlayFocus = null
    mapCtl.clearSelectionOverlay()
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
