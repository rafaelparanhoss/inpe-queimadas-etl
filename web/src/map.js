import L from 'leaflet'

let map
let layer
let selectionOverlay
let legendControl
let legendEl
let pointsLayer
let pointsRaw
let pointsDateLabel
let viewportChangeHandler

const BRAZIL_BOUNDS = [[-34.5, -74.5], [6.0, -28.0]]
const FALLBACK_COLORS = ['#1a1b2f', '#ffd166', '#fca311', '#f77f00', '#d62828', '#5a189a']
const CHOROPLETH_PANE = 'choroplethPane'
const POINTS_PANE = 'pointsPane'

function numberLabel(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '0'
  return new Intl.NumberFormat('pt-BR', { maximumFractionDigits: 0 }).format(n)
}

function toTitleCasePt(text) {
  if (text === null || text === undefined) return ''
  const base = String(text).trim()
  if (!base) return ''
  return base
    .split(/\s+/)
    .map((raw) => {
      if (/^[A-Z0-9]{2,}$/.test(raw)) return raw
      const lower = raw.toLowerCase()
      return lower.charAt(0).toUpperCase() + lower.slice(1)
    })
    .join(' ')
}

function sanitizeBreaks(rawBreaks, domain) {
  const out = []
  for (const value of rawBreaks) {
    const n = Number(value)
    if (!Number.isFinite(n)) continue
    if (!out.length || n > out[out.length - 1]) {
      out.push(n)
    }
  }
  if (out.length >= 2) return out

  const d0 = Number.isFinite(Number(domain?.[0])) ? Number(domain[0]) : 0
  const d1Raw = Number.isFinite(Number(domain?.[1])) ? Number(domain[1]) : d0
  const d1 = d1Raw > d0 ? d1Raw : d0 + 1
  return [d0, d1]
}

function resolveLegendMeta(choro) {
  const rawBreaks = Array.isArray(choro?.breaks)
    ? choro.breaks.map((x) => Number(x)).filter((x) => Number.isFinite(x))
    : []
  const domain = Array.isArray(choro?.domain) && choro.domain.length === 2
    ? choro.domain.map((x) => Number(x))
    : [0, 0]
  const breaks = sanitizeBreaks(rawBreaks, domain)
  const method = String(choro?.method || 'quantile')
  const unit = String(choro?.unit || 'focos')
  const zeroClass = Boolean(choro?.zero_class)

  const rawPalette = Array.isArray(choro?.palette)
    ? choro.palette.filter((x) => typeof x === 'string' && x.trim())
    : []

  const classes = Math.max(1, breaks.length - 1)
  const needed = classes + (zeroClass ? 1 : 0)
  const palette = [...rawPalette]
  while (palette.length < needed) {
    const fill = FALLBACK_COLORS[Math.min(palette.length, FALLBACK_COLORS.length - 1)]
    palette.push(fill)
  }

  return { breaks, domain, method, unit, zeroClass, palette }
}

function getClassIndex(value, legendMeta) {
  const n = Number(value) || 0
  const classes = Math.max(1, legendMeta.breaks.length - 1)
  if (legendMeta.zeroClass && n <= 0) return 0

  const offset = legendMeta.zeroClass ? 1 : 0
  const lowerBound = Number(legendMeta.breaks[0])
  if (Number.isFinite(lowerBound) && n < lowerBound) {
    return offset
  }

  for (let i = 0; i < classes; i += 1) {
    const low = Number(legendMeta.breaks[i])
    const high = Number(legendMeta.breaks[i + 1])
    const isLast = i === classes - 1
    if (!Number.isFinite(high)) {
      return offset + i
    }
    if ((n >= low && n < high) || (isLast && n >= low)) {
      return offset + i
    }
  }

  return offset + classes - 1
}

function colorForValue(value, legendMeta) {
  if (!legendMeta?.palette?.length) return '#334155'
  const idx = getClassIndex(value, legendMeta)
  return legendMeta.palette[Math.min(idx, legendMeta.palette.length - 1)]
}

function ensureLegend() {
  if (legendControl) return
  legendControl = L.control({ position: 'bottomright' })
  legendControl.onAdd = () => {
    legendEl = L.DomUtil.create('div', 'map-legend')
    legendEl.innerHTML = '<div class="title">Legenda</div>'
    return legendEl
  }
  legendControl.addTo(map)
}

function setLegend(choro, title) {
  ensureLegend()
  if (!legendEl) return

  const legendMeta = resolveLegendMeta(choro)
  const breaks = legendMeta.breaks
  const palette = legendMeta.palette
  const classes = Math.max(1, breaks.length - 1)
  const items = []

  if (legendMeta.zeroClass) {
    items.push({ color: palette[0], label: `0 ${legendMeta.unit}` })
  }

  for (let i = 0; i < classes; i += 1) {
    const low = Number(breaks[i])
    const high = Number(breaks[i + 1])
    const colorIdx = (legendMeta.zeroClass ? 1 : 0) + i
    const isLast = i === classes - 1
    const rangeLabel = isLast
      ? `${numberLabel(low)} a ${numberLabel(high)} ${legendMeta.unit}`
      : `${numberLabel(low)} a < ${numberLabel(high)} ${legendMeta.unit}`
    items.push({
      color: palette[Math.min(colorIdx, palette.length - 1)],
      label: rangeLabel,
    })
  }

  const lines = items
    .map((item) => `<div class="item"><span class="swatch" style="background:${item.color}"></span><span>${item.label}</span></div>`)
    .join('')
  const domainLabel = `${numberLabel(legendMeta.domain[0])} - ${numberLabel(legendMeta.domain[1])}`

  legendEl.innerHTML = `
    <div class="title">${title || 'Legenda'}</div>
    <div class="sub">Metodo: ${legendMeta.method}</div>
    <div class="sub">Dominio: ${domainLabel}</div>
    ${lines}
  `
}

function getViewportBbox() {
  if (!map) return null
  const b = map.getBounds()
  if (!b.isValid()) return null
  const minLon = b.getWest()
  const minLat = b.getSouth()
  const maxLon = b.getEast()
  const maxLat = b.getNorth()
  return [minLon, minLat, maxLon, maxLat]
}

function clusterRadius(count) {
  if (count >= 1000) return 20
  if (count >= 300) return 16
  if (count >= 100) return 13
  if (count >= 30) return 11
  return 9
}

function clusterCellSize(zoom) {
  const raw = 60 / Math.pow(1.25, Math.max(0, zoom - 4))
  return Math.max(8, Math.min(60, raw))
}

function escapeHtml(text) {
  return String(text || '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;')
}

function splitMulti(value) {
  if (value === null || value === undefined) return []
  if (Array.isArray(value)) {
    return value
      .map((x) => String(x || '').trim())
      .filter((x) => x.length > 0)
  }
  return String(value)
    .split(/\s*[;|]\s*/)
    .map((x) => x.trim())
    .filter((x) => x.length > 0)
}

function mergeLabelKey(label, key) {
  const lbl = String(label || '').trim()
  const k = String(key || '').trim()
  if (lbl && k && lbl.toUpperCase() !== k.toUpperCase()) {
    return `${lbl} (${k})`
  }
  return lbl || k || ''
}

function mergeMultiLabelKey(labels, keys) {
  const out = []
  const n = Math.max(labels.length, keys.length)
  for (let i = 0; i < n; i += 1) {
    const item = mergeLabelKey(labels[i], keys[i])
    if (item && !out.includes(item)) out.push(item)
  }
  return out
}

function truncateList(values, maxItems = 2) {
  if (!values.length) return '-'
  if (values.length <= maxItems) return values.join(', ')
  const head = values.slice(0, maxItems).join(', ')
  return `${head} +${values.length - maxItems}`
}

function pointDetailHtml(point) {
  const n = Number(point.n || 1)
  const uf = String(point.uf || '').trim().toUpperCase() || '-'
  const mun = mergeLabelKey(point.mun_label, point.mun_key) || '-'
  const bioma = mergeLabelKey(point.bioma_label, point.bioma_key) || '-'
  const ucItems = mergeMultiLabelKey(splitMulti(point.uc_label), splitMulti(point.uc_key))
  const tiItems = mergeMultiLabelKey(splitMulti(point.ti_label), splitMulti(point.ti_key))
  const uc = truncateList(ucItems, 2)
  const ti = truncateList(tiItems, 2)
  const day = pointsDateLabel || '-'

  return [
    `<strong>Data:</strong> ${escapeHtml(day)}`,
    `<strong>Focos:</strong> ${numberLabel(n)}`,
    `<strong>UF:</strong> ${escapeHtml(uf)}`,
    `<strong>Municipio:</strong> ${escapeHtml(mun)}`,
    `<strong>Bioma:</strong> ${escapeHtml(bioma)}`,
    `<strong>UC(s):</strong> ${escapeHtml(uc)}`,
    `<strong>TI(s):</strong> ${escapeHtml(ti)}`,
  ].join('<br/>')
}

function renderPointsClusters() {
  if (!pointsLayer) return
  pointsLayer.clearLayers()
  if (!pointsRaw?.length || !map) return

  const b = map.getBounds()
  if (!b.isValid()) return
  const minLon = b.getWest()
  const minLat = b.getSouth()
  const maxLon = b.getEast()
  const maxLat = b.getNorth()
  const zoom = Math.max(0, Math.floor(map.getZoom()))
  const cellSize = clusterCellSize(zoom)
  const buckets = new Map()

  for (const p of pointsRaw) {
    const lon = Number(p.lon)
    const lat = Number(p.lat)
    const n = Number(p.n || 1)
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue
    if (lon < minLon || lon > maxLon || lat < minLat || lat > maxLat) continue
    const projected = map.project([lat, lon], zoom)
    const bx = Math.floor(projected.x / cellSize)
    const by = Math.floor(projected.y / cellSize)
    const key = `${bx}:${by}`
    const bucket = buckets.get(key)
    if (!bucket) {
      buckets.set(key, {
        count: 1,
        sumLon: lon,
        sumLat: lat,
        n,
        sample: p,
      })
      continue
    }
    bucket.count += 1
    bucket.sumLon += lon
    bucket.sumLat += lat
    bucket.n += n
  }

  for (const bucket of buckets.values()) {
    const lon = bucket.sumLon / bucket.count
    const lat = bucket.sumLat / bucket.count
    if (bucket.count > 1) {
      const count = Number(bucket.count || 0)
      const marker = L.circleMarker([lat, lon], {
        pane: POINTS_PANE,
        radius: clusterRadius(count),
        color: '#ffe29a',
        weight: 1.5,
        fillColor: '#d62828',
        fillOpacity: 0.74,
        interactive: true,
      })
      marker.bindTooltip(`Pontos: ${numberLabel(count)}`, { sticky: true })
      marker.bindPopup(`Cluster: ${numberLabel(count)} focos`)
      marker.on('click', () => {
        map.setView([lat, lon], Math.min(zoom + 2, 14))
      })
      pointsLayer.addLayer(marker)
      continue
    }

    const n = Number(bucket.n || 1)
    const marker = L.circleMarker([lat, lon], {
      pane: POINTS_PANE,
      radius: 4,
      color: '#ffd166',
      weight: 1,
      fillColor: '#f77f00',
      fillOpacity: 0.86,
      interactive: true,
    })
    const detailHtml = pointDetailHtml(bucket.sample || { n })
    marker.bindTooltip(detailHtml, { sticky: true })
    marker.bindPopup(detailHtml)
    pointsLayer.addLayer(marker)
  }
}

export function initMap(onFeaturePick) {
  map = L.map('map', { zoomControl: true })
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 18,
    attribution: '&copy; OpenStreetMap &copy; CARTO',
  }).addTo(map)
  if (!map.getPane(CHOROPLETH_PANE)) {
    map.createPane(CHOROPLETH_PANE)
    map.getPane(CHOROPLETH_PANE).style.zIndex = '410'
  }
  if (!map.getPane(POINTS_PANE)) {
    map.createPane(POINTS_PANE)
    map.getPane(POINTS_PANE).style.zIndex = '650'
  }
  map.fitBounds(BRAZIL_BOUNDS)
  layer = L.geoJSON({ type: 'FeatureCollection', features: [] }, { pane: CHOROPLETH_PANE }).addTo(map)
  pointsLayer = L.layerGroup().addTo(map)
  pointsRaw = []
  pointsDateLabel = null
  selectionOverlay = null
  viewportChangeHandler = null
  setLegend(null, 'Legenda')

  map.on('moveend zoomend', () => {
    renderPointsClusters()
    if (viewportChangeHandler) viewportChangeHandler()
  })

  return {
    setChoropleth: (choro, options = {}) => {
      if (!choro?.geojson) return
      const layerType = options.layerType || 'uf'
      const selectedUf = options.selectedUf || null
      const selectedMun = options.selectedMun || null
      const pointsEnabled = Boolean(options.pointsEnabled)
      const geojson = choro.geojson
      const legendMeta = resolveLegendMeta(choro)

      if (layer) layer.remove()
      setLegend(choro, layerType === 'mun' ? 'Municipios' : 'UFs')

      const style = (feat) => {
        const props = feat.properties || {}
        const n = Number(props.n_focos) || 0
        const uf = props.uf
        const key = props.key || uf
        const selected = layerType === 'mun'
          ? (selectedMun && String(selectedMun) === String(key))
          : (selectedUf && String(selectedUf) === String(uf))

        return {
          weight: selected ? 3.1 : 1.1,
          opacity: selected ? 1 : 0.85,
          color: selected ? '#f8fafc' : '#13223d',
          fillOpacity: n > 0 ? (selected ? 0.88 : 0.66) : 0.22,
          fillColor: colorForValue(n, legendMeta),
        }
      }

      const onEachFeature = (feat, lyr) => {
        if (pointsEnabled) return
        const p = feat.properties || {}
        const key = p.key || p.uf
        const uf = String(p.uf || '')
        const label = layerType === 'mun'
          ? toTitleCasePt(p.label || key || '')
          : String(p.uf || key || '').toUpperCase()
        const n = Number(p.n_focos) || 0
        const mean = Number(p.mean_per_day) || 0
        const title = layerType === 'mun' ? `Municipio: ${label}` : `UF: ${label}`

        lyr.bindTooltip(
          `${title}<br/>Focos: ${numberLabel(n)}<br/>Media/Dia: ${mean.toFixed(1)}`,
          { sticky: true },
        )

        lyr.on('mouseover', () => {
          lyr.setStyle({
            weight: 3.4,
            fillOpacity: 0.84,
          })
        })

        lyr.on('mouseout', () => {
          layer.resetStyle(lyr)
        })

        if (layerType === 'mun') {
          lyr.on('click', () => onFeaturePick('mun', String(key), String(label)))
        } else {
          lyr.on('click', () => onFeaturePick('uf', uf, uf))
        }
      }

      layer = L.geoJSON(geojson, {
        pane: CHOROPLETH_PANE,
        style,
        onEachFeature,
        interactive: !pointsEnabled,
      }).addTo(map)
    },
    setPointsData: (payload) => {
      const points = Array.isArray(payload?.points) ? payload.points : []
      pointsDateLabel = payload?.date ? String(payload.date) : null
      if (!points.length) {
        pointsRaw = []
        if (pointsLayer) pointsLayer.clearLayers()
        return
      }
      pointsRaw = points
        .filter((p) => Number.isFinite(Number(p.lon)) && Number.isFinite(Number(p.lat)))
        .map((p) => ({
          lon: Number(p.lon),
          lat: Number(p.lat),
          n: Number(p.n) || 1,
          uf: p.uf || null,
          mun_key: p.mun_key || null,
          mun_label: p.mun_label || null,
          bioma_key: p.bioma_key || null,
          bioma_label: p.bioma_label || null,
          uc_key: p.uc_key || null,
          uc_label: p.uc_label || null,
          ti_key: p.ti_key || null,
          ti_label: p.ti_label || null,
        }))
      renderPointsClusters()
    },
    clearPoints: () => {
      pointsRaw = []
      pointsDateLabel = null
      if (pointsLayer) pointsLayer.clearLayers()
    },
    onViewportChange: (fn) => {
      viewportChangeHandler = fn
    },
    getViewportBbox: () => getViewportBbox(),
    getZoom: () => (map ? map.getZoom() : 0),
    setSelectionOverlay: (geojson) => {
      if (selectionOverlay) {
        selectionOverlay.remove()
        selectionOverlay = null
      }
      if (!geojson || !Array.isArray(geojson.features) || !geojson.features.length) return false

      const style = {
        color: '#ffe29a',
        weight: 2.8,
        opacity: 1,
        fillColor: '#ff8c2b',
        fillOpacity: 0.16,
        dashArray: '6 4',
      }

      selectionOverlay = L.geoJSON(geojson, {
        style: () => style,
        interactive: false,
      }).addTo(map)
      return true
    },
    clearSelectionOverlay: () => {
      if (selectionOverlay) {
        selectionOverlay.remove()
        selectionOverlay = null
      }
    },
    fitToSelectionOverlay: () => {
      if (!selectionOverlay) return false
      const b = selectionOverlay.getBounds()
      if (!b.isValid()) return false
      map.fitBounds(b, {
        padding: [20, 20],
        maxZoom: 11,
      })
      return true
    },
    fitToBbox: (bbox) => {
      if (!Array.isArray(bbox) || bbox.length !== 4) return
      const [minLng, minLat, maxLng, maxLat] = bbox.map((x) => Number(x))
      if (![minLng, minLat, maxLng, maxLat].every(Number.isFinite)) return
      map.fitBounds([[minLat, minLng], [maxLat, maxLng]], {
        padding: [20, 20],
        maxZoom: 10,
      })
    },
    fitBrazil: () => {
      map.fitBounds(BRAZIL_BOUNDS)
    },
  }
}
