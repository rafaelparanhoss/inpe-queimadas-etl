import L from 'leaflet'

let map
let layer
let selectionOverlay
let legendControl
let legendEl

const BRAZIL_BOUNDS = [[-34.5, -74.5], [6.0, -28.0]]
const FALLBACK_COLORS = ['#1a1b2f', '#ffd166', '#fca311', '#f77f00', '#d62828', '#5a189a']

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

export function initMap(onFeaturePick) {
  map = L.map('map', { zoomControl: true })
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 18,
    attribution: '&copy; OpenStreetMap &copy; CARTO',
  }).addTo(map)
  map.fitBounds(BRAZIL_BOUNDS)
  layer = L.geoJSON({ type: 'FeatureCollection', features: [] }).addTo(map)
  selectionOverlay = null
  setLegend(null, 'Legenda')

  return {
    setChoropleth: (choro, options = {}) => {
      if (!choro?.geojson) return
      const layerType = options.layerType || 'uf'
      const selectedUf = options.selectedUf || null
      const selectedMun = options.selectedMun || null
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

      layer = L.geoJSON(geojson, { style, onEachFeature }).addTo(map)
    },
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
