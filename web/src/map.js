import L from 'leaflet'

let map
let layer
let legendControl
let legendEl

const BRAZIL_BOUNDS = [[-34.5, -74.5], [6.0, -28.0]]

function numberLabel(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return '0'
  return new Intl.NumberFormat('pt-BR', { maximumFractionDigits: 0 }).format(n)
}

function colorForValue(value, breaks, palette) {
  if (!Array.isArray(palette) || !palette.length) return '#334155'
  const n = Number(value) || 0
  if (n <= 0) return palette[0]
  const safeBreaks = Array.isArray(breaks) ? breaks : []
  for (let i = 0; i < safeBreaks.length; i += 1) {
    if (n <= Number(safeBreaks[i])) {
      return palette[Math.min(i + 1, palette.length - 1)]
    }
  }
  return palette[palette.length - 1]
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
  const breaks = Array.isArray(choro?.breaks) ? choro.breaks : []
  const palette = Array.isArray(choro?.palette) ? choro.palette : []
  const items = []

  if (palette.length) {
    items.push({ color: palette[0], label: '0' })
    for (let i = 0; i < breaks.length; i += 1) {
      const low = i === 0 ? 1 : Math.ceil(Number(breaks[i - 1]))
      const high = Math.ceil(Number(breaks[i]))
      const label = i === 0 ? `1 - ${numberLabel(high)}` : `${numberLabel(low)} - ${numberLabel(high)}`
      items.push({ color: palette[Math.min(i + 1, palette.length - 1)], label })
    }
  }

  const lines = items.map((item) => {
    return `<div class="item"><span class="swatch" style="background:${item.color}"></span><span>${item.label}</span></div>`
  })

  legendEl.innerHTML = `
    <div class="title">${title || 'Legenda'}</div>
    ${lines.join('')}
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
  setLegend(null, 'Legenda')

  return {
    setChoropleth: (choro, options = {}) => {
      if (!choro?.geojson) return
      const layerType = options.layerType || 'uf'
      const selectedUf = options.selectedUf || null
      const selectedMun = options.selectedMun || null
      const geojson = choro.geojson

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
          weight: selected ? 2.8 : 0.8,
          opacity: selected ? 1 : 0.8,
          color: selected ? '#f8fafc' : '#1f2937',
          fillOpacity: n > 0 ? (selected ? 0.85 : 0.66) : 0.28,
          fillColor: colorForValue(n, choro.breaks, choro.palette),
        }
      }

      const onEachFeature = (feat, lyr) => {
        const p = feat.properties || {}
        const key = p.key || p.uf
        const uf = p.uf || ''
        const label = p.label || p.uf || key || ''
        const n = Number(p.n_focos) || 0
        const mean = Number(p.mean_per_day) || 0
        lyr.bindTooltip(
          `${label}<br/>UF: ${uf}<br/>Focos: ${numberLabel(n)}<br/>Media/Dia: ${mean.toFixed(1)}`,
          { sticky: true },
        )
        if (layerType === 'mun') {
          lyr.on('click', () => onFeaturePick('mun', String(key), String(label)))
        } else {
          lyr.on('click', () => onFeaturePick('uf', String(uf), String(uf)))
        }
      }

      layer = L.geoJSON(geojson, { style, onEachFeature }).addTo(map)
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
