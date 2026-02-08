import L from 'leaflet'

let map
let layer

const BRAZIL_BOUNDS = [[-34.5, -74.5], [6.0, -28.0]]

function getColor(v, breaks) {
  if (v > breaks[4]) return 5
  if (v > breaks[3]) return 4
  if (v > breaks[2]) return 3
  if (v > breaks[1]) return 2
  if (v > breaks[0]) return 1
  return 0
}

function computeBreaks(values) {
  const xs = values.filter(v => Number.isFinite(v)).sort((a, b) => a - b)
  if (!xs.length) return [0, 1, 2, 3, 4]
  const q = p => xs[Math.floor((xs.length - 1) * p)]
  return [q(0.2), q(0.4), q(0.6), q(0.8), q(0.95)]
}

export function initMap(onUfClick) {
  map = L.map('map', { zoomControl: true })
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18,
    attribution: '&copy; OpenStreetMap',
  }).addTo(map)
  map.fitBounds(BRAZIL_BOUNDS)
  layer = L.geoJSON({ type: 'FeatureCollection', features: [] }).addTo(map)

  return {
    setGeojson: (fc, selectedUf) => {
      if (layer) layer.remove()
      const values = (fc.features || []).map(f => Number(f.properties?.n_focos) || 0)
      const breaks = computeBreaks(values)

      const style = (feat) => {
        const uf = feat.properties?.uf
        const n = Number(feat.properties?.n_focos) || 0
        const c = getColor(n, breaks)
        const isSel = selectedUf && uf === selectedUf
        return {
          weight: isSel ? 3 : 1,
          opacity: 1,
          fillOpacity: isSel ? 0.65 : 0.45,
          color: '#222',
          fillColor: ['#f2f2f2', '#e0e0e0', '#cfcfcf', '#bdbdbd', '#ababab', '#9a9a9a'][c],
        }
      }

      const onEachFeature = (feat, lyr) => {
        const p = feat.properties || {}
        const uf = p.uf
        const n = Number(p.n_focos) || 0
        const mean = Number(p.mean_per_day) || 0
        lyr.bindTooltip(`${uf}<br/>focos: ${n}<br/>media/dia: ${mean.toFixed(1)}`, { sticky: true })
        lyr.on('click', () => onUfClick(uf))
      }

      layer = L.geoJSON(fc, { style, onEachFeature }).addTo(map)
    },
  }
}
