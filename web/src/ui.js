import { formatInt } from './validate.js'

const FALLBACK_LABEL = {
  uf: 'Todas',
  bioma: 'Todos',
  mun: 'Todos',
  uc: 'Todas',
  ti: 'Todas',
}

const PT_SMALL_WORDS = new Set(['de', 'da', 'do', 'das', 'dos', 'em', 'para', 'e'])

export function toTitleCasePt(text) {
  if (text === null || text === undefined) return ''
  const base = String(text).trim()
  if (!base) return ''
  let wordIndex = 0
  return base
    .split(/(\s+|[-/])/)
    .map((token) => {
      if (!token || /^\s+$/.test(token) || token === '-' || token === '/') {
        return token
      }
      if (/^[A-ZÀ-Ý0-9]{2,}$/.test(token)) {
        wordIndex += 1
        return token
      }
      const lower = token.toLowerCase()
      if (wordIndex > 0 && PT_SMALL_WORDS.has(lower)) {
        wordIndex += 1
        return lower
      }
      wordIndex += 1
      return lower.charAt(0).toUpperCase() + lower.slice(1)
    })
    .join('')
}

export function initUi() {
  const elFrom = document.getElementById('from')
  const elTo = document.getElementById('to')
  const elUfSelect = document.getElementById('ufSelect')
  const elApply = document.getElementById('apply')
  const elClear = document.getElementById('clear')
  const elLast30 = document.getElementById('last30')
  const elShowMunLayer = document.getElementById('showMunLayer')
  const elMunLayerHint = document.getElementById('munLayerHint')
  const elShowPoints = document.getElementById('showPoints')
  const elPointsBadge = document.getElementById('pointsBadge')
  const elPointsHint = document.getElementById('pointsHint')
  const ufLabel = document.getElementById('ufLabel')
  const biomaLabel = document.getElementById('biomaLabel')
  const munLabel = document.getElementById('munLabel')
  const ucLabel = document.getElementById('ucLabel')
  const tiLabel = document.getElementById('tiLabel')
  const totalLabel = document.getElementById('totalLabel')
  const status = document.getElementById('status')
  const chips = document.getElementById('activeChips')
  const munGuardrail = document.getElementById('munGuardrail')
  const kpiTotal = document.getElementById('kpiTotal')
  const kpiMean = document.getElementById('kpiMean')
  const kpiPeak = document.getElementById('kpiPeak')
  const kpiDays = document.getElementById('kpiDays')
  const ucTableBody = document.getElementById('ucTableBody')
  const tiTableBody = document.getElementById('tiTableBody')

  return {
    getInputs: () => ({ from: elFrom.value, to: elTo.value }),
    setInputs: ({ from, to }) => {
      elFrom.value = from
      elTo.value = to
    },
    onUfSelect: (fn) => elUfSelect.addEventListener('change', () => fn(elUfSelect.value)),
    setUfSelect: (uf) => {
      const next = uf ? String(uf).toUpperCase() : ''
      elUfSelect.value = next
    },
    onApply: (fn) => elApply.addEventListener('click', fn),
    onClear: (fn) => elClear.addEventListener('click', fn),
    onLast30: (fn) => elLast30.addEventListener('click', fn),
    onMunLayerToggle: (fn) => elShowMunLayer.addEventListener('change', () => fn(elShowMunLayer.checked)),
    onPointsToggle: (fn) => elShowPoints.addEventListener('change', () => fn(elShowPoints.checked)),
    onChipRemove: (fn) => chips.addEventListener('click', (ev) => {
      const target = ev.target
      if (!(target instanceof HTMLElement)) return
      const chip = target.closest('[data-chip-key]')
      if (!(chip instanceof HTMLElement)) return
      const key = chip.dataset.chipKey
      if (!key) return
      fn(key)
    }),
    onTopTablePick: (fn) => {
      const handler = (ev) => {
        const target = ev.target
        if (!(target instanceof HTMLElement)) return
        const btn = target.closest('.tablePick')
        if (!(btn instanceof HTMLElement)) return
        const key = btn.dataset.filter
        const value = btn.dataset.value
        const label = btn.dataset.label
        if (!key || !value) return
        fn(key, value, label || value)
      }
      ucTableBody.addEventListener('click', handler)
      tiTableBody.addEventListener('click', handler)
    },
    setFilterLabels: (filters) => {
      ufLabel.textContent = filters.uf ? String(filters.uf).toUpperCase() : FALLBACK_LABEL.uf
      biomaLabel.textContent = filters.bioma ? toTitleCasePt(filters.bioma) : FALLBACK_LABEL.bioma
      munLabel.textContent = filters.mun ? toTitleCasePt(filters.mun) : FALLBACK_LABEL.mun
      ucLabel.textContent = filters.uc ? toTitleCasePt(filters.uc) : FALLBACK_LABEL.uc
      tiLabel.textContent = filters.ti ? toTitleCasePt(filters.ti) : FALLBACK_LABEL.ti
    },
    setActiveChips: (filters) => {
      chips.innerHTML = ''
      const entries = Object.entries(filters).filter(([, value]) => value)
      if (!entries.length) {
        const empty = document.createElement('span')
        empty.className = 'chipMuted'
        empty.textContent = 'Sem filtros dimensionais'
        chips.appendChild(empty)
        return
      }
      for (const [key, value] of entries) {
        const chip = document.createElement('button')
        chip.type = 'button'
        chip.className = 'chip'
        chip.dataset.chipKey = key
        chip.textContent = `${toTitleCasePt(key)}: ${toTitleCasePt(value)} x`
        chips.appendChild(chip)
      }
    },
    setMunLayerToggle: ({ enabled, checked }) => {
      elShowMunLayer.disabled = !enabled
      elShowMunLayer.checked = Boolean(checked)
    },
    setMunLayerHint: (text) => {
      elMunLayerHint.textContent = text || ''
    },
    setPointsToggle: ({ checked }) => {
      elShowPoints.checked = Boolean(checked)
    },
    setPointsHint: (text) => {
      elPointsHint.textContent = text || ''
    },
    setPointsBadge: (payload) => {
      if (!payload) {
        elPointsBadge.className = 'pill hidden'
        elPointsBadge.textContent = ''
        return
      }
      if (payload.error) {
        elPointsBadge.className = 'pill error'
        elPointsBadge.textContent = 'erro'
        return
      }
      const returned = Number(payload.returned || 0)
      const limit = Number(payload.limit || 0)
      const truncated = Boolean(payload.truncated)
      elPointsBadge.className = truncated ? 'pill warn' : 'pill'
      elPointsBadge.textContent = truncated
        ? `truncado ${formatInt(returned)}/${formatInt(limit)}`
        : `${formatInt(returned)} pontos`
    },
    setMunGuardrail: (note) => {
      munGuardrail.textContent = note || ''
      munGuardrail.style.display = note ? 'block' : 'none'
    },
    setKpis: (summary) => {
      const total = Number(summary?.total_n_focos || 0)
      const mean = Number(summary?.mean_per_day || 0)
      const peakDay = summary?.peak_day || '-'
      const peakN = Number(summary?.peak_n_focos || 0)
      const days = Number(summary?.days || 0)
      kpiTotal.textContent = formatInt(total)
      kpiMean.textContent = mean.toFixed(2)
      kpiPeak.textContent = peakDay === '-' ? '-' : `${peakDay} (${formatInt(peakN)})`
      kpiDays.textContent = formatInt(days)
    },
    setTopUc: (items) => renderTopTable(ucTableBody, items, 'uc'),
    setTopTi: (items) => renderTopTable(tiTableBody, items, 'ti'),
    setTotal: (n) => {
      totalLabel.textContent = formatInt(n)
    },
    setStatus: (txt) => {
      status.textContent = txt || ''
    },
  }
}

function renderTopTable(tbody, items, filterKey) {
  tbody.innerHTML = ''
  if (!items?.length) {
    const row = document.createElement('tr')
    const cell = document.createElement('td')
    cell.colSpan = 2
    cell.className = 'empty'
    cell.textContent = 'Sem dados'
    row.appendChild(cell)
    tbody.appendChild(row)
    return
  }

  for (const item of items) {
    const row = document.createElement('tr')
    const labelCell = document.createElement('td')
    const valueCell = document.createElement('td')
    valueCell.className = 'num'

    const btn = document.createElement('button')
    btn.type = 'button'
    btn.className = 'tablePick'
    btn.dataset.filter = filterKey
    btn.dataset.value = item.key
    btn.dataset.label = item.label || item.key
    btn.textContent = toTitleCasePt(item.label || item.key)

    labelCell.appendChild(btn)
    valueCell.textContent = formatInt(item.n_focos || 0)
    row.appendChild(labelCell)
    row.appendChild(valueCell)
    tbody.appendChild(row)
  }
}

export function debounce(fn, ms) {
  let t = null
  return (...args) => {
    clearTimeout(t)
    t = setTimeout(() => fn(...args), ms)
  }
}
