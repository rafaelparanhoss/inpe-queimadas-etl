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
  const elUfSearch = document.getElementById('ufSearch')
  const elUfSelect = document.getElementById('ufSelect')
  const elMunSearch = document.getElementById('munSearch')
  const elMunSelect = document.getElementById('munSelect')
  const elBiomaSearch = document.getElementById('biomaSearch')
  const elBiomaSelect = document.getElementById('biomaSelect')
  const elUcSearch = document.getElementById('ucSearch')
  const elUcSelect = document.getElementById('ucSelect')
  const elTiSearch = document.getElementById('tiSearch')
  const elTiSelect = document.getElementById('tiSelect')
  const elApply = document.getElementById('apply')
  const elClear = document.getElementById('clear')
  const elLast30 = document.getElementById('last30')
  const elShowMunLayer = document.getElementById('showMunLayer')
  const elMunLayerHint = document.getElementById('munLayerHint')
  const elShowPoints = document.getElementById('showPoints')
  const elPointsBadge = document.getElementById('pointsBadge')
  const elPointsDateMode = document.getElementById('pointsDateMode')
  const elPointsDateCustom = document.getElementById('pointsDateCustom')
  const elPointsCustomCol = document.getElementById('pointsCustomCol')
  const elPointsHint = document.getElementById('pointsHint')
  const elPointsMeta = document.getElementById('pointsMeta')
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

  const FILTER_OPTION_LABEL = {
    uf: 'Todas',
    bioma: 'Todos',
    mun: 'Todos',
    uc: 'Todas',
    ti: 'Todas',
  }

  const filterSearchInputs = {
    uf: elUfSearch,
    bioma: elBiomaSearch,
    mun: elMunSearch,
    uc: elUcSearch,
    ti: elTiSearch,
  }

  const filterSelects = {
    uf: elUfSelect,
    bioma: elBiomaSelect,
    mun: elMunSelect,
    uc: elUcSelect,
    ti: elTiSelect,
  }

  function renderFilterOptionLabel(entity, item) {
    const raw = item?.label || item?.key || ''
    if (entity === 'uf') return String(raw || item?.key || '').toUpperCase()
    return toTitleCasePt(raw)
  }

  function setFilterOptions(entity, items) {
    const select = filterSelects[entity]
    if (!select) return
    const currentValue = select.value || ''
    select.innerHTML = ''
    const empty = document.createElement('option')
    empty.value = ''
    empty.textContent = FILTER_OPTION_LABEL[entity] || 'Todos'
    select.appendChild(empty)

    for (const item of (items || [])) {
      const key = String(item?.key || '').trim()
      if (!key) continue
      const option = document.createElement('option')
      option.value = key
      option.textContent = renderFilterOptionLabel(entity, item)
      option.dataset.label = String(item?.label || item?.key || key)
      select.appendChild(option)
    }

    if (currentValue && [...select.options].some((opt) => opt.value === currentValue)) {
      select.value = currentValue
    } else {
      select.value = ''
    }
  }

  function getSelectedFilterOption(entity) {
    const select = filterSelects[entity]
    if (!select) return null
    const option = select.options[select.selectedIndex]
    if (!option || !option.value) return null
    return {
      key: option.value,
      label: option.dataset.label || option.textContent || option.value,
    }
  }

  return {
    getInputs: () => ({ from: elFrom.value, to: elTo.value }),
    setInputs: ({ from, to }) => {
      elFrom.value = from
      elTo.value = to
    },
    onUfSelect: (fn) => elUfSelect.addEventListener('change', () => fn(elUfSelect.value)),
    onFilterSelect: (entity, fn) => {
      const select = filterSelects[entity]
      if (!select) return
      select.addEventListener('change', () => {
        const selected = getSelectedFilterOption(entity)
        fn(selected?.key || null, selected?.label || null)
      })
    },
    onFilterSearch: (entity, fn) => {
      const input = filterSearchInputs[entity]
      if (!input) return
      input.addEventListener('input', () => fn(input.value || ''))
    },
    setFilterOptions,
    setFilterSelect: (entity, key) => {
      const select = filterSelects[entity]
      if (!select) return
      const next = key ? String(key) : ''
      if ([...select.options].some((opt) => opt.value === next)) {
        select.value = next
      } else {
        select.value = ''
      }
    },
    setFilterSearchValue: (entity, value) => {
      const input = filterSearchInputs[entity]
      if (!input) return
      input.value = value || ''
    },
    setFilterDisabled: (entity, disabled) => {
      const select = filterSelects[entity]
      const input = filterSearchInputs[entity]
      if (select) select.disabled = Boolean(disabled)
      if (input) input.disabled = Boolean(disabled)
    },
    setUfSelect: (uf) => {
      const next = uf ? String(uf).toUpperCase() : ''
      if ([...elUfSelect.options].some((opt) => opt.value === next)) {
        elUfSelect.value = next
      } else {
        elUfSelect.value = ''
      }
    },
    onApply: (fn) => elApply.addEventListener('click', fn),
    onClear: (fn) => elClear.addEventListener('click', fn),
    onLast30: (fn) => elLast30.addEventListener('click', fn),
    onMunLayerToggle: (fn) => elShowMunLayer.addEventListener('change', () => fn(elShowMunLayer.checked)),
    onPointsToggle: (fn) => elShowPoints.addEventListener('change', () => fn(elShowPoints.checked)),
    onPointsDateMode: (fn) => elPointsDateMode.addEventListener('change', () => fn(elPointsDateMode.value)),
    onPointsDateCustom: (fn) => elPointsDateCustom.addEventListener('change', () => fn(elPointsDateCustom.value)),
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
    setFilterLabels: (filters, labels = {}) => {
      const ufDisplay = labels.uf || filters.uf || null
      const biomaDisplay = labels.bioma || filters.bioma || null
      const munDisplay = labels.mun || filters.mun || null
      const ucDisplay = labels.uc || filters.uc || null
      const tiDisplay = labels.ti || filters.ti || null
      ufLabel.textContent = ufDisplay ? String(ufDisplay).toUpperCase() : FALLBACK_LABEL.uf
      biomaLabel.textContent = biomaDisplay ? toTitleCasePt(biomaDisplay) : FALLBACK_LABEL.bioma
      munLabel.textContent = munDisplay ? toTitleCasePt(munDisplay) : FALLBACK_LABEL.mun
      ucLabel.textContent = ucDisplay ? toTitleCasePt(ucDisplay) : FALLBACK_LABEL.uc
      tiLabel.textContent = tiDisplay ? toTitleCasePt(tiDisplay) : FALLBACK_LABEL.ti
    },
    setActiveChips: (filters, labels = {}) => {
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
        const display = labels[key] || value
        chip.textContent = `${toTitleCasePt(key)}: ${toTitleCasePt(display)} x`
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
    setPointsDateControls: ({ mode, customDate, customVisible, disabled }) => {
      const safeMode = mode || 'peak_day'
      elPointsDateMode.value = safeMode
      elPointsDateMode.disabled = Boolean(disabled)
      elPointsDateCustom.value = customDate || ''
      elPointsDateCustom.disabled = Boolean(disabled || !customVisible)
      elPointsCustomCol.classList.toggle('hidden', !customVisible)
    },
    setPointsHint: (text) => {
      elPointsHint.textContent = text || ''
    },
    setPointsMeta: (payload) => {
      if (!payload) {
        elPointsMeta.textContent = ''
        return
      }
      const returned = Number(payload.returned || 0)
      const limit = Number(payload.limit || 0)
      const truncated = Boolean(payload.truncated)
      const dateTxt = String(payload.date || '-')
      const lines = [
        `Dia dos pontos: ${dateTxt}`,
        `Pontos carregados: ${formatInt(returned)}`,
      ]
      if (truncated) {
        lines.push(`Amostra ativa: limite ${formatInt(limit)}.`)
      }
      if (returned === 0) {
        lines.push('0 pontos nesse dia para os filtros atuais.')
      }
      elPointsMeta.textContent = lines.join('\n')
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
