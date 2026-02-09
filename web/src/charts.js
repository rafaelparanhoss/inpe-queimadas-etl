import Chart from 'chart.js/auto'

let topUfChart
let topBiomaChart
let topMunChart
let tsChart

const CHART_TEXT = '#dce8ff'
const CHART_GRID = 'rgba(156, 176, 206, 0.2)'
const BAR_COLOR = '#4f8cff'
const BAR_HOVER = '#7aa9ff'
const LINE_COLOR = '#38bdf8'

function baseScales() {
  return {
    x: {
      ticks: {
        color: CHART_TEXT,
        autoSkip: false,
        maxRotation: 50,
        minRotation: 0,
        font: { size: 12, weight: '500' },
      },
      grid: { color: CHART_GRID },
    },
    y: {
      ticks: { color: CHART_TEXT, font: { size: 12, weight: '500' } },
      grid: { color: CHART_GRID },
    },
  }
}

function createBarChart(ctx, onPick) {
  const chart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: [],
      datasets: [{
        label: 'Focos',
        data: [],
        backgroundColor: BAR_COLOR,
        hoverBackgroundColor: BAR_HOVER,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: { padding: { top: 6, right: 6, bottom: 2, left: 2 } },
      onClick: (_, elements) => {
        if (!elements?.length) return
        const idx = elements[0].index
        const item = chart.$items?.[idx]
        if (item) onPick(item)
      },
      plugins: {
        legend: { display: false, labels: { color: CHART_TEXT, font: { size: 12 } } },
        tooltip: {
          titleColor: '#f8fafc',
          bodyColor: '#dbeafe',
          backgroundColor: 'rgba(12, 21, 38, 0.96)',
          borderColor: 'rgba(119, 148, 194, 0.42)',
          borderWidth: 1,
        },
      },
      scales: baseScales(),
    },
  })
  chart.$items = []
  return chart
}

function setBarData(chart, items) {
  const safeItems = items || []
  chart.$items = safeItems
  chart.data.labels = safeItems.map((x) => x.label || x.key)
  chart.data.datasets[0].data = safeItems.map((x) => x.n_focos || 0)
  chart.update()
}

function computeTsTicks(nPoints) {
  if (nPoints <= 14) {
    return {
      autoSkip: false,
      maxTicksLimit: Math.max(2, nPoints),
      maxRotation: 0,
      minRotation: 0,
    }
  }
  if (nPoints <= 45) {
    return {
      autoSkip: true,
      maxTicksLimit: 9,
      maxRotation: 45,
      minRotation: 45,
    }
  }
  return {
    autoSkip: true,
    maxTicksLimit: 10,
    maxRotation: 45,
    minRotation: 45,
  }
}

function formatAxisDate(isoDate, granularity) {
  const raw = String(isoDate || '')
  const [yyyy = '', mm = '', dd = ''] = raw.split('-')
  if (granularity === 'month') {
    return yyyy && mm ? `${yyyy}-${mm}` : raw
  }
  if (mm && dd) return `${mm}-${dd}`
  return raw
}

export function initCharts(onFilterClick) {
  const topUfCtx = document.getElementById('topUfChart')
  const topBiomaCtx = document.getElementById('topBiomaChart')
  const topMunCtx = document.getElementById('topMunChart')
  const tsCtx = document.getElementById('tsChart')

  topUfChart = createBarChart(topUfCtx, (item) => onFilterClick('uf', item))
  topBiomaChart = createBarChart(topBiomaCtx, (item) => onFilterClick('bioma', item))
  topMunChart = createBarChart(topMunCtx, (item) => onFilterClick('mun', item))

  tsChart = new Chart(tsCtx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [{
        label: 'Focos / Dia',
        data: [],
        borderColor: LINE_COLOR,
        pointRadius: 2,
        pointHoverRadius: 4,
        tension: 0.25,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: { padding: { top: 6, right: 6, bottom: 2, left: 2 } },
      plugins: {
        legend: { display: false, labels: { color: CHART_TEXT, font: { size: 12 } } },
        tooltip: {
          titleColor: '#f8fafc',
          bodyColor: '#dbeafe',
          backgroundColor: 'rgba(12, 21, 38, 0.96)',
          borderColor: 'rgba(119, 148, 194, 0.42)',
          borderWidth: 1,
          callbacks: {
            title: (ctx) => {
              if (!ctx?.length) return ''
              const idx = ctx[0].dataIndex
              return tsChart.$fullLabels?.[idx] || String(ctx[0].label || '')
            },
          },
        },
      },
      scales: baseScales(),
    },
  })
  tsChart.$fullLabels = []
  tsChart.$granularity = 'day'

  return {
    setTopUf: (items) => setBarData(topUfChart, items),
    setTopBioma: (items) => setBarData(topBiomaChart, items),
    setTopMun: (items) => setBarData(topMunChart, items),
    setTimeseries: (items, granularity = 'day') => {
      const safeItems = items || []
      const fullLabels = safeItems.map((x) => String(x.day))
      tsChart.$fullLabels = fullLabels
      tsChart.$granularity = granularity
      tsChart.data.labels = fullLabels.map((x) => formatAxisDate(x, granularity))
      tsChart.data.datasets[0].data = safeItems.map((x) => x.n_focos || 0)
      const tickConfig = computeTsTicks(safeItems.length)
      tsChart.options.scales.x.ticks = {
        ...tsChart.options.scales.x.ticks,
        ...tickConfig,
      }
      tsChart.update()
    },
  }
}
