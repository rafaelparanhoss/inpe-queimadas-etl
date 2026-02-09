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
      ticks: { color: CHART_TEXT, autoSkip: false, maxRotation: 50, minRotation: 0 },
      grid: { color: CHART_GRID },
    },
    y: {
      ticks: { color: CHART_TEXT },
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
      animation: false,
      onClick: (_, elements) => {
        if (!elements?.length) return
        const idx = elements[0].index
        const item = chart.$items?.[idx]
        if (item) onPick(item)
      },
      plugins: {
        legend: { display: false, labels: { color: CHART_TEXT } },
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
      animation: false,
      plugins: {
        legend: { display: false, labels: { color: CHART_TEXT } },
      },
      scales: baseScales(),
    },
  })

  return {
    setTopUf: (items) => setBarData(topUfChart, items),
    setTopBioma: (items) => setBarData(topBiomaChart, items),
    setTopMun: (items) => setBarData(topMunChart, items),
    setTimeseries: (items) => {
      const safeItems = items || []
      tsChart.data.labels = safeItems.map((x) => x.day)
      tsChart.data.datasets[0].data = safeItems.map((x) => x.n_focos || 0)
      tsChart.update()
    },
  }
}
