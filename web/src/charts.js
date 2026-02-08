import Chart from 'chart.js/auto'

let topUfChart
let topBiomaChart
let topMunChart
let tsChart

function createBarChart(ctx, onPick) {
  const chart = new Chart(ctx, {
    type: 'bar',
    data: { labels: [], datasets: [{ label: 'focos', data: [] }] },
    options: {
      responsive: true,
      animation: false,
      onClick: (_, elements) => {
        if (!elements?.length) return
        const idx = elements[0].index
        const key = chart.$keys?.[idx]
        if (key) onPick(key)
      },
      plugins: { legend: { display: false } },
      scales: { x: { ticks: { autoSkip: false, maxRotation: 50, minRotation: 0 } } },
    },
  })
  chart.$keys = []
  return chart
}

function setBarData(chart, items) {
  chart.$keys = (items || []).map((x) => x.key)
  chart.data.labels = (items || []).map((x) => x.label || x.key)
  chart.data.datasets[0].data = (items || []).map((x) => x.n_focos || 0)
  chart.update()
}

export function initCharts(onFilterClick) {
  const topUfCtx = document.getElementById('topUfChart')
  const topBiomaCtx = document.getElementById('topBiomaChart')
  const topMunCtx = document.getElementById('topMunChart')
  const tsCtx = document.getElementById('tsChart')

  topUfChart = createBarChart(topUfCtx, (key) => onFilterClick('uf', key))
  topBiomaChart = createBarChart(topBiomaCtx, (key) => onFilterClick('bioma', key))
  topMunChart = createBarChart(topMunCtx, (key) => onFilterClick('mun', key))

  tsChart = new Chart(tsCtx, {
    type: 'line',
    data: { labels: [], datasets: [{ label: 'focos/dia', data: [] }] },
    options: {
      responsive: true,
      animation: false,
      plugins: { legend: { display: false } },
    },
  })

  return {
    setTopUf: (items) => setBarData(topUfChart, items),
    setTopBioma: (items) => setBarData(topBiomaChart, items),
    setTopMun: (items) => setBarData(topMunChart, items),
    setTimeseries: (items) => {
      tsChart.data.labels = (items || []).map((x) => x.day)
      tsChart.data.datasets[0].data = (items || []).map((x) => x.n_focos || 0)
      tsChart.update()
    },
  }
}
