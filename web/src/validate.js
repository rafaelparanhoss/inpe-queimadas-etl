export function sum(arr, key) {
  return arr.reduce((acc, x) => acc + (Number(x[key]) || 0), 0)
}

export function formatInt(n) {
  return new Intl.NumberFormat('pt-BR').format(n)
}

export function assertClose(a, b) {
  return a === b
}
