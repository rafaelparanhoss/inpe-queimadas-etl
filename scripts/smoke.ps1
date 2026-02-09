param(
    [string]$BaseUrl = "http://127.0.0.1:8000",
    [string]$From,
    [string]$To
)

$ErrorActionPreference = "Stop"

if (-not $From) {
    $From = (Get-Date).AddDays(-30).ToString("yyyy-MM-dd")
}
if (-not $To) {
    $To = (Get-Date).AddDays(1).ToString("yyyy-MM-dd")
}

function Invoke-Status200 {
    param([string]$Url)

    $code = curl.exe -s -o NUL -w "%{http_code}" "$Url"
    if ($code -ne "200") {
        throw "Expected 200, got $code for $Url"
    }
    Write-Host "200 $Url"
}

function Get-Json {
    param([string]$Url)

    $code = curl.exe -s -o NUL -w "%{http_code}" "$Url"
    if ($code -ne "200") {
        throw "Expected 200, got $code for $Url"
    }
    return (Invoke-RestMethod -Uri $Url -Method Get -TimeoutSec 120)
}

Write-Host "Running smoke against $BaseUrl from=$From to=$To"

Invoke-Status200 "$BaseUrl/health"
Invoke-Status200 "$BaseUrl/api/validate?from=$From&to=$To"
Invoke-Status200 "$BaseUrl/api/choropleth/uf?from=$From&to=$To"
Invoke-Status200 "$BaseUrl/api/points?date=$From&bbox=-61.0,-16.5,-55.0,-10.0&limit=5000"

$points = Get-Json "$BaseUrl/api/points?date=$From&bbox=-61.0,-16.5,-55.0,-10.0&limit=5000"
if ([int]$points.returned -gt [int]$points.limit) {
    throw "Points returned greater than limit"
}
Write-Host ("points returned={0} limit={1} truncated={2}" -f $points.returned, $points.limit, $points.truncated)

$summaryRs = Get-Json "$BaseUrl/api/summary?from=$From&to=$To&uf=RS"
if ($summaryRs.peak_day) {
    $peakDay = [string]$summaryRs.peak_day
    $pointsRsPeak = Get-Json "$BaseUrl/api/points?date=$peakDay&bbox=-74,-34,-34,6&limit=5000&uf=RS"
    if ([int]$summaryRs.total_n_focos -gt 0 -and [int]$pointsRsPeak.returned -le 0) {
        throw "Expected points for RS on peak_day=$peakDay"
    }
    Write-Host ("points RS peak_day={0} returned={1}" -f $peakDay, $pointsRsPeak.returned)
}

$topUf = Get-Json "$BaseUrl/api/top?group=uf&from=$From&to=$To&limit=1"
$ufKey = $null
if ($topUf.items -and $topUf.items.Count -gt 0) {
    $ufKey = [string]$topUf.items[0].key
}
if (-not $ufKey) {
    $ufKey = "MT"
}

Invoke-Status200 "$BaseUrl/api/choropleth/mun?from=$From&to=$To&uf=$ufKey"
Invoke-Status200 "$BaseUrl/api/bounds?entity=uf&key=$ufKey"

$topUc = Get-Json "$BaseUrl/api/top?group=uc&from=$From&to=$To&limit=1"
if ($topUc.items -and $topUc.items.Count -gt 0) {
    $ucKey = [string]$topUc.items[0].key
    if ($ucKey) {
        Invoke-Status200 "$BaseUrl/api/bounds?entity=uc&key=$ucKey"
        Invoke-Status200 "$BaseUrl/api/geo?entity=uc&key=$ucKey&from=$From&to=$To"
    }
}

$topTi = Get-Json "$BaseUrl/api/top?group=ti&from=$From&to=$To&limit=1"
if ($topTi.items -and $topTi.items.Count -gt 0) {
    $tiKey = [string]$topTi.items[0].key
    if ($tiKey) {
        Invoke-Status200 "$BaseUrl/api/bounds?entity=ti&key=$tiKey"
        Invoke-Status200 "$BaseUrl/api/geo?entity=ti&key=$tiKey&from=$From&to=$To"
    }
}

Write-Host "SMOKE OK"
