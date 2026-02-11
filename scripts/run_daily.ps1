param(
    [string]$Date
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not $Date) {
    $Date = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd")
}

$logDir = Join-Path $repoRoot "logs"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$logFile = Join-Path $logDir ("run_daily_{0}.log" -f $Date)

function Invoke-Step {
    param(
        [string]$Title,
        [string[]]$Args
    )

    $header = "==> {0}" -f $Title
    Write-Host $header
    $header | Out-File -FilePath $logFile -Encoding utf8 -Append

    & python @Args 2>&1 | Tee-Object -FilePath $logFile -Append
    if ($LASTEXITCODE -ne 0) {
        throw ("Step failed ({0}) with exit code {1}" -f $Title, $LASTEXITCODE)
    }
}

Invoke-Step -Title "ETL run (date=$Date)" -Args @(
    "-m", "etl.app", "run",
    "--date", $Date,
    "--checks",
    "--engine", "direct"
)

Invoke-Step -Title "Validate marts (dry-run)" -Args @(
    "-m", "etl.validate_marts",
    "--apply-minimal",
    "--dry-run",
    "--engine", "direct"
)

$done = "run_daily completed successfully | date={0}" -f $Date
Write-Host $done
$done | Out-File -FilePath $logFile -Encoding utf8 -Append
