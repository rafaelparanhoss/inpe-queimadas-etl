param(
    [switch]$ForceUnlock
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path ".").Path
$gitDir = Join-Path $repoRoot ".git"
$lockPath = Join-Path $gitDir "index.lock"

if (-not (Test-Path $gitDir)) {
    Write-Error "[index_recover] .git not found in current directory"
    exit 1
}

Write-Host "[index_recover] repo: $repoRoot"
Write-Host "[index_recover] lock: $lockPath"

$gitProcs = @()
try {
    $gitProcs += Get-Process -Name git -ErrorAction SilentlyContinue
} catch {}
try {
    $gitProcs += Get-Process -Name "git-remote-https" -ErrorAction SilentlyContinue
} catch {}
try {
    $gitProcs += Get-Process -Name "git-lfs" -ErrorAction SilentlyContinue
} catch {}
$gitProcs = $gitProcs | Select-Object -Unique

if (Test-Path $lockPath) {
    Write-Host "[index_recover] index.lock present"
    if ($gitProcs.Count -gt 0) {
        Write-Host "[index_recover] git process running. do not remove lock now."
        $gitProcs | Select-Object Name, Id | Format-Table -AutoSize
        exit 2
    }

    if (-not $ForceUnlock) {
        Write-Host "[index_recover] no git process found."
        Write-Host "[index_recover] re-run with -ForceUnlock to remove stale index.lock."
        exit 3
    }

    Remove-Item -LiteralPath $lockPath -Force
    Write-Host "[index_recover] stale index.lock removed"
} else {
    Write-Host "[index_recover] index.lock absent"
}

Write-Host "[index_recover] running git add dry-run"
git add -A --dry-run
if ($LASTEXITCODE -ne 0) {
    Write-Error "[index_recover] git add dry-run still failing"
    exit 4
}

Write-Host "[index_recover] ok"
