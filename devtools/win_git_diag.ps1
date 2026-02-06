param(
    [string]$Target = "sqlm/marts/aux/031_uf_poly_day_full.sql",
    [string]$LogPath = "logs/win_git_diag_ps1.log"
)

$ErrorActionPreference = "Continue"

if (-not (Test-Path ".git")) {
    Write-Error "[win_git_diag] not inside git repo"
    exit 1
}

$logDir = Split-Path $LogPath -Parent
if ($logDir -and -not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

function Write-Section([string]$Title) {
    Write-Output ""
    Write-Output "== $Title =="
}

& {
    Write-Section "env"
    Get-Date
    Get-Location
    git rev-parse --show-toplevel
    git --version
    Write-Output "MSYSTEM=$env:MSYSTEM"

    Write-Section "git core/feature config"
    git config --show-origin --list | Select-String -Pattern "core\\.|feature\\."

    Write-Section "target fs checks"
    Write-Output "target=$Target"
    $resolvedTarget = Resolve-Path -LiteralPath $Target -ErrorAction Stop
    $resolvedPath = $resolvedTarget.ProviderPath
    $targetParent = Split-Path -Path $resolvedPath -Parent
    Write-Output "resolved_path=$resolvedPath"
    Write-Output "resolved_parent=$targetParent"
    Write-Output ("parent_exists_testpath=" + (Test-Path -LiteralPath $targetParent))
    Write-Output ("parent_exists_io=" + [System.IO.Directory]::Exists($targetParent))
    Get-Item -Force -LiteralPath $resolvedPath
    Write-Output ("test_path=" + (Test-Path -LiteralPath $resolvedPath))

    Write-Section "python open/read test"
    python -c "import os; p=r'$resolvedPath'; print('exists',os.path.exists(p),'isfile',os.path.isfile(p)); print('read_bytes',len(open(p,'rb').read())) if os.path.exists(p) else print('missing')"

    Write-Section "git add verbose target"
    git add --verbose -- "$Target"

    Write-Section "git add trace target"
    $env:GIT_TRACE = "1"
    $env:GIT_TRACE_PERFORMANCE = "1"
    git add -- "$Target"
    Remove-Item Env:GIT_TRACE -ErrorAction SilentlyContinue
    Remove-Item Env:GIT_TRACE_PERFORMANCE -ErrorAction SilentlyContinue
} 2>&1 | Tee-Object -FilePath $LogPath

Write-Output "[win_git_diag] log saved: $LogPath"
