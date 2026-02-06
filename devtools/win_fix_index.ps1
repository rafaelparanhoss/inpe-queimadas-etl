param(
    [string]$Root = "sqlm"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $Root)) {
    Write-Error "path not found: $Root"
    exit 1
}

Write-Host "[win_fix_index] normalize attributes under $Root"
attrib -R -S -H "$Root\*" /S /D | Out-Null

$files = Get-ChildItem -Path $Root -Recurse -File
Write-Host "[win_fix_index] recreate files: $($files.Count)"

foreach ($file in $files) {
    $tmp = "$($file.FullName).tmp_rewrite"
    Copy-Item -LiteralPath $file.FullName -Destination $tmp -Force
    Remove-Item -LiteralPath $file.FullName -Force
    Move-Item -LiteralPath $tmp -Destination $file.FullName -Force
}

Write-Host "[win_fix_index] git add dry-run"
git add -A --dry-run
if ($LASTEXITCODE -ne 0) {
    Write-Error "git add dry-run failed"
    exit 1
}

Write-Host "[win_fix_index] done"
