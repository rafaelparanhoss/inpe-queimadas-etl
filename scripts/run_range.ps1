[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$From,

    [Parameter(Mandatory = $true)]
    [string]$To,

    [switch]$NoChecks,

    [string]$Engine = "direct",

    [bool]$StopOnError = $true,

    [int]$SleepSeconds = 0,

    [string]$LogDir = "logs",

    [string]$PythonExe = "python",

    [string]$SmokeBaseUrl = ""
)

$ErrorActionPreference = "Stop"

function Parse-IsoDate {
    param([string]$Value, [string]$Name)

    $parsed = [DateTime]::MinValue
    $ok = [DateTime]::TryParseExact(
        $Value,
        "yyyy-MM-dd",
        [System.Globalization.CultureInfo]::InvariantCulture,
        [System.Globalization.DateTimeStyles]::None,
        [ref]$parsed
    )
    if (-not $ok) {
        throw "invalid $Name '$Value' (expected YYYY-MM-DD)"
    }
    return $parsed.Date
}

function Load-DotEnv {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return
    }
    foreach ($line in Get-Content -Path $Path -Encoding UTF8) {
        $raw = $line.Trim()
        if (-not $raw -or $raw.StartsWith("#")) {
            continue
        }
        $eq = $raw.IndexOf("=")
        if ($eq -lt 1) {
            continue
        }
        $key = $raw.Substring(0, $eq).Trim()
        $val = $raw.Substring($eq + 1).Trim()
        if (($val.StartsWith('"') -and $val.EndsWith('"')) -or ($val.StartsWith("'") -and $val.EndsWith("'"))) {
            $val = $val.Substring(1, $val.Length - 2)
        }
        Set-Item -Path ("Env:{0}" -f $key) -Value $val
    }
}

function Write-RunLine {
    param([string]$Message, [string]$GlobalLog)

    Write-Host $Message
    $Message | Out-File -FilePath $GlobalLog -Encoding utf8 -Append
}

function Invoke-LoggedProcess {
    param(
        [string]$Executable,
        [string[]]$Arguments,
        [string]$WorkingDirectory,
        [string]$LogFile,
        [string]$GlobalLog,
        [string]$StdoutTmp,
        [string]$StderrTmp
    )

    if (Test-Path $StdoutTmp) { Remove-Item -Force $StdoutTmp }
    if (Test-Path $StderrTmp) { Remove-Item -Force $StderrTmp }

    $proc = Start-Process `
        -FilePath $Executable `
        -ArgumentList $Arguments `
        -WorkingDirectory $WorkingDirectory `
        -NoNewWindow `
        -Wait `
        -PassThru `
        -RedirectStandardOutput $StdoutTmp `
        -RedirectStandardError $StderrTmp

    if (Test-Path $StdoutTmp) {
        Get-Content $StdoutTmp | Tee-Object -FilePath $LogFile -Append | Tee-Object -FilePath $GlobalLog -Append | Out-Null
        Remove-Item -Force $StdoutTmp
    }
    if (Test-Path $StderrTmp) {
        Get-Content $StderrTmp | Tee-Object -FilePath $LogFile -Append | Tee-Object -FilePath $GlobalLog -Append | Out-Null
        Remove-Item -Force $StderrTmp
    }

    return $proc.ExitCode
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$pythonCmd = Get-Command $PythonExe -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    throw "python executable not found in PATH (tried '$PythonExe')"
}

Load-DotEnv -Path (Join-Path $repoRoot ".env")

# Make module discovery deterministic when running from repo root.
if ($env:PYTHONPATH) {
    $env:PYTHONPATH = ("src;{0}" -f $env:PYTHONPATH)
}
else {
    $env:PYTHONPATH = "src"
}

# Avoid uv cache permission problems on locked user profiles.
$uvCacheDir = Join-Path $repoRoot ".uv_cache"
New-Item -ItemType Directory -Path $uvCacheDir -Force | Out-Null
$env:UV_CACHE_DIR = $uvCacheDir

# Fail fast with a clear message if selected Python cannot run ETL dependencies.
$probe = Start-Process `
    -FilePath $pythonCmd.Source `
    -ArgumentList @("-c", "__import__('etl');__import__('psycopg')") `
    -WorkingDirectory $repoRoot `
    -NoNewWindow `
    -Wait `
    -PassThru
if ($probe.ExitCode -ne 0) {
    throw "python '$($pythonCmd.Source)' cannot import required modules (etl, psycopg). Use -PythonExe pointing to your ETL environment."
}

$fromDate = Parse-IsoDate -Value $From -Name "from"
$toDate = Parse-IsoDate -Value $To -Name "to"
if ($fromDate -ge $toDate) {
    throw "invalid range: require from < to (to is exclusive)"
}
if ($SleepSeconds -lt 0) {
    throw "SleepSeconds must be >= 0"
}

$baseLogDir = Join-Path $repoRoot $LogDir
New-Item -ItemType Directory -Path $baseLogDir -Force | Out-Null

$runStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$runDir = Join-Path $baseLogDir ("run_range_{0}" -f $runStamp)
New-Item -ItemType Directory -Path $runDir -Force | Out-Null
$globalLog = Join-Path $baseLogDir ("run_range_{0}.log" -f $runStamp)

$dates = @()
$cursor = $fromDate
while ($cursor -lt $toDate) {
    $dates += $cursor
    $cursor = $cursor.AddDays(1)
}

$total = $dates.Count
$failedDates = New-Object System.Collections.Generic.List[string]
$durations = [ordered]@{}
$runStart = Get-Date

Write-RunLine -GlobalLog $globalLog -Message ("run_range start | from={0} | to={1} | days={2} | checks={3} | engine={4} | stop_on_error={5}" -f $fromDate.ToString("yyyy-MM-dd"), $toDate.ToString("yyyy-MM-dd"), $total, (-not $NoChecks), $Engine, $StopOnError)
Write-RunLine -GlobalLog $globalLog -Message ("uv_cache_dir={0}" -f $uvCacheDir)

for ($i = 0; $i -lt $total; $i++) {
    $day = $dates[$i]
    $dayStr = $day.ToString("yyyy-MM-dd")
    $dayLog = Join-Path $runDir ("{0}.log" -f $dayStr)
    $started = Get-Date

    Write-RunLine -GlobalLog $globalLog -Message ("[{0}/{1}] {2} start ..." -f ($i + 1), $total, $dayStr)

    # Daily pass: only ETL + enrich + marts daily. No sqlm/canonical/checks.
    $args = @(
        "-m", "etl.app", "run",
        "--date", $dayStr,
        "--engine", $Engine,
        "--mode", "full"
    )

    $stdoutTmp = Join-Path $runDir ("{0}.stdout.tmp" -f $dayStr)
    $stderrTmp = Join-Path $runDir ("{0}.stderr.tmp" -f $dayStr)
    $exitCode = Invoke-LoggedProcess `
        -Executable $pythonCmd.Source `
        -Arguments $args `
        -WorkingDirectory $repoRoot `
        -LogFile $dayLog `
        -GlobalLog $globalLog `
        -StdoutTmp $stdoutTmp `
        -StderrTmp $stderrTmp

    $elapsed = (Get-Date) - $started
    $elapsedText = "{0:mm\:ss}" -f $elapsed
    $durations[$dayStr] = $elapsedText

    if ($exitCode -eq 0) {
        Write-RunLine -GlobalLog $globalLog -Message ("[{0}/{1}] {2} OK ({3})" -f ($i + 1), $total, $dayStr, $elapsedText)
    }
    else {
        $failedDates.Add($dayStr) | Out-Null
        Write-RunLine -GlobalLog $globalLog -Message ("[{0}/{1}] {2} FAIL exit={3} ({4})" -f ($i + 1), $total, $dayStr, $exitCode, $elapsedText)
        if ($StopOnError) {
            Write-RunLine -GlobalLog $globalLog -Message "aborting due to StopOnError=true"
            exit $exitCode
        }
    }

    if ($SleepSeconds -gt 0 -and $i -lt ($total - 1)) {
        Start-Sleep -Seconds $SleepSeconds
    }
}

if (-not $NoChecks) {
    Write-RunLine -GlobalLog $globalLog -Message "final validate_marts dry-run start ..."
    $finalLog = Join-Path $runDir "_final_validate_marts.log"
    $finalOut = Join-Path $runDir "_final_validate_marts.stdout.tmp"
    $finalErr = Join-Path $runDir "_final_validate_marts.stderr.tmp"
    $finalArgs = @("-m", "etl.validate_marts", "--apply-minimal", "--dry-run", "--engine", $Engine)

    $finalExit = Invoke-LoggedProcess `
        -Executable $pythonCmd.Source `
        -Arguments $finalArgs `
        -WorkingDirectory $repoRoot `
        -LogFile $finalLog `
        -GlobalLog $globalLog `
        -StdoutTmp $finalOut `
        -StderrTmp $finalErr

    if ($finalExit -ne 0) {
        Write-RunLine -GlobalLog $globalLog -Message ("final validate_marts dry-run FAIL exit={0}" -f $finalExit)
        if ($StopOnError) {
            exit $finalExit
        }
        $failedDates.Add("_final_validate_marts") | Out-Null
    }
    else {
        Write-RunLine -GlobalLog $globalLog -Message "final validate_marts dry-run OK"
    }

    if ($SmokeBaseUrl) {
        $smokeScript = Join-Path $repoRoot "scripts\\smoke.ps1"
        if (Test-Path $smokeScript) {
            Write-RunLine -GlobalLog $globalLog -Message ("final smoke start | base_url={0}" -f $SmokeBaseUrl)
            $smokeLog = Join-Path $runDir "_final_smoke.log"
            $smokeOut = Join-Path $runDir "_final_smoke.stdout.tmp"
            $smokeErr = Join-Path $runDir "_final_smoke.stderr.tmp"

            $smokeArgs = @(
                "-ExecutionPolicy", "Bypass",
                "-File", $smokeScript,
                "-BaseUrl", $SmokeBaseUrl,
                "-From", $fromDate.ToString("yyyy-MM-dd"),
                "-To", $toDate.ToString("yyyy-MM-dd")
            )

            $smokeExit = Invoke-LoggedProcess `
                -Executable "powershell" `
                -Arguments $smokeArgs `
                -WorkingDirectory $repoRoot `
                -LogFile $smokeLog `
                -GlobalLog $globalLog `
                -StdoutTmp $smokeOut `
                -StderrTmp $smokeErr

            if ($smokeExit -ne 0) {
                Write-RunLine -GlobalLog $globalLog -Message ("final smoke FAIL exit={0}" -f $smokeExit)
                if ($StopOnError) {
                    exit $smokeExit
                }
                $failedDates.Add("_final_smoke") | Out-Null
            }
            else {
                Write-RunLine -GlobalLog $globalLog -Message "final smoke OK"
            }
        }
    }
    else {
        Write-RunLine -GlobalLog $globalLog -Message "final smoke skipped (SmokeBaseUrl not provided)"
    }
}
else {
    Write-RunLine -GlobalLog $globalLog -Message "final validate/checks skipped by -NoChecks"
}

$totalElapsed = (Get-Date) - $runStart
$durationText = $totalElapsed.ToString("hh\:mm\:ss")
foreach ($kv in $durations.GetEnumerator()) {
    Write-RunLine -GlobalLog $globalLog -Message ("day_duration | {0}={1}" -f $kv.Key, $kv.Value)
}

$failedCount = $failedDates.Count
$summary = "run_range done | ok={0} | fail={1} | duration={2}" -f ($total - $failedCount), $failedCount, $durationText
Write-RunLine -GlobalLog $globalLog -Message $summary
Write-RunLine -GlobalLog $globalLog -Message ("logs | global={0} | per_day_dir={1}" -f $globalLog, $runDir)

if ($failedCount -gt 0) {
    Write-RunLine -GlobalLog $globalLog -Message ("failed_dates={0}" -f ($failedDates -join ","))
    exit 1
}

exit 0
