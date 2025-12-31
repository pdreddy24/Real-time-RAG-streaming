# tools/smoke_test.ps1
# Purpose: Push 1 schema-valid message into wikimedia.cleaned, verify indexer counters increment,
#          verify Prometheus target is UP, and verify Prometheus ingests the metric.
# Run: powershell -ExecutionPolicy Bypass -File .\tools\smoke_test.ps1

$ErrorActionPreference = "Stop"

Write-Host "`n=== Smoke Test: Kafka -> Indexer -> Metrics -> Prometheus ===`n"

# -------- Config (override via env if you want) --------
$Topic          = $env:KAFKA_TOPIC        ; if (-not $Topic) { $Topic = "wikimedia.cleaned" }
$Group          = $env:KAFKA_GROUP        ; if (-not $Group) { $Group = "rag-indexer-v3" }
$MetricsUrl     = $env:INDEXER_METRICS    ; if (-not $MetricsUrl) { $MetricsUrl = "http://localhost:9100/metrics" }
$PromTargetsApi = $env:PROM_TARGETS_API   ; if (-not $PromTargetsApi) { $PromTargetsApi = "http://localhost:9090/api/v1/targets" }
$PromQueryApi   = $env:PROM_QUERY_API     ; if (-not $PromQueryApi) { $PromQueryApi = "http://localhost:9090/api/v1/query" }

$MaxWaitSeconds = 45
$PollSeconds    = 2

# -------- Helpers --------
function Require-Command {
  param([string]$Cmd)
  $ok = Get-Command $Cmd -ErrorAction SilentlyContinue
  if (-not $ok) { throw "FAIL: Required command not found: $Cmd" }
}

function Get-MetricsText {
  param([Parameter(Mandatory=$true)][string]$Url)
  # External commands return string[] (one per line). Join to a single string.
  return (@((curl.exe -s $Url)) -join "`n")
}

function Get-CounterValue {
  param(
    [Parameter(Mandatory=$true)][string]$MetricsText,
    [Parameter(Mandatory=$true)][string]$MetricName
  )
  $pattern = "(?m)^\s*$([regex]::Escape($MetricName))\s+([0-9]+(\.[0-9]+)?)\s*$"
  $m = [regex]::Match($MetricsText, $pattern)
  if (-not $m.Success) { return $null }
  return [double]$m.Groups[2].Value
}

function Get-MetricLine {
  param([string]$MetricsText, [string]$MetricName)
  return ($MetricsText -split "`n" | Where-Object { $_ -match "^\s*$([regex]::Escape($MetricName))\s+" } | Select-Object -First 1)
}

# -------- Preflight --------
Require-Command "docker"
Require-Command "curl.exe"

Write-Host "Step 0) Containers status (quick view)"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" |
  Select-String -Pattern "wikimedia-redpanda|wikimedia-rag-indexer|prometheus|wikimedia-producer" |
  ForEach-Object { $_.Line }
Write-Host ""

# -------- Step 1) Baseline counters --------
Write-Host "Step 1) Read baseline indexer counters"
$metricsBefore = Get-MetricsText -Url $MetricsUrl

$beforeMsg  = Get-CounterValue -MetricsText $metricsBefore -MetricName "rag_indexer_messages_total"
$beforeProc = Get-CounterValue -MetricsText $metricsBefore -MetricName "rag_indexer_processed_total"
$beforeFail = Get-CounterValue -MetricsText $metricsBefore -MetricName "rag_indexer_failures_total"

if ($null -eq $beforeMsg -or $null -eq $beforeProc -or $null -eq $beforeFail) {
  Write-Host "DEBUG: metrics line messages:  $(Get-MetricLine $metricsBefore 'rag_indexer_messages_total')"
  Write-Host "DEBUG: metrics line processed: $(Get-MetricLine $metricsBefore 'rag_indexer_processed_total')"
  Write-Host "DEBUG: metrics line failures:  $(Get-MetricLine $metricsBefore 'rag_indexer_failures_total')"
  throw "FAIL: Could not parse counters from $MetricsUrl."
}

Write-Host ("IDX(before): messages={0} processed={1} failures={2}" -f $beforeMsg, $beforeProc, $beforeFail)
Write-Host ""

# -------- Step 2) Produce a schema-valid message --------
Write-Host "Step 2) Produce 1 SCHEMA-VALID message into topic: $Topic"

$eid = [guid]::NewGuid().ToString()
$payload = @{
  source    = "smoke-test"
  event_id  = $eid
  title     = "smoke-test"
  url       = "https://www.wikidata.org/wiki/Q42"
  diff_url  = "https://www.wikidata.org/wiki/Q42"
  wiki      = "wikidatawiki"
  domain    = "www.wikidata.org"
  type      = "edit"
  namespace = 0
  timestamp = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
  dt        = (Get-Date).ToUniversalTime().ToString("o")
  user      = "smoke-test"
  bot       = $false
  minor     = $false
  comment   = "smoke test produce"
  content   = "Title: smoke-test`nType: edit`nWiki: wikidatawiki (www.wikidata.org)`nComment: smoke test produce`nDiff: https://www.wikidata.org/wiki/Q42"
}

$payloadJson = $payload | ConvertTo-Json -Compress
$payloadJson | docker exec -i wikimedia-redpanda rpk topic produce $Topic -k $eid | Out-Host
Write-Host ""

# -------- Step 3) Consume the LAST message (prove we saw OUR smoke message) --------
Write-Host "Step 3) Consume LAST message from topic (should show source=smoke-test)"
docker exec -it wikimedia-redpanda rpk topic consume $Topic -n 1 --offset -1 | Out-Host
Write-Host ""

# -------- Step 4) Consumer group describe --------
Write-Host "Step 4) Consumer group describe: $Group"
docker exec -it wikimedia-redpanda rpk group describe $Group | Out-Host
Write-Host ""

# -------- Step 5) Wait for counters to increment --------
Write-Host "Step 5) Wait for indexer counters to increment (max ${MaxWaitSeconds}s)"
$deadline = (Get-Date).AddSeconds($MaxWaitSeconds)

$msgIncreased  = $false
$procIncreased = $false

do {
  Start-Sleep -Seconds $PollSeconds

  $metricsNow = Get-MetricsText -Url $MetricsUrl
  $nowMsg  = Get-CounterValue -MetricsText $metricsNow -MetricName "rag_indexer_messages_total"
  $nowProc = Get-CounterValue -MetricsText $metricsNow -MetricName "rag_indexer_processed_total"
  $nowFail = Get-CounterValue -MetricsText $metricsNow -MetricName "rag_indexer_failures_total"

  if ($null -eq $nowMsg -or $null -eq $nowProc -or $null -eq $nowFail) {
    throw "FAIL: Could not parse counters during polling from $MetricsUrl."
  }

  Write-Host ("IDX(now): messages={0} processed={1} failures={2}" -f $nowMsg, $nowProc, $nowFail)

  $msgIncreased  = ($nowMsg  -gt $beforeMsg)
  $procIncreased = ($nowProc -gt $beforeProc)

} while ((Get-Date) -lt $deadline -and -not ($msgIncreased -and $procIncreased))

if (-not ($msgIncreased -and $procIncreased)) {
  Write-Host "DEBUG: indexer may be SKIPPING the produced message. Check logs:"
  Write-Host "       docker logs --tail 200 wikimedia-rag-indexer"
  throw "FAIL: Indexer counters did not increment within ${MaxWaitSeconds}s."
}

Write-Host ("PASS: Indexer processed message (messages {0}->{1}, processed {2}->{3})" -f $beforeMsg, $nowMsg, $beforeProc, $nowProc)
Write-Host ""

# -------- Step 6) Prometheus target health check --------
Write-Host "Step 6) Validate Prometheus target 'rag-indexer' is UP"
$targets = Invoke-RestMethod $PromTargetsApi
$target = $targets.data.activeTargets | Where-Object { $_.labels.job -eq "rag-indexer" }

if (-not $target) { throw "FAIL: rag-indexer target missing in Prometheus" }
if ($target.health -ne "up") { throw "FAIL: rag-indexer target health=$($target.health) lastError=$($target.lastError)" }

Write-Host "PROM(target): job=$($target.labels.job) instance=$($target.labels.instance) health=$($target.health) lastScrape=$($target.lastScrape)"
Write-Host ""

# -------- Step 7) Prometheus query (proof scrape + ingest) --------
Write-Host "Step 7) Prometheus query (proof ingest): rag_indexer_processed_total"
$q = [uri]::EscapeDataString("rag_indexer_processed_total")
$resp = Invoke-RestMethod ("$PromQueryApi?query=$q")

if ($resp.status -ne "success" -or -not $resp.data.result -or $resp.data.result.Count -lt 1) {
  throw "FAIL: Prometheus has no data for rag_indexer_processed_total (target may be UP but not ingesting / no samples yet)."
}

$sample = $resp.data.result[0].value[1]
Write-Host "PROM(query): rag_indexer_processed_total=$sample"
Write-Host ""

Write-Host "=== Done: ALL CHECKS PASSED ===`n" -ForegroundColor Green
exit 0
