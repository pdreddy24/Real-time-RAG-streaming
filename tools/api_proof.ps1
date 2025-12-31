param(
  [string]$ApiUrl = "http://localhost:8000/query",
  [int]$TopK = 3,
  [int]$Rounds = 10
)

$ErrorActionPreference = "Stop"

function Assert($cond, $msg) { if (-not $cond) { throw "FAIL: $msg" } }

Write-Host "`n=== API Proof: $ApiUrl ===" -ForegroundColor Cyan

# Optional health check
try {
  $health = Invoke-RestMethod "http://localhost:8000/health" -TimeoutSec 5
  Write-Host "Health: $($health | ConvertTo-Json -Compress)" -ForegroundColor Green
} catch {
  Write-Host "Health endpoint not available (ok). Continuing..." -ForegroundColor Yellow
}

$queries = @(
  "recent wikimedia edit about sports",
  "recent wikimedia edit about politics",
  "recent wikimedia edit about anime",
  "recent wikimedia edit about India",
  "recent wikimedia edit about technology"
)

for ($i=1; $i -le $Rounds; $i++) {
  $q = $queries[($i-1) % $queries.Count]
  $body = @{ query = $q; top_k = $TopK } | ConvertTo-Json

  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  $resp = Invoke-RestMethod -Method Post -Uri $ApiUrl -ContentType "application/json" -Body $body -TimeoutSec 30
  $sw.Stop()

  Assert ($null -ne $resp.results) "No 'results' field in response"
  Assert ($resp.results.Count -ge 1) "Empty results for query: $q"

  $r0 = $resp.results[0]
  $hasText = ($r0.PSObject.Properties.Name -contains "content") -or
             ($r0.PSObject.Properties.Name -contains "text") -or
             ($r0.PSObject.Properties.Name -contains "url")
  Assert $hasText "Top result missing content/text/url"

  Write-Host ("PASS round {0}/{1}: {2}ms query='{3}' results={4}" -f $i,$Rounds,$sw.ElapsedMilliseconds,$q,$resp.results.Count) -ForegroundColor Green
}

Write-Host "`n=== DONE: API proof passed ===`n" -ForegroundColor Green
