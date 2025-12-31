param(
  [string]$DbContainer = "wikimedia-postgres",
  [string]$DbUser = "postgres",
  [string]$DbName = "postgres",
  [string]$Table = "document_chunks",
  [string]$TimeCol = "created_at"
)

$ErrorActionPreference = "Stop"

function Psql([string]$sql) {
  docker exec -i $DbContainer psql -U $DbUser -d $DbName -v ON_ERROR_STOP=1 -c $sql
}

Write-Host "`n=== DB Proof: $DbContainer / $DbName ===" -ForegroundColor Cyan

Write-Host "`n1) Rowcount snapshot" -ForegroundColor Yellow
Psql "SELECT '$Table' AS table, COUNT(*)::bigint AS row_count FROM public.$Table;"

Write-Host "`n2) Top tables by estimated rowcount" -ForegroundColor Yellow
Psql "SELECT schemaname, relname, n_live_tup AS est_rows FROM pg_stat_user_tables ORDER BY n_live_tup DESC LIMIT 15;"

Write-Host "`n3) Largest tables by size" -ForegroundColor Yellow
Psql "SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS total_size FROM pg_catalog.pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 15;"

Write-Host "`n4) Index inventory (public schema)" -ForegroundColor Yellow
Psql "SELECT schemaname, tablename, indexname FROM pg_indexes WHERE schemaname='public' ORDER BY tablename, indexname;"

Write-Host "`n5) Retention sanity (latest rows)" -ForegroundColor Yellow
# If your schema doesn't have created_at, switch TimeCol to dt (you already have document_chunks_dt_idx)
try {
  Psql "SELECT * FROM public.$Table ORDER BY $TimeCol DESC NULLS LAST LIMIT 10;"
} catch {
  Write-Host "TimeCol '$TimeCol' failed. Try: -TimeCol dt" -ForegroundColor Yellow
}

Write-Host "`n=== DONE ===`n" -ForegroundColor Green
