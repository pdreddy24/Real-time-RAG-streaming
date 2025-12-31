# tools/db_migrate.ps1
param(
  [string]$Db = "postgres",
  [string]$User = "postgres",
  [string]$Container = "wikimedia-postgres",
  [string]$MigrationsDir = ".\migrations"
)

$ErrorActionPreference = "Stop"

$files = Get-ChildItem $MigrationsDir -Filter "*.sql" | Sort-Object Name
if ($files.Count -eq 0) { throw "No migrations found in $MigrationsDir" }

foreach ($f in $files) {
  Write-Host "Applying $($f.Name)..."
  docker exec -i $Container psql -U $User -d $Db -v ON_ERROR_STOP=1 -f - < $f.FullName | Out-Host
}
Write-Host "PASS: migrations applied."
