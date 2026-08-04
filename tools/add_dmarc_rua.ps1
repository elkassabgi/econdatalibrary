# Sets DMARC aggregate reporting (rua) for hfdatalibrary.com.
# Reports go to admin@hfdatalibrary.com, which ALREADY forwards to elkassabgi@gmail.com
# via Cloudflare Email Routing (verified) — so no new routing rule is needed.
# Safe + reversible: only touches the _dmarc TXT record. Policy stays p=none (report-only).
# Run:  powershell -ExecutionPolicy Bypass -File tools\add_dmarc_rua.ps1

$ErrorActionPreference = 'Stop'
$envFile = Join-Path $PSScriptRoot '..\.env'
$line = (Get-Content $envFile | Where-Object { $_ -match '^CF_ADMIN_TOKEN=' } | Select-Object -First 1)
if (-not $line) { throw 'CF_ADMIN_TOKEN not found in .env' }
$tok = $line -replace '^CF_ADMIN_TOKEN=', '' -replace '"', ''
$zone = '06f8dcf9bdfc425747cd158739c75c50'          # hfdatalibrary.com
$rec  = '653330090df20e2f21566f90fd292f10'          # _dmarc TXT record
$hdr  = @{ Authorization = "Bearer $tok"; 'Content-Type' = 'application/json' }

Write-Host 'Update _dmarc TXT -> v=DMARC1; p=none; rua=mailto:admin@hfdatalibrary.com; fo=1'
$dnsBody = '{"content":"v=DMARC1; p=none; rua=mailto:admin@hfdatalibrary.com; fo=1"}'
$r = Invoke-RestMethod -Method Patch -Uri "https://api.cloudflare.com/client/v4/zones/$zone/dns_records/$rec" -Headers $hdr -Body $dnsBody
Write-Host "dns updated: success=$($r.success)  content now: $($r.result.content)"
Write-Host ''
Write-Host 'Done. Aggregate DMARC reports (XML, ~daily, from google/yahoo/microsoft etc.) will arrive'
Write-Host 'at elkassabgi@gmail.com (via the existing admin@hfdatalibrary.com forward).'
