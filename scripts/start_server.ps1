param(
  [int]$Port = 19720
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
python -m qmt_simulator.server --host 0.0.0.0 --port $Port

