@echo off
cd /d "%~dp0.."
python -m qmt_simulator.gateway.gateway_main %*

