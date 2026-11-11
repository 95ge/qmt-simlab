@echo off
cd /d "%~dp0.."
python -m qmt_simulator.server %*

