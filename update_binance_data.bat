@echo off
REM Activate the Conda environment
call conda activate crypto

REM Run the Python script
python binance_api.py

REM Deactivate the Conda environment
call conda deactivate

