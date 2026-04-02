@echo off
cd /d "%~dp0"
"%~dp0.venv\Scripts\python.exe" scrape_vct.py >> "%~dp0scraper.log" 2>&1
