@echo off
cd /d "%~dp0"
C:\Python314\python.exe scrape_vct.py >> "%~dp0scraper.log" 2>&1
