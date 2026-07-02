@echo off
REM Resilient IMF ingest runner (Windows, detached). Re-launches the streamed
REM full pull with --skip-existing until the log reports DONE or attempts run out.
REM Each restart resumes by skipping already-written parquet (incl. chunk parts).
setlocal enabledelayedexpansion
cd /d D:\research\econfindatalibrary
set LOG=D:\research\econfindatalibrary\data\ingest_imf_full.log
for /L %%i in (1,1,30) do (
  findstr /C:"DONE in" "%LOG%" >nul 2>&1
  if !errorlevel! EQU 0 (
    echo [runner] DONE detected at attempt %%i >> "%LOG%"
    goto :done
  )
  echo [runner] === attempt %%i/30 %DATE% %TIME% === >> "%LOG%"
  del /Q D:\research\econfindatalibrary\data\tmp*.csv.gz >nul 2>&1
  del /Q D:\research\econfindatalibrary\data\clean_full\imf\*.tmp >nul 2>&1
  set PYTHONIOENCODING=utf-8
  python jobs\ingest_imf_full.py --skip-existing >> "%LOG%" 2>&1
  echo [runner] attempt %%i exited rc=!errorlevel! >> "%LOG%"
  findstr /C:"DONE in" "%LOG%" >nul 2>&1
  if !errorlevel! EQU 0 goto :done
  timeout /t 6 /nobreak >nul 2>&1
)
:done
echo [runner] FINISHED runner loop >> "%LOG%"
endlocal
