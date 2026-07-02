@echo off
REM Resilient ECB ingest runner: the host periodically OOM-kills heavy ingest jobs.
REM ingest_ecb.py is restart-safe (resume skips finished whole flows; giants resume
REM per-chunk). Re-launch until it exits 0 (a clean DONE), capping the attempts.
setlocal
set ROOT=D:\research\econfindatalibrary
set PY=python
set LOG=%ROOT%\data\clean_full\ecb_full.log
set ATTEMPT=0
:loop
set /a ATTEMPT+=1
echo ============================================================ >> "%LOG%"
echo [resilient] attempt %ATTEMPT% starting at %DATE% %TIME% >> "%LOG%"
%PY% "%ROOT%\jobs\ingest_ecb.py" --workers 3 >> "%LOG%" 2>&1
set RC=%ERRORLEVEL%
echo [resilient] attempt %ATTEMPT% exited rc=%RC% at %DATE% %TIME% >> "%LOG%"
if "%RC%"=="0" goto done
if %ATTEMPT% GEQ 20 goto giveup
goto loop
:done
echo [resilient] COMPLETED cleanly after %ATTEMPT% attempt(s) >> "%LOG%"
exit /b 0
:giveup
echo [resilient] GAVE UP after %ATTEMPT% attempts (last rc=%RC%) >> "%LOG%"
exit /b 1
