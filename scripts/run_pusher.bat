@echo off
REM ─────────────────────────────────────────────────────────────────────────
REM  run_pusher.bat
REM  Runs local_pusher.py using the project's Python environment.
REM  Schedule this file with Windows Task Scheduler (see setup_task.bat).
REM ─────────────────────────────────────────────────────────────────────────

SET PROJECT_ROOT=c:\Users\arabi\.cursor\Project 3
SET PYTHON=python

REM Change to project root so relative imports work correctly.
cd /d "%PROJECT_ROOT%"

REM Run the pusher.  stdout+stderr go to pusher.log (handled inside the script).
%PYTHON% backend\local_pusher.py

exit /b %ERRORLEVEL%
