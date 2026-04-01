@echo off
REM ─────────────────────────────────────────────────────────────────────────
REM  setup_task.bat
REM  Run this ONCE (as Administrator) to register the Windows Task Scheduler
REM  job.  After that, the pusher runs automatically every 15 minutes on
REM  weekdays between 07:30 and 18:15 ET.
REM
REM  Usage:  Right-click → "Run as administrator"
REM ─────────────────────────────────────────────────────────────────────────

SET TASK_NAME=PowerTheme_LocalPusher
SET BAT_PATH=c:\Users\arabi\.cursor\Project 3\scripts\run_pusher.bat

REM Delete any existing task with the same name (safe to run repeatedly).
schtasks /delete /tn "%TASK_NAME%" /f 2>nul

REM Create the task:
REM   /SC MINUTE /MO 15   → every 15 minutes
REM   /ST 07:30           → start at 07:30
REM   /ET 18:15           → stop at 18:15
REM   /D MON,TUE,WED,THU,FRI  → weekdays only
REM   /RL HIGHEST         → run with highest privileges (needed for some Python envs)
schtasks /create ^
  /tn "%TASK_NAME%" ^
  /tr "\"%BAT_PATH%\"" ^
  /sc MINUTE ^
  /mo 15 ^
  /st 07:30 ^
  /et 18:15 ^
  /d MON,TUE,WED,THU,FRI ^
  /rl HIGHEST ^
  /f

IF %ERRORLEVEL% EQU 0 (
    echo.
    echo  Task "%TASK_NAME%" created successfully.
    echo  It will run every 15 minutes, Mon-Fri, 07:30-18:15.
    echo.
    echo  To run it manually right now:
    echo    schtasks /run /tn "%TASK_NAME%"
    echo.
    echo  To delete it later:
    echo    schtasks /delete /tn "%TASK_NAME%" /f
) ELSE (
    echo.
    echo  ERROR: Task creation failed. Make sure you ran this as Administrator.
)

pause
