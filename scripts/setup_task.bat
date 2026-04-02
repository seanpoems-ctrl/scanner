@echo off
:: Delete the old task first so we can start fresh
schtasks /delete /tn "Dashboard_Pusher" /f >nul 2>&1

:: Create the new 24/7 task (runs every 15 minutes, every day)
schtasks /create /tn "Dashboard_Pusher" /tr "'C:\Users\arabi\.cursor\Project 3\scripts\push_static.bat'" /sc minute /mo 15 /rl HIGHEST /f

echo.
echo === TASK CREATED SUCCESSFULLY ===
pause
