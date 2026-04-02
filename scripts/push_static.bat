@echo off
SET PROJECT_ROOT=%~dp0..
cd /d "%PROJECT_ROOT%"
echo Running local_pusher_static.py...
py backend\local_pusher_static.py
