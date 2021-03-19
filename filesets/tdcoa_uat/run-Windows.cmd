cd %~dp0
echo off

echo Setup Python Virtual Environment
echo (this might take a minute the first time)
mkdir log
python -m venv ./env
source ./env/bin/activate
python -m pip uninstall tdcoa -y
set TDCOA_LOGFILE=./log/%date:/=%.txt


echo Install tdCOA
python -m pip install -f wheel --upgrade tdcoa

echo Launching Application
tdcoaw &
pause
