cd "$(dirname "$0")"
echo off

echo "Setup Python Virtual Environment"
echo "(this might take a minute the first time)"
mkdir log
python3 -m venv ./env
source ./env/bin/activate
python -m pip uninstall tdcoa -y
export TDCOA_LOGFILE=./log/$(date +%Y%m%d-%H%M%S).txt
export TDCOA_NOVERIFY=1

echo Install tdCOA
python -m pip install -f wheel --upgrade tdcoa

echo Launching Application
tdcoaw &
pause
