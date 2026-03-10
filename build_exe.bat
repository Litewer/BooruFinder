@echo off
setlocal

set APP_NAME=BooruFinder

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

pyinstaller ^
  --noconfirm ^
  --onefile ^
  --windowed ^
  --name %APP_NAME% ^
  --add-data "static;static" ^
  desktop.py

echo.
echo Build complete: dist\%APP_NAME%.exe
pause
