@echo off
setlocal

set SDK_DIR=C:\Android\Sdk
set GRADLE_BIN=C:\Tools\gradle-8.7\bin\gradle.bat
set APP_DIR=%~dp0android_app

if not exist "%GRADLE_BIN%" (
  echo Gradle not found: %GRADLE_BIN%
  exit /b 1
)

if not exist "%SDK_DIR%" (
  echo Android SDK not found: %SDK_DIR%
  exit /b 1
)

set ANDROID_HOME=%SDK_DIR%
set ANDROID_SDK_ROOT=%SDK_DIR%

echo sdk.dir=%SDK_DIR:\=\\%> "%APP_DIR%\local.properties"

copy /Y "%~dp0app.py" "%APP_DIR%\app\src\main\python\app.py" >nul
copy /Y "%~dp0static\index.html" "%APP_DIR%\app\src\main\python\static\index.html" >nul
copy /Y "%~dp0static\app.js" "%APP_DIR%\app\src\main\python\static\app.js" >nul
copy /Y "%~dp0static\app.css" "%APP_DIR%\app\src\main\python\static\app.css" >nul

pushd "%APP_DIR%"
call "%GRADLE_BIN%" assembleDebug
if errorlevel 1 (
  popd
  exit /b 1
)
popd

copy /Y "%APP_DIR%\app\build\outputs\apk\debug\app-debug.apk" "%~dp0dist\BooruFinder-android-debug.apk" >nul
echo.
echo Build complete: dist\BooruFinder-android-debug.apk
