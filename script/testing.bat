@echo off
setlocal EnableDelayedExpansion

set HOST=http://localhost:8080/query

echo ===== CREATE TABLE =====
set "QUERY=CREATE TABLE test (id INT 2, name STRING 1)"
call :request "%QUERY%"

echo.
echo ===== INSERT =====

for /L %%i in (0,1,9999999) do (

    set /A mod=%%i %% 10000
    if !mod! EQU 0 echo Insert: %%i

    for /f %%u in ('powershell -NoProfile -Command "[guid]::NewGuid().ToString()"') do (
        set "UUID=%%u"
    )

    set "QUERY=INSERT INTO test (id, name) VALUES (int(%%i), string(!UUID!))"
    call :request "%QUERY%"
)

echo.
echo ===== SELECT =====
set "QUERY=SELECT * FROM test WHERE id < int(99960) AND id >= int(99950)"
call :request "%QUERY%"

goto :eof

:request
:request
echo Sending:
echo {"query":"%~1"}

curl -v ^
  -X POST ^
  -H "Content-Type: application/json" ^
  --data "{\"query\":\"%~1\"}" ^
  %HOST%

echo.
exit /b

echo.
exit /b