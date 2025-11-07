@echo off
set COUNT=0
set INTERVAL=2
set LOG_FILE=cmd.log

:LOOP_START
set /a COUNT+=1
echo [%DATE% %TIME%] start test %COUNT%/1000 >> "%LOG_FILE%"
go test -run 2A  >> "%LOG_FILE%" 2>&1

if %COUNT% lss 1000 (
    ping -n %INTERVAL% 127.0.0.1 > nul
    goto LOOP_START
)
