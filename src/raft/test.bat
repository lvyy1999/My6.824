@echo off
set COUNT=0
set INTERVAL=3
set MAX_COUNT=10
set LOG_FILE=raft.log

:LOOP_START
set /a COUNT+=1
echo [%TIME%] start test %COUNT%/%MAX_COUNT% >> "%LOG_FILE%" 2>&1
go test  >> "%LOG_FILE%" 2>&1

if %COUNT% lss %MAX_COUNT% (
    ping -n %INTERVAL% 127.0.0.1 > nul
    goto LOOP_START
)
