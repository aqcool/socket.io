@echo OFF
setlocal ENABLEDELAYEDEXPANSION
pushd "%~dp0"

:: Generate ESC character safely for ANSI colors
for /F "tokens=1,2 delims=#" %%a in ('"prompt #$H#$E# & echo on & for %%b in (1) do rem"') do set "ESC=%%b"

:: Color Definitions
set "C_RESET=%ESC%[0m"
set "C_CYAN=%ESC%[36m"
set "C_GREEN=%ESC%[32m"
set "C_YELLOW=%ESC%[33m"
set "C_RED=%ESC%[31m"

:: Configuration
set "GOPROXY=https://proxy.golang.org,direct"
set "TEST_TIMEOUT=60s"

:: Check for Go installation
where go >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo %C_RED%[Fatal] Go is not installed or not in PATH.%C_RESET%
    exit /b 1
)

:: Router
if "%~1"=="" goto :help
if /I "%~1"=="help"    goto :help
if /I "%~1"=="env"     goto :cmd_env

if /I "%~1"=="deps" (
    call :RunCmd "go mod tidy" "Deps 1/3"
    if exist "go.work" (
        call :RunCmd "go work sync" "Deps 2/3"
        call :RunCmd "go work vendor" "Deps 3/3"
    ) else (
        call :RunCmd "go mod vendor" "Deps 2/2"
    )
    goto :finalize
)

if /I "%~1"=="get" (
    call :RunCmd "go get ./..." "Get"
    goto :finalize
)

if /I "%~1"=="build" (
    call :RunCmd "go build ./..." "Build"
    goto :finalize
)

if /I "%~1"=="fmt" (
    call :RunCmd "go fmt ./..." "Fmt"
    goto :finalize
)

if /I "%~1"=="clean" (
    call :RunCmd "go clean -mod=mod -v -r ./..." "Clean"
    goto :finalize
)

if /I "%~1"=="update" (
    call :RunCmd "go get -u -v ./..." "Update"
    if !ERRORLEVEL! EQU 0 (
        call :RunCmd "go mod tidy" "Deps 1/3"
        if exist "go.work" (
            call :RunCmd "go work sync" "Deps 2/3"
            call :RunCmd "go work vendor" "Deps 3/3"
        ) else (
            call :RunCmd "go mod vendor" "Deps 2/2"
        )
    )
    goto :finalize
)

if /I "%~1"=="vet" (
    call :RunCmd "go mod tidy" "Deps 1/2"
    if !ERRORLEVEL! EQU 0 call :RunCmd "go vet ./..." "Vet"
    goto :finalize
)

if /I "%~1"=="lint" (
    where golangci-lint >nul 2>nul
    if !ERRORLEVEL! NEQ 0 (
        echo %C_RED%[Error] golangci-lint is not installed.%C_RESET%
        exit /b 1
    )
    set "LINT_FIX="
    if /I "%~2"=="--fix" set "LINT_FIX=--fix"
    call :RunCmd "go mod tidy" "Deps 1/2"
    if !ERRORLEVEL! EQU 0 call :RunCmd "golangci-lint run !LINT_FIX! ./... <nul" "Lint"
    goto :finalize
)

if /I "%~1"=="test" (
    call :RunCmd "go mod tidy" "Deps 1/2"
    echo %C_CYAN%[Test] Cleaning test cache...%C_RESET%
    go clean -testcache
    call :RunCmd "go test -timeout=%TEST_TIMEOUT% -race -cover -covermode=atomic ./... <nul" "Test"
    goto :finalize
)

echo %C_RED%[Error] Unknown command: %~1%C_RESET%
goto :help

:finalize
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%
exit /b 0

:: :RunCmd [Command] [Label]
:RunCmd
    set "CMD=%~1"
    set "LABEL=%~2"
    echo %C_CYAN%[%LABEL%] Processing: .%C_RESET%
    call %CMD%
    if !ERRORLEVEL! NEQ 0 (
        echo %C_RED%[%LABEL%] Failed.^ (Exit Code: !ERRORLEVEL!^)%C_RESET%
        exit /b !ERRORLEVEL!
    )
    exit /b 0

:cmd_env
    go env
    exit /b 0

:help
    echo.
    echo %C_YELLOW%Usage: make.bat [command] [options]%C_RESET%
    echo.
    echo %C_CYAN%Standard Commands:%C_RESET%
    echo    deps        Run 'go mod tidy', 'go work sync' ^& 'go work vendor'
    echo    get         Run 'go get ./...'
    echo    build       Run 'go build ./...'
    echo    fmt         Run 'go fmt ./...'
    echo    clean       Run 'go clean' (recursive)
    echo    test        Run tests with race detection and coverage
    echo.
    echo %C_CYAN%Composite Commands:%C_RESET%
    echo    update      Update all dependencies (-u) and refresh deps
    echo    vet         Run 'vet' after tidying modules
    echo    lint        Run golangci-lint (add --fix to auto-fix)
    echo.
    exit /b 0
