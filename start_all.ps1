param(
    [string]$PythonExe = "",
    [switch]$SkipNpmInstall
)

$ErrorActionPreference = "Stop"

function Resolve-PythonExecutable {
    param(
        [string]$PreferredPython
    )

    if ($PreferredPython) {
        if (Test-Path -LiteralPath $PreferredPython) {
            return (Resolve-Path -LiteralPath $PreferredPython).Path
        }
        throw "The provided Python executable was not found: $PreferredPython"
    }

    $knownCandidates = @(
        "C:\Users\willi\AppData\Local\Programs\Python\Python311\python.exe",
        "C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.11_3.11.2544.0_x64__qbz5n2kfra8p0\python3.11.exe"
    )

    foreach ($candidate in $knownCandidates) {
        if (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }

    throw "Could not find a Python executable. Pass -PythonExe with the full path to python.exe."
}

function Start-ServiceWindow {
    param(
        [string]$Title,
        [string]$Command
    )

    Start-Process powershell -ArgumentList @(
        "-NoExit",
        "-Command",
        "& { $host.UI.RawUI.WindowTitle = '$Title'; $Command }"
    )
}

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$webRoot = Join-Path $repoRoot "apps_sdk\web"
$pythonPath = Resolve-PythonExecutable -PreferredPython $PythonExe
$npmInstallCommand = if ($SkipNpmInstall) { "" } else { "npm install; " }

$backendCommand = @"
Set-Location -LiteralPath '$repoRoot';
`$env:PYTHONPATH='.';
`$env:AWS_DEFAULT_REGION='us-west-2';
`$env:AWS_ACCESS_KEY_ID='$env:AWS_ACCESS_KEY_ID';
`$env:AWS_SECRET_ACCESS_KEY='$env:AWS_SECRET_ACCESS_KEY';
`$env:AWS_SESSION_TOKEN='$env:AWS_SESSION_TOKEN';
& '$pythonPath' -m uvicorn app.main:app --reload --port 8000
"@

$frontendCommand = @"
Set-Location -LiteralPath '$webRoot';
$npmInstallCommand`$env:BROWSER='default';
npm run dev
"@

$mcpCommand = @"
Set-Location -LiteralPath '$webRoot';
$npmInstallCommand npm run build;
Set-Location -LiteralPath '$repoRoot';
`$env:PYTHONPATH='.';
`$env:AWS_DEFAULT_REGION='us-west-2';
`$env:AWS_ACCESS_KEY_ID='$env:AWS_ACCESS_KEY_ID';
`$env:AWS_SECRET_ACCESS_KEY='$env:AWS_SECRET_ACCESS_KEY';
`$env:AWS_SESSION_TOKEN='$env:AWS_SESSION_TOKEN';
& '$pythonPath' -m uvicorn apps_sdk.server.main:app --reload --port 8001
"@

Write-Host "Starting backend, frontend, and MCP server..."
Write-Host "Repo root: $repoRoot"
Write-Host "Python: $pythonPath"

Start-ServiceWindow -Title "Datathon Backend" -Command $backendCommand
Start-ServiceWindow -Title "Datathon Frontend" -Command $frontendCommand
Start-ServiceWindow -Title "Datathon MCP Server" -Command $mcpCommand

Write-Host "Started three windows:"
Write-Host "  - Backend:  http://127.0.0.1:8000"
Write-Host "  - Frontend: Vite will print the browser URL, usually http://127.0.0.1:5173"
Write-Host "  - MCP:      http://127.0.0.1:8001/mcp"
