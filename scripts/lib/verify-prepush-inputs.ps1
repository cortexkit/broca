param([Parameter(Mandatory=$true)][string]$InputsDirectory)
$ErrorActionPreference = 'Stop'
function Refuse([string]$Name, [string]$Reason) {
    [Console]::Error.WriteLine("prepush: NOT-RUN windows: offline input $Name $Reason")
    exit 200
}
$manifestPath = Join-Path $InputsDirectory 'transport-manifest.json'
if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    Refuse 'transport-manifest.json' 'is missing on the runner'
}
try { $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json }
catch { Refuse 'transport-manifest.json' 'is not valid JSON' }
if ($manifest.version -ne 1 -or @($manifest.files).Count -eq 0) {
    Refuse 'transport-manifest.json' 'has no supported file inventory'
}
foreach ($file in $manifest.files) {
    $name = [string]$file.name
    if ($name -notmatch '^[A-Za-z0-9._-]+$' -or $name -eq '.' -or $name -eq '..') {
        Refuse $name 'has an unsafe name'
    }
    $path = Join-Path $InputsDirectory $name
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { Refuse $name 'is missing on the runner' }
    try {
        $length = (Get-Item -LiteralPath $path).Length
        $actual = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
    } catch { Refuse $name 'cannot be read on the runner' }
    if ($length -ne [long]$file.size) { Refuse $name 'has a size mismatch' }
    if ($actual -ne ([string]$file.sha256).ToLowerInvariant()) { Refuse $name 'has a SHA256 mismatch' }
}
Write-Output "prepush: verified $(@($manifest.files).Count) offline input files on Windows"

function CheckTool([string]$Name, [string]$Executable, [string[]]$Arguments, [string]$Pattern) {
    try {
        $output = (& $Executable @Arguments 2>&1 | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or $output -notmatch $Pattern) { Refuse $Name "has an unsupported version: $output" }
    } catch { Refuse $Name 'is missing or could not report its version' }
    Write-Output "prepush: tool $Name $output"
}
CheckTool 'Python' 'python.exe' @('--version') '^Python 3\.14\.'
CheckTool 'Node' 'node.exe' @('--version') '^v24\.'
CheckTool 'npm' 'npm.cmd' @('--version') '^\d+\.\d+\.\d+'
CheckTool 'nats-server' 'nats-server.exe' @('--version') '^nats-server: v2\.15\.0$'
CheckTool 'CMake' 'cmake.exe' @('--version') '^cmake version \d+\.\d+'
CheckTool 'Ninja' 'ninja.exe' @('--version') '^\d+\.\d+'
CheckTool 'Bun' 'bun.exe' @('--version') '^1\.4\.4$'
