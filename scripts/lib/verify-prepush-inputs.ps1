param(
    [Parameter(Mandatory=$true)][string]$InputsDirectory,
    [Parameter(Mandatory=$true)][string]$ToolsDirectory,
    [Parameter(Mandatory=$true)][string]$PathFile
)
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
$listed = @{}
foreach ($file in $manifest.files) {
    $name = [string]$file.name
    if ($name -notmatch '^[A-Za-z0-9._-]+$' -or $name -eq '.' -or $name -eq '..') {
        Refuse $name 'has an unsafe name'
    }
    if ($name -eq 'transport-manifest.json' -or $listed.ContainsKey($name)) {
        Refuse $name 'has a duplicate or reserved manifest entry'
    }
    $listed[$name] = $true
    $size = 0L
    if (-not [long]::TryParse([string]$file.size, [ref]$size) -or $size -lt 0 -or
        [string]$file.sha256 -notmatch '^[a-fA-F0-9]{64}$') {
        Refuse $name 'has invalid size or SHA256 metadata'
    }
    $path = Join-Path $InputsDirectory $name
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { Refuse $name 'is missing on the runner' }
    try {
        $length = (Get-Item -LiteralPath $path).Length
        $actual = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
    } catch { Refuse $name 'cannot be read on the runner' }
    if ($length -ne $size) { Refuse $name 'has a size mismatch' }
    if ($actual -ne ([string]$file.sha256).ToLowerInvariant()) { Refuse $name 'has a SHA256 mismatch' }
}
foreach ($name in @('manifest.json', 'subconscious-source.bundle', 'claustrum-source.tar',
                    'fixture-cargo-sources.tar.gz', 'fixture-cargo-subconscious.toml', 'fixture-cargo-claustrum.toml')) {
    if (-not $listed.ContainsKey($name)) { Refuse $name 'is missing from the transport manifest' }
}
try { $paths = @(Get-ChildItem -LiteralPath $InputsDirectory -Force) }
catch { Refuse 'prepush-inputs/' 'cannot be listed on the runner' }
foreach ($path in $paths) {
    if ($path.Name -eq 'transport-manifest.json') { continue }
    if ($path.PSIsContainer -or -not $listed.ContainsKey($path.Name)) {
        Refuse $path.Name 'is not a listed input file'
    }
}
Write-Output "prepush: verified $(@($manifest.files).Count) offline input files on Windows"

function ProbeTool([string]$Name, [string]$Executable, [string]$Pattern) {
    try {
        $output = (& $Executable --version 2>&1 | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or $output -notmatch $Pattern) { return $false }
    } catch { return $false }
    Write-Output "prepush: tool $Name $output"
    return $true
}
function CheckTool([string]$Name, [string]$Executable, [string]$Pattern) {
    $result = @(ProbeTool $Name $Executable $Pattern)
    if ($result[-1] -ne $true) { Refuse $Name 'is missing or has an unsupported version' }
    $result | Select-Object -SkipLast 1 | Write-Output
}
CheckTool 'Python' 'python.exe' '^Python 3\.14\.\d+$'
CheckTool 'nats-server' 'nats-server.exe' '^nats-server: v2\.15\.0$'
CheckTool 'Bun' 'bun.exe' '^1\.4\.\d+$'

$addedPaths = @()
function UseOfflineTool([string]$Name, [string]$Archive, [string]$BinDirectory) {
    if (-not $listed.ContainsKey($Archive)) { Refuse $Archive 'is missing from the transport manifest' }
    $destination = Join-Path $ToolsDirectory ([Guid]::NewGuid().ToString('N'))
    try {
        New-Item -ItemType Directory -Path $destination -Force | Out-Null
        Expand-Archive -LiteralPath (Join-Path $InputsDirectory $Archive) -DestinationPath $destination
        $bin = if ($BinDirectory) { Join-Path $destination $BinDirectory } else { $destination }
        $env:PATH = $bin + [IO.Path]::PathSeparator + $env:PATH
        $script:addedPaths += $bin
    } catch { Refuse $Archive 'could not be extracted' }
    Write-Output "prepush: using verified offline $Name from $Archive"
}

$node = @(ProbeTool 'Node' 'node.exe' '^v24\.\d+\.\d+$')
$npm = @(ProbeTool 'npm' 'npm.cmd' '^\d+\.\d+\.\d+$')
if ($node[-1] -eq $true -and $npm[-1] -eq $true) {
    $node | Select-Object -SkipLast 1 | Write-Output
    $npm | Select-Object -SkipLast 1 | Write-Output
} else {
    UseOfflineTool 'Node/npm' 'node-v24.11.1-win-x64.zip' 'node-v24.11.1-win-x64'
    CheckTool 'Node' 'node.exe' '^v24\.11\.1$'
    CheckTool 'npm' 'npm.cmd' '^\d+\.\d+\.\d+$'
}
foreach ($tool in @(
    @('CMake', 'cmake.exe', '^cmake version 4\.4\.5(\r?\n|$)', 'cmake-4.4.5-windows-x86_64.zip', 'cmake-4.4.5-windows-x86_64/bin'),
    @('Ninja', 'ninja.exe', '^1\.13\.2$', 'ninja-win.zip', '')
)) {
    $result = @(ProbeTool $tool[0] $tool[1] $tool[2])
    if ($result[-1] -eq $true) { $result | Select-Object -SkipLast 1 | Write-Output }
    else {
        UseOfflineTool $tool[0] $tool[3] $tool[4]
        CheckTool $tool[0] $tool[1] $tool[2]
    }
}
try {
    New-Item -ItemType Directory -Path (Split-Path -Parent $PathFile) -Force | Out-Null
    [IO.File]::WriteAllLines($PathFile, [string[]]$addedPaths, [Text.UTF8Encoding]::new($false))
} catch { Refuse 'tool PATH' 'could not be recorded for Git Bash' }
