# xmd official image build script (linux/amd64 only by design).
#
# Build mode (no -Push): go vet -> cross-compile static linux/amd64 binary ->
# docker build (alpine wrapper image, no network steps inside the build; see
# xingmeng/Dockerfile). Image name is fixed: defeatedperson/xmdisk.
#
# Version policy: manual semver input (vX.Y.Z). The last successfully built
# version is remembered in var/build/image-version.txt (gitignored). On each
# run the script shows the last version and suggests the next patch bump.
# Rebuilding the same tag is allowed but asks for confirmation.
#
# Push mode (-Push): no build. Pushes the last built local version tag plus
# "latest" to Docker Hub (defeatedperson/xmdisk). One-time setup: docker login.
#
# Usage:
#   powershell -File scripts/build-image.ps1                     # build, prompt for version
#   powershell -File scripts/build-image.ps1 v0.2.0              # build, explicit version
#   powershell -File scripts/build-image.ps1 v0.2.0 -Yes         # no confirmations
#   powershell -File scripts/build-image.ps1 v0.2.0 -RunTests    # go test first
#   powershell -File scripts/build-image.ps1 -Push               # publish last built to Docker Hub

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Version,          # build mode: image tag, e.g. v0.1.0; omit to be prompted.
                               # (ignored in push mode)

    [switch]$Push,             # push mode: publish last built version + latest to Docker Hub

    [switch]$RunTests,         # build mode: run full "go test ./..." before building
    [switch]$Yes               # assume yes for all confirmations (non-interactive use)
)

$ErrorActionPreference = "Stop"
$Root     = Split-Path -Parent $PSScriptRoot   # scripts/ -> repo root
$StateDir  = Join-Path $Root "var/build"
$StateFile = Join-Path $StateDir "image-version.txt"
$SrcDir    = Join-Path $Root "xingmeng"

function Write-Info { param($m) Write-Host $m -ForegroundColor Cyan }
function Write-Ok   { param($m) Write-Host $m -ForegroundColor Green }
function Write-Warn2{ param($m) Write-Host $m -ForegroundColor Yellow }
function Write-Fail { param($m) Write-Host $m -ForegroundColor Red; exit 1 }

# --- version handling ------------------------------------------------------

$verPattern = '^v\d+\.\d+\.\d+$'
$last = ""
if (Test-Path $StateFile) { $last = (Get-Content $StateFile -First 1).Trim() }

# Suggestion: patch bump of the last version, or v0.1.0 for the first build.
$suggest = "v0.1.0"
if ($last -match $verPattern) {
    $p = $last.Substring(1).Split(".")
    $suggest = "v{0}.{1}.{2}" -f [int]$p[0], [int]$p[1], ([int]$p[2] + 1)
}

if ($last) {
    $builtAt = (Get-Item $StateFile).LastWriteTime.ToString("yyyy-MM-dd HH:mm")
    Write-Info "Last built image version: $last  (built $builtAt)"
} else {
    Write-Info "No previous image build on record; this is the first build."
}

$Image = "defeatedperson/xmdisk"

# --- push mode: publish the last built version, no build --------------------

if ($Push) {
    if (-not [string]::IsNullOrWhiteSpace($Version)) {
        Write-Warn2 "-Push publishes the last built version; version argument '$Version' is ignored."
    }
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Write-Fail "docker not found in PATH." }
    $null = & { $ErrorActionPreference = 'Continue'; docker version --format "{{.Server.Version}}" *> $null }
    if ($LASTEXITCODE -ne 0) { Write-Fail "Docker daemon not reachable (is Docker Desktop running?)." }
    if (-not $last) { Write-Fail "No image build on record yet; run the build flow first." }

    Write-Info "=== Pushing ${Image}:$last and :latest to Docker Hub ==="
    $null = & { $ErrorActionPreference = 'Continue'; docker image inspect "${Image}:$last" *> $null }
    if ($LASTEXITCODE -ne 0) { Write-Fail "Local image ${Image}:$last not found; rebuild it first." }

    docker push "${Image}:$last"
    if ($LASTEXITCODE -ne 0) { Write-Fail "docker push failed (not logged in? run: docker login)" }
    docker push "${Image}:latest"
    if ($LASTEXITCODE -ne 0) { Write-Fail "docker push latest failed." }
    Write-Ok "=== Done: pushed ${Image}:$last and ${Image}:latest ==="
    exit 0
}

# --- build mode: version input ----------------------------------------------

if ([string]::IsNullOrWhiteSpace($Version)) {
    if ($Yes) {
        $Version = $suggest
    } else {
        $Version = Read-Host "New image version [suggestion: $suggest]"
        if ([string]::IsNullOrWhiteSpace($Version)) { $Version = $suggest }
    }
}
if ($Version -notmatch $verPattern) {
    Write-Fail "Invalid version '$Version': expected vX.Y.Z (e.g. v0.1.0)."
}
if ($Version -eq $last) {
    Write-Warn2 "Version $Version was already built before; the local image tag will be overwritten."
    if (-not $Yes) {
        $ans = Read-Host "Rebuild the same tag anyway? (y/N)"
        if ($ans -notmatch '(?i)^y') { Write-Fail "Aborted by user." }
    }
}

# --- environment sanity ----------------------------------------------------

if (-not (Get-Command go -ErrorAction SilentlyContinue)) { Write-Fail "go not found in PATH." }
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Write-Fail "docker not found in PATH." }
# PS 5.1: with $ErrorActionPreference=Stop, stderr redirection of native commands
# raises NativeCommandError; probe inside a Continue-preference scope.
$null = & { $ErrorActionPreference = 'Continue'; docker version --format "{{.Server.Version}}" *> $null }
if ($LASTEXITCODE -ne 0) { Write-Fail "Docker daemon not reachable (is Docker Desktop running?)." }

$gitRev = & { $ErrorActionPreference = 'Continue'; git -C $Root rev-parse --short HEAD 2>$null }
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($gitRev)) { $gitRev = "" }

Write-Info "=== Building ${Image}:$Version (linux/amd64) ==="

# --- build -----------------------------------------------------------------

Push-Location $SrcDir

if ($RunTests) {
    Write-Info "--- go test ./... ---"
    go test ./...
    if ($LASTEXITCODE -ne 0) { Pop-Location; Write-Fail "Tests failed; aborting." }
}

Write-Info "--- go vet ./... ---"
go vet ./...
if ($LASTEXITCODE -ne 0) { Pop-Location; Write-Fail "go vet failed; aborting." }

Write-Info "--- cross-compile linux/amd64 static binary ---"
$env:CGO_ENABLED = "0"
$env:GOOS = "linux"
$env:GOARCH = "amd64"
New-Item -ItemType Directory -Force -Path "dist" | Out-Null
go build -trimpath -ldflags "-s -w" -o dist/xmd ./cmd/xmd
$buildStatus = $LASTEXITCODE
Remove-Item Env:CGO_ENABLED, Env:GOOS, Env:GOARCH -ErrorAction SilentlyContinue
if ($buildStatus -ne 0) { Pop-Location; Write-Fail "Cross-compile failed." }
Pop-Location

Write-Info "--- docker build ---"
$labelArgs = @(
    "--label", "org.opencontainers.image.title=xmd",
    "--label", "org.opencontainers.image.description=Single-binary S3/SFTP/WebDAV self-hosted storage with master-slave replication",
    "--label", "org.opencontainers.image.version=$Version",
    "--label", "org.opencontainers.image.licenses=Apache-2.0",
    "--label", "org.opencontainers.image.source=https://github.com/defeatedperson/xmdisk"
)
if ($gitRev) { $labelArgs += @("--label", "org.opencontainers.image.revision=$gitRev") }

docker build -f (Join-Path $SrcDir "Dockerfile") @labelArgs `
    -t "${Image}:$Version" -t "${Image}:latest" $SrcDir
if ($LASTEXITCODE -ne 0) { Write-Fail "docker build failed." }

# --- record state ----------------------------------------------------------

New-Item -ItemType Directory -Force -Path $StateDir | Out-Null
Set-Content -Path $StateFile -Value $Version -Encoding Ascii

$binSize = "{0:N1} MB" -f ((Get-Item (Join-Path $SrcDir "dist/xmd")).Length / 1MB)
Write-Ok "=== Done ==="
Write-Ok "  image : ${Image}:$Version (+ latest)"
Write-Ok "  binary: xingmeng/dist/xmd ($binSize, linux/amd64 static)"
if ($gitRev) { Write-Ok "  commit: $gitRev" }
if (-not $Push) { Write-Host  "  note  : run again with -Push to publish this version to Docker Hub" -ForegroundColor DarkGray }
