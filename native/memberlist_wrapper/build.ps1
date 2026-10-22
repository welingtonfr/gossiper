# Build script for Windows — produces gossiper.dll in the project blobs/ directory
# Requirements: Go 1.21+, CGO_ENABLED=1, and a C compiler in PATH
# On Windows the easiest option is tdm-gcc or MSYS2 mingw-w64.

param(
    [string]$OutDir = "..\..\blobs\windows"
)

$ErrorActionPreference = "Stop"

Write-Host "Fetching Go dependencies..."
go mod tidy

Write-Host "Building gossiper.dll..."
$env:CGO_ENABLED = "1"
$env:GOOS = "windows"
$env:GOARCH = "amd64"

New-Item -ItemType Directory -Force $OutDir | Out-Null

go build -buildmode=c-shared `
    -o "$OutDir\gossiper.dll" `
    .

Write-Host "Done. Output: $OutDir\gossiper.dll"
