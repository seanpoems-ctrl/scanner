param(
    [Parameter(Mandatory = $true)]
    [string]$Message
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Step($text) {
    Write-Host ""
    Write-Host "==> $text" -ForegroundColor Cyan
}

function Warn($text) {
    Write-Host "WARN: $text" -ForegroundColor Yellow
}

function Ok($text) {
    Write-Host "OK: $text" -ForegroundColor Green
}

function Confirm($prompt) {
    $answer = Read-Host "$prompt (y/N)"
    return $answer -match "^(y|yes)$"
}

Step "Verifying git repository"
git rev-parse --is-inside-work-tree | Out-Null

Step "Pulling latest changes"
git pull

Step "Checking working tree"
$status = git status --porcelain
if (-not $status) {
    Warn "No local changes detected. Nothing to commit."
    exit 0
}
git status --short

Step "Building frontend (quick safety check)"
if (Test-Path ".\frontend\package.json") {
    Push-Location ".\frontend"
    npm run build
    Pop-Location
} else {
    Warn "frontend/package.json not found; skipping frontend build."
}

Step "Compiling backend Python files (quick syntax check)"
if (Test-Path ".\backend") {
    python -m compileall .\backend
} else {
    Warn "backend folder not found; skipping backend compile check."
}

Step "Reviewing diff"
git diff --stat

if (-not (Confirm "Create commit with message: `"$Message`"?")) {
    Warn "Commit cancelled by user."
    exit 0
}

Step "Committing changes"
git add .
git commit -m $Message

if (-not (Confirm "Push this commit to remote now?")) {
    Warn "Push skipped. Commit is local only."
    exit 0
}

Step "Pushing to remote"
git push

Step "Done"
Ok "Update flow complete."
Write-Host ""
Write-Host "Next: confirm Render and Vercel deployments are Ready, then hard refresh production." -ForegroundColor Gray
