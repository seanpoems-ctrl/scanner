# START HERE: Daily Dashboard Workflow

Use this checklist each day to get the dashboard running quickly and safely.

## 1) Open production first
- Open your Vercel app URL.
- If it loads, you are done.
- If it shows loading/fetch errors, continue to Step 2.

## 2) Wake backend (Render free tier)
- Open: `https://scanner-z7zl.onrender.com/docs`
- Wait for page to load.
- Then open: `https://scanner-z7zl.onrender.com/api/themes?view=scanner`
- Confirm JSON is returned.

## 3) Refresh frontend
- Go back to your Vercel app.
- Hard refresh with `Ctrl+F5`.

## 4) Quick troubleshoot if still blank
- Open browser devtools (`F12`) and check Console/Network.
- In Vercel, confirm env var:
  - `VITE_API_BASE_URL = https://scanner-z7zl.onrender.com`
  - scope: All Environments (Production + Preview + Development)
- If env var changed, redeploy latest Vercel deployment.

## 5) Local dev (when coding)
Open 2 terminals in project root:

### Backend
```powershell
.\.venv\Scripts\python -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

### Frontend
```powershell
cd frontend
npm run dev -- --host 127.0.0.1 --port 5173
```

## 6) End-of-day safe publish
From project root:

```powershell
.\scripts\safe-update-confirm.ps1 -Message "feat: short description"
```

This script will:
- pull latest
- run frontend build + backend compile checks
- ask before commit
- ask before push

## 7) Confirm cloud deploy after push
- Render: latest deploy is Live.
- Vercel: latest deployment is Ready.
- Open live app and hard refresh once.
