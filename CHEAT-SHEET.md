# Dashboard Cheat Sheet (Daily)

## Production (fastest)
1. Open Vercel app.
2. If it’s slow/blank, wake backend:
   - `https://scanner-z7zl.onrender.com/docs`
   - then `https://scanner-z7zl.onrender.com/api/themes?view=scanner`
3. Hard refresh Vercel: `Ctrl+F5`.

## Local dev (when coding)
Backend:
```powershell
.\.venv\Scripts\python -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

Frontend:
```powershell
cd frontend
npm run dev -- --host 127.0.0.1 --port 5173
```

## Publish updates (end of work)
```powershell
.\scripts\safe-update-confirm.ps1 -Message "feat: what you changed"
```

## If it breaks
- Check Vercel env var:
  - `VITE_API_BASE_URL = https://scanner-z7zl.onrender.com` (All Environments)
- Redeploy latest in Vercel + Render.
