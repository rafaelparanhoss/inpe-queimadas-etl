# Runbook v1.x

## 1) ETL (dia unico)
```powershell
python -m etl.app run --date YYYY-MM-DD --checks --engine direct --mode dashboard
```

## 2) API
```powershell
cd api
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --log-level info
```

## 3) WEB
```powershell
cd web
npm install
npm run dev
```

## 4) Smoke
```powershell
powershell -ExecutionPolicy Bypass -File scripts\smoke.ps1 -BaseUrl "http://127.0.0.1:8000"
```

## 5) Regra de datas
- Range da API: `[from,to)` (`to` exclusivo).
- Periodo de 1 dia: `from=D` e `to=D+1`.
