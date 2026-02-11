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

## 6) Workflow diario (GitHub Actions) em banco persistente

O workflow `daily.yml` executa para `D-1` (TZ `America/Sao_Paulo`) por padrao, com override opcional via `workflow_dispatch.inputs.date`.

Secrets obrigatorios no repositório:
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `DB_SSLMODE` (opcional; default `prefer`)

Comando executado no workflow:
```bash
uv run python -m etl.app run --date "$date_str" --checks --engine direct
uv run python -m etl.validate_marts --apply-minimal --dry-run --engine direct
```

## 7) Bootstrap inicial (1x no banco persistente)

O proprio `etl.app run --checks` executa as etapas de `ref/enrich/marts/sqlm`.  
Para inicializar o banco persistente:

```powershell
python -m etl.app run --date YYYY-MM-DD --checks --engine direct
```

Observacao: camadas tematicas (bioma/uc/ti) dependem de fontes no schema `ref`. Sem essas fontes, a pipeline segue com fallback (camadas vazias), sem quebrar o job.

## 8) Seguranca de rede

- Nao salvar credenciais em arquivos versionados.
- Use somente GitHub Secrets para `DB_*`.
- Restrinja acesso ao Postgres (allowlist, VPN, rede privada ou tunel), evitando exposicao publica irrestrita.
