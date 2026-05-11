# jd_token_bill

Local tooling for JoyAgent usage monitoring, billing views, and a customer-facing usage dashboard.

## Layout

| Path | Purpose |
|------|---------|
| `jd_token_bills/` | Admin: `joyagent_monitor.py` (poll + SQLite), `web_dashboard.py` (full dashboard + reconciliation) |
| `jd_token_bills_client/` | Customer: `client_dashboard.py` (usage-only web UI, Playwright-backed API proxy) |

## Requirements

- Python 3.10+
- Playwright Chromium (for dashboards that drive a logged-in browser)

## Quick start (customer dashboard)

```powershell
cd jd_token_bills_client
pip install -r requirements.txt
python -m playwright install chromium
python client_dashboard.py --login
python client_dashboard.py
```

Default URL: `http://127.0.0.1:8766/`

## Quick start (admin stack)

```powershell
cd jd_token_bills
pip install -r requirements_monitor.txt
python -m playwright install chromium
python joyagent_monitor.py --login
python web_dashboard.py
```

See inline `--help` on each script for options (months, ports, import bills, etc.).

## Security note

Do not commit `joyagent_profile/` or `*.db`; they contain session and usage data. They are listed in `.gitignore`.

## License

Use at your own risk; not affiliated with JD or JoyAgent.
