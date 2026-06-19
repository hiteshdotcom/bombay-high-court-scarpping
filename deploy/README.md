# Running the BHC scraper continuously on Ubuntu (EC2)

Continuous loop with a cool-down between runs, via **systemd**, plus
**logrotate** for the log file. AWS auth uses the keys already in `.env`.

Assumes the project lives at `/home/ubuntu/bombay-high-court-scarpping`. If it's elsewhere, edit the
two paths in `bhc-scraper.service` and `bhc-scraper.logrotate` first.

## 1. One-time setup on the server

```bash
sudo apt update && sudo apt install -y python3 python3-venv python3-pip
cd ~/bombay-high-court-scarpping
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install "scrapling[fetchers]" pymongo boto3
```

Confirm `.env` is correct (keys + the run you want):

```
START_DATE=01-01-2024
END_DATE=31-01-2024
REPT_ONLY=1
LIMIT=0
```

Smoke-test once in the foreground, then Ctrl+C:

```bash
python3 bhc_scrapling.py
```

## 2. Install the systemd service (the continuous loop + cool-down)

```bash
sudo cp ~/bombay-high-court-scarpping/deploy/bhc-scraper.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bhc-scraper.service
```

`enable --now` starts it immediately AND makes it auto-start on reboot.
The cool-down between runs is `RestartSec=3600` (1 hour) in the unit file —
change it to whatever you want and re-run `daemon-reload` + `restart`.

## 3. Install log rotation

```bash
sudo cp ~/bombay-high-court-scarpping/deploy/bhc-scraper.logrotate /etc/logrotate.d/bhc-scraper
sudo logrotate --debug /etc/logrotate.d/bhc-scraper   # dry-run to validate
```

## 4. Watch / manage it

```bash
# live application log (the file the script writes)
tail -f ~/bombay-high-court-scarpping/bhc_scrapling.log

# live service output via journald (includes restarts/cool-downs)
journalctl -u bhc-scraper -f

# status, stop, start, restart
systemctl status bhc-scraper
sudo systemctl stop bhc-scraper
sudo systemctl restart bhc-scraper

# stop it permanently (and don't start on reboot)
sudo systemctl disable --now bhc-scraper
```

## Notes

- **Idempotent re-runs:** with a fixed date window, each loop re-scrapes the
  same window. The script upserts by `uid`, so duplicates are not created —
  re-runs just refresh/confirm existing records. Widen `START_DATE`/`END_DATE`
  to cover more, or bump the dates forward over time for fresh judgments.
- **Cool-down meaning:** the timer starts when a run *finishes*. A run that
  takes 40 min + `RestartSec=3600` ⇒ next run starts ~1 h 40 m after the
  previous one began.
- **MongoDB:** `.env` points at MongoDB Atlas. Make sure the EC2 instance's
  public IP (or `0.0.0.0/0` for testing) is allowed in Atlas Network Access,
  or the script falls back to JSON-only.
