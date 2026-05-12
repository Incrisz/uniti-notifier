# Uniti Pipeline Notifier

A lightweight Python service that monitors the Uniti database and sends updates to Slack.

## What it does

- **Cron job alerts** — polls every 10 minutes and sends a Slack message the moment the intervention cron job runs, including whether it succeeded or failed and the exact timestamp.
- **6-hour stats reports** — every 6 hours (and once on startup) sends a snapshot to Slack showing:
  - Signals and milestones received in the last 6 hours
  - All-time totals for signals and milestones
  - Number of unchecked milestones (flagged with a warning if above zero)
  - When the intervention cron last ran

## Setup

1. Copy `.env.example` to `.env` and fill in your values:
   ```
   DATABASE_URL=postgresql://user:password@host:port/dbname
   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
   ```

2. Get a Slack webhook URL at `https://api.slack.com/apps` → Your App → Incoming Webhooks → Add New Webhook to Workspace.

## Running

**With Docker:**
```bash
docker build -t uniti-pipeline-notifier .
docker run --env-file .env uniti-pipeline-notifier
```

**Locally:**
```bash
pip install -r requirements.txt
python app.py
```
