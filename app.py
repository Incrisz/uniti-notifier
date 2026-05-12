import logging
import os
from datetime import datetime, timezone

import psycopg2
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# tracks the last cron run timestamp we already notified about
_last_known_run_at = None


def get_db():
    return psycopg2.connect(DATABASE_URL)


def send_slack(text: str):
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping Slack notification")
        return
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"text": f"<!channel>\n{text}"}, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        logger.error("Failed to send Slack message: %s", exc)


def check_cronjob_run():
    """Poll for a new cron run and notify Slack if one is detected."""
    global _last_known_run_at
    try:
        conn = get_db()
        cur = conn.cursor()

        # Try cron_job_status table first (managed by uniti-cronjob-service)
        try:
            cur.execute(
                "SELECT last_run_at, last_success, last_message, run_count "
                "FROM cron_job_status LIMIT 1"
            )
            row = cur.fetchone()
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            row = None

        if row:
            last_run_at, last_success, last_message, run_count = row
        else:
            # Fallback: detect new runs via intervention_logs
            cur.execute("SELECT MAX(created_at) FROM intervention_logs")
            result = cur.fetchone()
            last_run_at = result[0] if result else None
            last_success = last_run_at is not None
            last_message = "Detected via intervention_logs"
            run_count = "N/A"

        cur.close()
        conn.close()

        if not last_run_at:
            return

        if last_run_at != _last_known_run_at:
            _last_known_run_at = last_run_at
            icon = "✅" if last_success else "❌"
            ts = last_run_at.strftime("%Y-%m-%d %H:%M:%S UTC")
            send_slack(
                f"{icon} *Intervention Cron Job Ran*\n"
                f"• Time: `{ts}`\n"
                f"• Status: {'Success' if last_success else 'Failed'}\n"
                f"• Total runs: {run_count}\n"
                f"• Note: {last_message or 'N/A'}"
            )

    except Exception as exc:
        logger.error("check_cronjob_run error: %s", exc)


def send_stats_report():
    """Send a 6-hour DB stats snapshot to Slack."""
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM signal_logs")
        total_signals = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM milestone_logs")
        total_milestones = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM milestone_logs WHERE is_checked = false")
        unchecked = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM signal_logs "
            "WHERE created_at >= NOW() - INTERVAL '6 hours'"
        )
        signals_6h = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM milestone_logs "
            "WHERE created_at >= NOW() - INTERVAL '6 hours'"
        )
        milestones_6h = cur.fetchone()[0]

        cur.execute("SELECT MAX(created_at) FROM intervention_logs")
        last_intervention = cur.fetchone()[0]

        cur.close()
        conn.close()

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        last_int_str = (
            last_intervention.strftime("%Y-%m-%d %H:%M UTC")
            if last_intervention
            else "Never"
        )
        unchecked_flag = " ⚠️" if unchecked > 0 else " ✅"

        send_slack(
            f"📊 *Signals-Milestones Stats Report* — `{now_str}`\n\n"
            f"*Last 6 Hours*\n"
            f"• Signals received: `{signals_6h}`\n"
            f"• Milestones triggered: `{milestones_6h}`\n\n"
            f"*All Time*\n"
            f"• Total signals: `{total_signals}`\n"
            f"• Total milestones: `{total_milestones}`\n"
            f"• Unchecked milestones: `{unchecked}`{unchecked_flag}\n\n"
            f"*Last Intervention Cron Run:* `{last_int_str}`"
        )

    except Exception as exc:
        logger.error("send_stats_report error: %s", exc)
        send_slack(f"❌ *Stats report failed*: {exc}")


if __name__ == "__main__":
    logger.info("Starting Uniti pipeline notifier...")

    # Fire an immediate stats report on startup
    send_stats_report()

    scheduler = BlockingScheduler(timezone="UTC")

    # Detect new cron runs — poll every 2 minutes
    scheduler.add_job(check_cronjob_run, "interval", minutes=2, id="cronjob_check")

    # DB stats snapshot — every 6 hours
    scheduler.add_job(send_stats_report, "interval", hours=6, id="stats_report")

    logger.info("Scheduler running: cron-check every 2 min | stats every 6 h")
    scheduler.start()
