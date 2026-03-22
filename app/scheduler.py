from apscheduler.schedulers.background import BackgroundScheduler
from .crawler import run_scan

scheduler = BackgroundScheduler()
_scheduler_started = False


def scheduled_job(app):
    with app.app_context():
        run_scan()


def init_scheduler(app):
    global _scheduler_started
    if _scheduler_started:
        return

    interval = app.config["SCAN_INTERVAL_MINUTES"]
    scheduler.add_job(
        func=scheduled_job,
        trigger="interval",
        minutes=interval,
        args=[app],
        id="pdf_scan_job",
        replace_existing=True,
    )
    scheduler.start()
    _scheduler_started = True
