import os
from flask import Flask
from .extensions import db
from .routes import bp
from .scheduler import init_scheduler


def create_app():
    app = Flask(__name__)

    data_dir = os.getenv("DATA_DIR", "/data")
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(os.path.join(data_dir, "pdfs"), exist_ok=True)

    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-change-me")
    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv(
        "DATABASE_URL", f"sqlite:///{os.path.join(data_dir, 'app.db')}"
    )
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["DATA_DIR"] = data_dir
    app.config["PDF_STORAGE_DIR"] = os.path.join(data_dir, "pdfs")
    app.config["SCAN_URL"] = os.getenv(
        "SCAN_URL",
        "https://governodigital.foz.pr.gov.br/governo-digital/contribuinte/imoveis/1/13?aba=debitos",
    )
    app.config["SEQUENTIAL_SCAN_ENABLED"] = os.getenv("SEQUENTIAL_SCAN_ENABLED", "true").lower() == "true"
    app.config["SEQUENTIAL_URL_TEMPLATE"] = os.getenv(
        "SEQUENTIAL_URL_TEMPLATE",
        "https://governodigital.foz.pr.gov.br/governo-digital/contribuinte/imoveis/1/{id}?aba=debitos",
    )
    app.config["SEQUENCE_START_ID"] = int(os.getenv("SEQUENCE_START_ID", "1"))
    app.config["SEQUENCE_END_ID"] = int(os.getenv("SEQUENCE_END_ID", "5000"))
    app.config["SCAN_BATCH_SIZE"] = int(os.getenv("SCAN_BATCH_SIZE", "50"))
    app.config["SCAN_INTERVAL_MINUTES"] = int(os.getenv("SCAN_INTERVAL_MINUTES", "30"))
    app.config["CRAWL_MAX_DEPTH"] = int(os.getenv("CRAWL_MAX_DEPTH", "2"))
    app.config["CRAWL_TIMEOUT_SECONDS"] = int(os.getenv("CRAWL_TIMEOUT_SECONDS", "25"))
    app.config["PLAYWRIGHT_ENABLED"] = os.getenv("PLAYWRIGHT_ENABLED", "true").lower() == "true"
    app.config["PLAYWRIGHT_HEADLESS"] = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"
    app.config["PLAYWRIGHT_TIMEOUT_MS"] = int(os.getenv("PLAYWRIGHT_TIMEOUT_MS", "30000"))
    app.config["PLAYWRIGHT_WAIT_AFTER_PRINT_MS"] = int(os.getenv("PLAYWRIGHT_WAIT_AFTER_PRINT_MS", "2500"))
    app.config["USER_AGENT"] = os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    )

    db.init_app(app)
    app.register_blueprint(bp)

    with app.app_context():
        db.create_all()
        init_scheduler(app)

    return app
