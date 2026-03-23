import os
from flask import Flask
from .extensions import db
from .routes import bp
from .scheduler import init_scheduler


def _get_database_uri(data_dir: str) -> str:
    default_sqlite = f"sqlite:///{os.path.join(data_dir, 'app.db')}"
    database_url = (os.getenv("DATABASE_URL") or os.getenv("SQLALCHEMY_DATABASE_URI") or "").strip()

    if not database_url:
        return default_sqlite

    # Railway/alguns providers podem entregar postgres://
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+psycopg://", 1)
    # Se vier sem driver explícito, força psycopg v3
    elif database_url.startswith("postgresql://") and not database_url.startswith("postgresql+"):
        database_url = database_url.replace("postgresql://", "postgresql+psycopg://", 1)

    return database_url


def create_app():
    app = Flask(__name__)

    data_dir = os.getenv("DATA_DIR", "/data")
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(os.path.join(data_dir, "pdfs"), exist_ok=True)

    database_uri = _get_database_uri(data_dir)

    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-change-me")
    app.config["SQLALCHEMY_DATABASE_URI"] = database_uri
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    if database_uri.startswith("postgresql+psycopg://") or database_uri.startswith("postgresql://"):
        app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
            "pool_pre_ping": True,
            "pool_recycle": 1800,
        }

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
    app.config["AUTO_INSTALL_PLAYWRIGHT_BROWSERS"] = (
        os.getenv("AUTO_INSTALL_PLAYWRIGHT_BROWSERS", "true").lower() == "true"
    )
    os.environ.setdefault(
        "PLAYWRIGHT_BROWSERS_PATH",
        os.getenv("PLAYWRIGHT_BROWSERS_PATH", os.path.join(data_dir, "ms-playwright")),
    )
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
