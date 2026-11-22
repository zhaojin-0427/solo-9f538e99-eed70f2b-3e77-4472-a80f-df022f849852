"""Flask 应用工厂。"""
from __future__ import annotations

import os

from flask import Flask

from . import db as db_module
from . import routes


def create_app(config: dict | None = None) -> Flask:
    app = Flask(__name__)
    app.config.from_mapping(
        DATABASE=os.environ.get(
            "PLANNER_DB", os.path.join(app.instance_path, "plans.db")
        ),
        MAX_CONTENT_LENGTH=8 * 1024 * 1024,  # 清单上传上限 8MB
        SECRET_KEY=os.environ.get("SECRET_KEY", "dev-only-secret-change-me"),
    )
    if config:
        app.config.update(config)

    db_module.init_db(app)
    app.teardown_appcontext(db_module.close_db)
    app.register_blueprint(routes.bp)

    @app.template_filter("human_bytes")
    def human_bytes(n) -> str:
        try:
            n = float(n)
        except (TypeError, ValueError):
            return str(n)
        for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
            if n < 1024 or unit == "PiB":
                if unit == "B":
                    return f"{int(n)} {unit}"
                return f"{n:.2f} {unit}"
            n /= 1024
        return f"{n:.2f} PiB"

    @app.template_filter("pct")
    def pct(part, whole) -> str:
        try:
            if not whole:
                return "0.0%"
            return f"{100.0 * part / whole:.1f}%"
        except (TypeError, ValueError):
            return "-"

    return app
