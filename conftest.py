import pytest

from shard_planner import create_app
from shard_planner.db import get_db


@pytest.fixture()
def app(tmp_path):
    return create_app({"TESTING": True, "DATABASE": str(tmp_path / "test.db")})


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def db(app):
    with app.app_context():
        yield get_db()
