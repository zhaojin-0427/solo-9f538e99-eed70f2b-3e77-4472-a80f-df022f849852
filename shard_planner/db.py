"""SQLite 连接与 schema 初始化。"""
from __future__ import annotations

import os

import sqlite3
from flask import current_app, g

SCHEMA = """
CREATE TABLE IF NOT EXISTS plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_hash TEXT NOT NULL UNIQUE,
    worker_count INTEGER NOT NULL,
    worker_byte_limit INTEGER NOT NULL,
    entity_count INTEGER NOT NULL,
    partition_count INTEGER NOT NULL,
    total_bytes INTEGER NOT NULL,
    total_records INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'published',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS shards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id INTEGER NOT NULL REFERENCES plans(id),
    shard_index INTEGER NOT NULL,
    total_bytes INTEGER NOT NULL,
    total_records INTEGER NOT NULL,
    UNIQUE (plan_id, shard_index)
);

CREATE TABLE IF NOT EXISTS shard_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    shard_id INTEGER NOT NULL REFERENCES shards(id),
    entity TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    records INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS partitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id INTEGER NOT NULL REFERENCES plans(id),
    shard_id INTEGER NOT NULL REFERENCES shards(id),
    partition_key TEXT NOT NULL,
    entity TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    records INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_shards_plan ON shards(plan_id);
CREATE INDEX IF NOT EXISTS idx_shard_entities_shard ON shard_entities(shard_id);
CREATE INDEX IF NOT EXISTS idx_partitions_plan ON partitions(plan_id);
CREATE INDEX IF NOT EXISTS idx_partitions_shard ON partitions(shard_id);
"""


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(current_app.config["DATABASE"])
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
        g.db.execute("PRAGMA busy_timeout = 5000")
    return g.db


def close_db(_exc=None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db(app) -> None:
    path = app.config["DATABASE"]
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with app.app_context():
        db = get_db()
        db.executescript(SCHEMA)
        db.execute("PRAGMA journal_mode = WAL")
        db.commit()
