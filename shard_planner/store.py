"""计划的持久化与查询。计划只插入、不更新：新清单产生新计划，已发布计划不被改写。"""
from __future__ import annotations

import sqlite3

from .planner import Partition, aggregate_entities


def find_plan_by_hash(db: sqlite3.Connection, manifest_hash: str):
    return db.execute("SELECT * FROM plans WHERE manifest_hash = ?", (manifest_hash,)).fetchone()


def list_plans(db: sqlite3.Connection):
    return db.execute("SELECT * FROM plans ORDER BY id DESC").fetchall()


def create_plan(
    db: sqlite3.Connection,
    manifest_hash: str,
    worker_count: int,
    worker_byte_limit: int,
    partitions: list[Partition],
    shards: list[list[str]],
) -> int:
    """把可行方案整体写入一个事务。manifest_hash 冲突时抛 sqlite3.IntegrityError。"""
    entity_agg = aggregate_entities(partitions)
    total_bytes = sum(p.bytes for p in partitions)
    total_records = sum(p.records for p in partitions)

    try:
        db.execute("BEGIN")
        cur = db.execute(
            """
            INSERT INTO plans (manifest_hash, worker_count, worker_byte_limit,
                               entity_count, partition_count, total_bytes, total_records)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (manifest_hash, worker_count, worker_byte_limit,
             len(entity_agg), len(partitions), total_bytes, total_records),
        )
        plan_id = cur.lastrowid

        entity_to_shard: dict[str, int] = {}
        for shard_index, entities in enumerate(shards):
            shard_bytes = sum(entity_agg[e][0] for e in entities)
            shard_records = sum(entity_agg[e][1] for e in entities)
            cur = db.execute(
                "INSERT INTO shards (plan_id, shard_index, total_bytes, total_records) VALUES (?, ?, ?, ?)",
                (plan_id, shard_index, shard_bytes, shard_records),
            )
            shard_id = cur.lastrowid
            for entity in entities:
                entity_to_shard[entity] = shard_id
                db.execute(
                    "INSERT INTO shard_entities (shard_id, entity, bytes, records) VALUES (?, ?, ?, ?)",
                    (shard_id, entity, entity_agg[entity][0], entity_agg[entity][1]),
                )

        for p in sorted(partitions, key=lambda p: (p.entity, p.partition_key)):
            db.execute(
                """
                INSERT INTO partitions (plan_id, shard_id, partition_key, entity, bytes, records)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (plan_id, entity_to_shard[p.entity], p.partition_key, p.entity, p.bytes, p.records),
            )
        db.commit()
        return plan_id
    except Exception:
        db.rollback()
        raise


def get_plan_detail(db: sqlite3.Connection, plan_id: int):
    """返回计划详情：plan 行 + 每个分片的实体与分区列表。不存在时返回 None。"""
    plan = db.execute("SELECT * FROM plans WHERE id = ?", (plan_id,)).fetchone()
    if plan is None:
        return None
    shard_rows = db.execute(
        "SELECT * FROM shards WHERE plan_id = ? ORDER BY shard_index", (plan_id,)
    ).fetchall()
    shards = []
    for shard in shard_rows:
        entities = db.execute(
            "SELECT entity, bytes, records FROM shard_entities WHERE shard_id = ? ORDER BY entity",
            (shard["id"],),
        ).fetchall()
        parts = db.execute(
            """
            SELECT partition_key, entity, bytes, records
            FROM partitions WHERE shard_id = ? ORDER BY entity, partition_key
            """,
            (shard["id"],),
        ).fetchall()
        shards.append({"row": shard, "entities": entities, "partitions": parts})
    return {"plan": plan, "shards": shards}
