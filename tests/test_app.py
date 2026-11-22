"""Web 层集成测试：发布、幂等重试、超限分类、不可变性、导出。"""
import io
import json

FEASIBLE_MANIFEST = [
    {"partition_key": "dt=2026-01-01/hour=00", "entity": "orders", "bytes": 300, "records": 30},
    {"partition_key": "dt=2026-01-01/hour=01", "entity": "orders", "bytes": 200, "records": 20},
    {"partition_key": "dt=2026-01-01", "entity": "users", "bytes": 400, "records": 40},
    {"partition_key": "dt=2026-01-02", "entity": "events", "bytes": 700, "records": 70},
]


def post_plan(client, manifest, workers=2, limit=900):
    return client.post(
        "/plans",
        data={
            "manifest_text": json.dumps(manifest),
            "worker_count": str(workers),
            "worker_byte_limit": str(limit),
        },
    )


def plan_count(db):
    return db.execute("SELECT COUNT(*) AS c FROM plans").fetchone()["c"]


def test_publish_feasible_plan(client, db):
    resp = post_plan(client, FEASIBLE_MANIFEST)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/plans/1")
    assert plan_count(db) == 1

    page = client.get("/plans/1")
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    # 页面展示每个分片的实体、文件与总量
    assert "分片 0" in html and "分片 1" in html
    assert "orders" in html and "users" in html and "events" in html
    assert "dt=2026-01-01/hour=00" in html
    assert "1600" in html  # 总字节


def test_shard_assignment_valid(client, db):
    post_plan(client, FEASIBLE_MANIFEST)
    shards = db.execute("SELECT * FROM shards WHERE plan_id = 1").fetchall()
    assert len(shards) == 2
    for shard in shards:
        assert shard["total_bytes"] <= 900
    # 同一实体的分区全部落在同一分片
    rows = db.execute(
        "SELECT entity, COUNT(DISTINCT shard_id) AS n FROM partitions GROUP BY entity"
    ).fetchall()
    assert all(r["n"] == 1 for r in rows)
    # 分片总量与分区明细一致
    for shard in shards:
        s = db.execute(
            "SELECT COALESCE(SUM(bytes),0) AS b, COALESCE(SUM(records),0) AS r "
            "FROM partitions WHERE shard_id = ?",
            (shard["id"],),
        ).fetchone()
        assert s["b"] == shard["total_bytes"]
        assert s["r"] == shard["total_records"]


def test_duplicate_submission_returns_original_plan(client, db):
    first = post_plan(client, FEASIBLE_MANIFEST)
    second = post_plan(client, FEASIBLE_MANIFEST)
    assert first.headers["Location"] == second.headers["Location"] == "/plans/1"
    assert plan_count(db) == 1
    # 跟随重定向能看到“返回原计划”的提示
    page = client.get("/plans/1", follow_redirects=False)
    assert page.status_code == 200


def test_duplicate_submission_reordered_rows(client, db):
    post_plan(client, FEASIBLE_MANIFEST)
    reordered = list(reversed(FEASIBLE_MANIFEST))
    resp = post_plan(client, reordered)
    assert resp.headers["Location"] == "/plans/1"
    assert plan_count(db) == 1


def test_new_manifest_creates_new_plan_and_keeps_old(client, db):
    post_plan(client, FEASIBLE_MANIFEST)
    changed = [dict(row) for row in FEASIBLE_MANIFEST]
    changed[0]["bytes"] = 250  # orders 变为 450，仍然可行：450+400<=900, 700<=900
    resp = post_plan(client, changed)
    assert resp.headers["Location"] == "/plans/2"
    assert plan_count(db) == 2
    # 已发布计划不被改写
    old = db.execute("SELECT * FROM plans WHERE id = 1").fetchone()
    assert old["total_bytes"] == 1600
    page = client.get("/plans/1")
    assert page.status_code == 200
    assert "1600" in page.get_data(as_text=True)


def test_same_manifest_different_params_is_new_plan(client, db):
    post_plan(client, FEASIBLE_MANIFEST, workers=2, limit=900)
    resp = post_plan(client, FEASIBLE_MANIFEST, workers=4, limit=900)
    assert resp.headers["Location"] == "/plans/2"
    assert plan_count(db) == 2


def test_single_entity_overflow_not_published(client, db):
    manifest = [
        {"partition_key": "p1", "entity": "huge", "bytes": 1500, "records": 1},
        {"partition_key": "p2", "entity": "tiny", "bytes": 10, "records": 1},
    ]
    resp = post_plan(client, manifest, workers=4, limit=1000)
    assert resp.status_code == 422
    assert "单实体超限" in resp.get_data(as_text=True)
    assert plan_count(db) == 0


def test_capacity_overflow_not_published(client, db):
    manifest = [
        {"partition_key": f"p{i}", "entity": f"e{i}", "bytes": 500, "records": 1}
        for i in range(4)
    ]
    resp = post_plan(client, manifest, workers=2, limit=900)
    assert resp.status_code == 422
    assert "分片超限" in resp.get_data(as_text=True)
    assert plan_count(db) == 0


def test_no_feasible_combination_not_published(client, db):
    manifest = [
        {"partition_key": f"p{i}", "entity": f"e{i}", "bytes": 600, "records": 1}
        for i in range(5)
    ]
    resp = post_plan(client, manifest, workers=3, limit=1000)
    assert resp.status_code == 422
    assert "无可行组合" in resp.get_data(as_text=True)
    assert plan_count(db) == 0


def test_invalid_manifest_rejected(client, db):
    resp = client.post(
        "/plans",
        data={"manifest_text": "not json at all {", "worker_count": "2", "worker_byte_limit": "10"},
    )
    assert resp.status_code == 400
    assert plan_count(db) == 0


def test_bad_params_rejected(client, db):
    resp = client.post(
        "/plans",
        data={"manifest_text": json.dumps(FEASIBLE_MANIFEST), "worker_count": "0",
              "worker_byte_limit": "100"},
    )
    assert resp.status_code == 400
    assert plan_count(db) == 0


def test_entity_limit_enforced(client, db):
    manifest = [
        {"partition_key": f"p{i}", "entity": f"e{i}", "bytes": 1, "records": 1}
        for i in range(41)
    ]
    resp = post_plan(client, manifest, workers=50, limit=100)
    assert resp.status_code == 400
    assert "实体数" in resp.get_data(as_text=True)
    assert plan_count(db) == 0


def test_csv_upload(client, db):
    csv_text = (
        "partition_key,entity,bytes,records\n"
        "dt=2026-01-01,orders,300,30\n"
        "dt=2026-01-02,orders,200,20\n"
        "dt=2026-01-01,users,400,40\n"
    )
    resp = client.post(
        "/plans",
        data={
            "manifest_file": (io.BytesIO(csv_text.encode("utf-8")), "manifest.csv"),
            "worker_count": "2",
            "worker_byte_limit": "900",
        },
        content_type="multipart/form-data",
    )
    assert resp.status_code == 302
    assert plan_count(db) == 1
    plan = db.execute("SELECT * FROM plans WHERE id = 1").fetchone()
    assert plan["total_bytes"] == 900
    assert plan["entity_count"] == 2


def test_export_json(client, db):
    post_plan(client, FEASIBLE_MANIFEST)
    resp = client.get("/plans/1/export.json")
    assert resp.status_code == 200
    payload = json.loads(resp.get_data(as_text=True))
    assert payload["plan_id"] == 1
    assert payload["worker_count"] == 2
    assert len(payload["shards"]) == 2
    total = sum(s["total_bytes"] for s in payload["shards"])
    assert total == 1600
    for shard in payload["shards"]:
        assert shard["total_bytes"] <= 900
        for p in shard["partitions"]:
            assert p["entity"] in shard["entities"]


def test_plan_404(client):
    assert client.get("/plans/999").status_code == 404
    assert client.get("/plans/999/export.json").status_code == 404


def test_index_lists_plans(client, db):
    post_plan(client, FEASIBLE_MANIFEST)
    page = client.get("/")
    html = page.get_data(as_text=True)
    assert "已发布计划（1）" in html
