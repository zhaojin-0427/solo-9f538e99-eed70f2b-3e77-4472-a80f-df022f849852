"""HTTP 路由：上传清单、发布/复用计划、查看与导出。"""
from __future__ import annotations

import csv
import io
import json
import sqlite3

from flask import (
    Blueprint,
    Response,
    abort,
    flash,
    redirect,
    render_template,
    request,
    url_for,
)

from . import store
from .db import get_db
from .planner import (
    CAPACITY_OVERFLOW,
    MAX_ENTITIES,
    MAX_WORKERS,
    NO_FEASIBLE_COMBINATION,
    SINGLE_ENTITY_OVERFLOW,
    SOLVER_LIMIT,
    ManifestError,
    build_plan,
    manifest_fingerprint,
    normalize_manifest,
)

bp = Blueprint("planner", __name__)

REQUIRED_CSV_COLUMNS = {"partition_key", "entity", "bytes", "records"}


def _parse_csv(text: str):
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise ManifestError("CSV 内容为空")
    fieldnames = {name.strip() for name in reader.fieldnames if name}
    missing = REQUIRED_CSV_COLUMNS - fieldnames
    if missing:
        raise ManifestError(
            "CSV 缺少必需列：%s（需要 partition_key,entity,bytes,records）" % ", ".join(sorted(missing))
        )
    return normalize_manifest(list(reader))


def _parse_manifest_from_request():
    uploaded = request.files.get("manifest_file")
    if uploaded is not None and uploaded.filename:
        raw = uploaded.read()
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            raise ManifestError("上传文件必须是 UTF-8 编码") from None
        if uploaded.filename.lower().endswith(".csv"):
            return _parse_csv(text)
    else:
        text = (request.form.get("manifest_text") or "").strip()
        if not text:
            raise ManifestError("请粘贴 JSON 清单或上传 JSON/CSV 文件")

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        # 允许直接把 CSV 贴进文本框
        return _parse_csv(text)
    return normalize_manifest(data)


def _parse_int_field(name: str, minimum: int, maximum: int | None, label: str) -> int:
    raw = (request.form.get(name) or "").strip()
    try:
        value = int(raw)
    except ValueError:
        raise ManifestError(f"{label}必须是整数") from None
    if value < minimum:
        raise ManifestError(f"{label}不能小于 {minimum}")
    if maximum is not None and value > maximum:
        raise ManifestError(f"{label}不能超过 {maximum}")
    return value


def _reason_message(reason: str, detail: dict) -> str:
    if reason == SINGLE_ENTITY_OVERFLOW:
        names = "、".join(f"{e['entity']}（{e['bytes']} 字节）" for e in detail["entities"])
        return (
            f"单实体超限：实体 {names} 超过单个 worker 字节上限 "
            f"{detail['worker_byte_limit']}。同一实体必须放在同一分片、不可拆分，未发布计划。"
        )
    if reason == CAPACITY_OVERFLOW:
        return (
            f"分片超限：清单总字节 {detail['total_bytes']} 超过 "
            f"{detail['worker_count']} 个 worker 的总容量 {detail['total_capacity']}"
            f"（每 worker 上限 {detail['worker_byte_limit']}），未发布计划。"
        )
    if reason == NO_FEASIBLE_COMBINATION:
        return (
            "无可行组合：总字节在总容量之内，但受“同一实体不可拆分”约束，"
            "找不到让每个分片都不超限的分配方案，未发布计划。"
        )
    if reason == SOLVER_LIMIT:
        return "求解超时：未能在限定步数内判定可行性，未发布计划。"
    return "无法生成计划，未发布。"


def _render_index(error: str | None = None, status: int = 200):
    plans = store.list_plans(get_db())
    return (
        render_template(
            "index.html",
            plans=plans,
            error=error,
            max_entities=MAX_ENTITIES,
            form={
                "manifest_text": request.form.get("manifest_text", ""),
                "worker_count": request.form.get("worker_count", ""),
                "worker_byte_limit": request.form.get("worker_byte_limit", ""),
            },
        ),
        status,
    )


@bp.get("/")
def index():
    return _render_index()


@bp.post("/plans")
def create_plan():
    try:
        partitions = _parse_manifest_from_request()
        worker_count = _parse_int_field("worker_count", 1, MAX_WORKERS, "worker 数")
        worker_byte_limit = _parse_int_field("worker_byte_limit", 1, None, "每个 worker 的字节上限")
    except ManifestError as exc:
        return _render_index(error=str(exc), status=400)

    fingerprint = manifest_fingerprint(partitions, worker_count, worker_byte_limit)
    db = get_db()

    # 同一清单（含参数）重试：直接返回原计划
    existing = store.find_plan_by_hash(db, fingerprint)
    if existing is not None:
        flash(f"相同清单与参数已发布过，返回原计划 #{existing['id']}。", "info")
        return redirect(url_for("planner.view_plan", plan_id=existing["id"]))

    result = build_plan(partitions, worker_count, worker_byte_limit)
    if not result.ok:
        return _render_index(error=_reason_message(result.reason, result.detail or {}), status=422)

    try:
        plan_id = store.create_plan(
            db, fingerprint, worker_count, worker_byte_limit, partitions, result.shards
        )
    except sqlite3.IntegrityError:
        # 并发下另一个请求已发布同一清单：返回原计划
        existing = store.find_plan_by_hash(db, fingerprint)
        flash(f"相同清单与参数已发布过，返回原计划 #{existing['id']}。", "info")
        return redirect(url_for("planner.view_plan", plan_id=existing["id"]))

    flash(f"计划 #{plan_id} 已发布。", "success")
    return redirect(url_for("planner.view_plan", plan_id=plan_id))


@bp.get("/plans/<int:plan_id>")
def view_plan(plan_id: int):
    detail = store.get_plan_detail(get_db(), plan_id)
    if detail is None:
        abort(404)
    return render_template("plan.html", **detail)


@bp.get("/plans/<int:plan_id>/export.json")
def export_plan(plan_id: int):
    detail = store.get_plan_detail(get_db(), plan_id)
    if detail is None:
        abort(404)
    plan = detail["plan"]
    payload = {
        "plan_id": plan["id"],
        "created_at": plan["created_at"],
        "worker_count": plan["worker_count"],
        "worker_byte_limit": plan["worker_byte_limit"],
        "total_bytes": plan["total_bytes"],
        "total_records": plan["total_records"],
        "shards": [
            {
                "shard_index": shard["row"]["shard_index"],
                "total_bytes": shard["row"]["total_bytes"],
                "total_records": shard["row"]["total_records"],
                "entities": [e["entity"] for e in shard["entities"]],
                "partitions": [
                    {
                        "partition_key": p["partition_key"],
                        "entity": p["entity"],
                        "bytes": p["bytes"],
                        "records": p["records"],
                    }
                    for p in shard["partitions"]
                ],
            }
            for shard in detail["shards"]
        ],
    }
    body = json.dumps(payload, ensure_ascii=False, indent=2)
    return Response(
        body,
        mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename=shard-plan-{plan_id}.json"},
    )
