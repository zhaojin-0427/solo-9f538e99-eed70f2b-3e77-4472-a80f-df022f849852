"""核心规划逻辑：清单校验、可行性判定、按实体装箱求解。

约束：同一实体的所有分区必须落在同一分片；每个分片总字节数不得超过
单个 worker 的字节上限。只有存在可行分配时才产出计划。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Optional

MAX_ENTITIES = 40
MAX_PARTITIONS = 20000
MAX_WORKERS = 512
MAX_KEY_LEN = 512
MAX_ENTITY_LEN = 256

# 失败原因码
SINGLE_ENTITY_OVERFLOW = "single_entity_overflow"   # 单实体超限
CAPACITY_OVERFLOW = "capacity_overflow"             # 分片超限（总容量不足）
NO_FEASIBLE_COMBINATION = "no_feasible_combination"  # 无可行组合
SOLVER_LIMIT = "solver_limit"                       # 求解步数超限（无法判定）

_NODE_BUDGET = 2_000_000  # 回溯求解的节点预算，防止病态输入拖垮服务


class ManifestError(ValueError):
    """清单内容不合法。"""


class _SolverLimit(Exception):
    """回溯求解超出节点预算。"""


@dataclass(frozen=True)
class Partition:
    partition_key: str
    entity: str
    bytes: int
    records: int


@dataclass
class PlanResult:
    ok: bool
    reason: Optional[str] = None
    detail: Optional[dict] = None
    shards: Optional[list] = None  # 每个分片的实体名列表（按分片下标）


def _as_non_negative_int(value, field: str, row_no: int) -> int:
    if isinstance(value, bool):
        raise ManifestError(f"第 {row_no} 行：字段 {field} 必须是整数，得到 {value!r}")
    if isinstance(value, int):
        n = value
    elif isinstance(value, str) and value.strip().lstrip("-").isdigit():
        n = int(value.strip())
    else:
        raise ManifestError(f"第 {row_no} 行：字段 {field} 必须是非负整数，得到 {value!r}")
    if n < 0:
        raise ManifestError(f"第 {row_no} 行：字段 {field} 不能为负数（{n}）")
    return n


def _as_non_empty_str(value, field: str, row_no: int, max_len: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ManifestError(f"第 {row_no} 行：字段 {field} 必须是非空字符串，得到 {value!r}")
    text = value.strip()
    if len(text) > max_len:
        raise ManifestError(f"第 {row_no} 行：字段 {field} 长度超过 {max_len}")
    return text


def normalize_manifest(rows) -> list[Partition]:
    """校验并规范化清单行，返回 Partition 列表。不合法时抛 ManifestError。"""
    if not isinstance(rows, list) or not rows:
        raise ManifestError("清单必须是非空的 JSON 数组，每行一个分区")
    if len(rows) > MAX_PARTITIONS:
        raise ManifestError(f"分区行数 {len(rows)} 超过上限 {MAX_PARTITIONS}")

    parts: list[Partition] = []
    entities: set[str] = set()
    for i, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise ManifestError(f"第 {i} 行：必须是对象，得到 {row!r}")
        key = _as_non_empty_str(row.get("partition_key"), "partition_key", i, MAX_KEY_LEN)
        entity = _as_non_empty_str(row.get("entity"), "entity", i, MAX_ENTITY_LEN)
        nbytes = _as_non_negative_int(row.get("bytes"), "bytes", i)
        nrecords = _as_non_negative_int(row.get("records"), "records", i)
        entities.add(entity)
        parts.append(Partition(key, entity, nbytes, nrecords))

    if len(entities) > MAX_ENTITIES:
        raise ManifestError(f"实体数 {len(entities)} 超过上限 {MAX_ENTITIES}")
    return parts


def manifest_fingerprint(partitions: list[Partition], worker_count: int, worker_byte_limit: int) -> str:
    """对规范化后的清单 + 参数计算稳定指纹。

    分区按 (entity, partition_key, bytes, records) 排序后哈希，
    因此同一清单即使行顺序不同也会得到同一指纹（重试返回原计划）。
    """
    canon = {
        "worker_count": worker_count,
        "worker_byte_limit": worker_byte_limit,
        "partitions": sorted(
            (
                {
                    "partition_key": p.partition_key,
                    "entity": p.entity,
                    "bytes": p.bytes,
                    "records": p.records,
                }
                for p in partitions
            ),
            key=lambda d: (d["entity"], d["partition_key"], d["bytes"], d["records"]),
        ),
    }
    blob = json.dumps(canon, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def aggregate_entities(partitions: list[Partition]) -> dict[str, list[int]]:
    """按实体聚合，返回 {entity: [bytes, records]}（按实体名排序，保证确定性）。"""
    agg: dict[str, list[int]] = {}
    for p in partitions:
        slot = agg.setdefault(p.entity, [0, 0])
        slot[0] += p.bytes
        slot[1] += p.records
    return dict(sorted(agg.items()))


def _ffd(sizes: list[int], order: list[int], remaining: list[int], bins: list[list[int]]) -> bool:
    """First-Fit-Decreasing 快速路径：能装下即成功。"""
    for idx in order:
        s = sizes[idx]
        best = -1
        for b in range(len(remaining)):
            if remaining[b] >= s and (best == -1 or remaining[b] < remaining[best]):
                best = b
        if best == -1:
            return False
        remaining[best] -= s
        bins[best].append(idx)
    return True


def _backtrack(sizes: list[int], order: list[int], remaining: list[int], bins: list[list[int]]) -> bool:
    """精确回溯装箱。

    剪枝（均可证明不丢解）：
    - 相同剩余容量的分片只试一个（对称去重）；
    - 若某分片剩余容量恰好等于当前实体大小且递归失败，则当前状态无解
      （交换论证：任何解都可改写为把该实体放进恰好装满的分片）。
    """
    nodes = [0]

    def rec(k: int) -> bool:
        nodes[0] += 1
        if nodes[0] > _NODE_BUDGET:
            raise _SolverLimit
        if k == len(order):
            return True
        idx = order[k]
        s = sizes[idx]
        tried: set[int] = set()
        # best-fit 顺序：优先试剩余空间刚好够的分片
        for b in sorted(range(len(remaining)), key=lambda b: remaining[b]):
            r = remaining[b]
            if r < s or r in tried:
                continue
            tried.add(r)
            remaining[b] = r - s
            bins[b].append(idx)
            if rec(k + 1):
                return True
            bins[b].pop()
            remaining[b] = r
            if r == s:
                return False
        return False

    return rec(0)


def _pack(sizes: list[int], num_bins: int, capacity: int) -> Optional[list[list[int]]]:
    """把 sizes 装进 num_bins 个容量为 capacity 的分片，返回每个分片的下标列表；无解返回 None。"""
    order = sorted(range(len(sizes)), key=lambda i: (-sizes[i], i))
    remaining = [capacity] * num_bins
    bins: list[list[int]] = [[] for _ in range(num_bins)]
    if _ffd(sizes, order, remaining, bins):
        return bins
    remaining = [capacity] * num_bins
    bins = [[] for _ in range(num_bins)]
    if _backtrack(sizes, order, remaining, bins):
        return bins
    return None


def build_plan(partitions: list[Partition], worker_count: int, worker_byte_limit: int) -> PlanResult:
    """判定可行性并给出分片方案。

    失败原因按确定性顺序判定：单实体超限 → 分片超限（总容量不足）→ 无可行组合。
    """
    entity_agg = aggregate_entities(partitions)

    too_big = {e: b for e, (b, _r) in entity_agg.items() if b > worker_byte_limit}
    if too_big:
        return PlanResult(
            False,
            SINGLE_ENTITY_OVERFLOW,
            {
                "entities": [{"entity": e, "bytes": b} for e, b in sorted(too_big.items())],
                "worker_byte_limit": worker_byte_limit,
            },
        )

    total_bytes = sum(b for b, _r in entity_agg.values())
    total_capacity = worker_count * worker_byte_limit
    if total_bytes > total_capacity:
        return PlanResult(
            False,
            CAPACITY_OVERFLOW,
            {
                "total_bytes": total_bytes,
                "worker_count": worker_count,
                "worker_byte_limit": worker_byte_limit,
                "total_capacity": total_capacity,
            },
        )

    names = sorted(entity_agg)
    sizes = [entity_agg[n][0] for n in names]
    try:
        assignment = _pack(sizes, worker_count, worker_byte_limit)
    except _SolverLimit:
        return PlanResult(False, SOLVER_LIMIT, {"node_budget": _NODE_BUDGET})
    if assignment is None:
        return PlanResult(
            False,
            NO_FEASIBLE_COMBINATION,
            {
                "entity_count": len(names),
                "worker_count": worker_count,
                "worker_byte_limit": worker_byte_limit,
                "total_bytes": total_bytes,
            },
        )
    shards = [[names[i] for i in bin_indices] for bin_indices in assignment]
    return PlanResult(True, shards=shards)
