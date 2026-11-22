"""规划核心：校验、可行性分类、装箱正确性。"""
import pytest

from shard_planner.planner import (
    CAPACITY_OVERFLOW,
    MAX_ENTITIES,
    NO_FEASIBLE_COMBINATION,
    SINGLE_ENTITY_OVERFLOW,
    ManifestError,
    Partition,
    aggregate_entities,
    build_plan,
    manifest_fingerprint,
    normalize_manifest,
)


def make_manifest(spec):
    """spec: {entity: [bytes, ...]}，每个字节数生成一个分区。"""
    rows = []
    for entity, sizes in spec.items():
        for i, b in enumerate(sizes):
            rows.append({"partition_key": f"{entity}/part-{i}", "entity": entity,
                         "bytes": b, "records": b // 10})
    return rows


def test_feasible_plan_respects_constraints():
    parts = normalize_manifest(make_manifest({"A": [300, 200], "B": [400], "C": [600, 100]}))
    result = build_plan(parts, worker_count=2, worker_byte_limit=900)
    assert result.ok
    assert len(result.shards) == 2
    # 同一实体不拆分
    placement = {}
    for shard_idx, entities in enumerate(result.shards):
        for e in entities:
            assert e not in placement, f"实体 {e} 被拆到多个分片"
            placement[e] = shard_idx
    assert set(placement) == {"A", "B", "C"}
    # 每个分片不超上限
    agg = aggregate_entities(parts)
    for entities in result.shards:
        assert sum(agg[e][0] for e in entities) <= 900


def test_entity_partitions_stay_together():
    parts = normalize_manifest(make_manifest({"A": [100, 100, 100], "B": [250]}))
    result = build_plan(parts, worker_count=2, worker_byte_limit=300)
    assert result.ok
    shard_of_a = [i for i, es in enumerate(result.shards) if "A" in es]
    assert len(shard_of_a) == 1


def test_single_entity_overflow():
    parts = normalize_manifest(make_manifest({"BIG": [900, 200], "small": [100]}))
    result = build_plan(parts, worker_count=4, worker_byte_limit=1000)
    assert not result.ok
    assert result.reason == SINGLE_ENTITY_OVERFLOW
    assert result.detail["entities"] == [{"entity": "BIG", "bytes": 1100}]


def test_capacity_overflow():
    parts = normalize_manifest(make_manifest({"A": [500], "B": [500], "C": [500], "D": [500]}))
    result = build_plan(parts, worker_count=2, worker_byte_limit=900)
    assert not result.ok
    assert result.reason == CAPACITY_OVERFLOW
    assert result.detail["total_bytes"] == 2000
    assert result.detail["total_capacity"] == 1800


def test_no_feasible_combination():
    # 总量 3000 == 3 * 1000，单实体均 <= 1000，但每个分片最多装一个 600
    parts = normalize_manifest(make_manifest({f"E{i}": [600] for i in range(5)}))
    result = build_plan(parts, worker_count=3, worker_byte_limit=1000)
    assert not result.ok
    assert result.reason == NO_FEASIBLE_COMBINATION


def test_exact_fit_backtracking_not_greedy():
    # FFD 会失败的情形：物品 4,4,3,3,2,2，容量 9，2 个分片
    # FFD 装箱 4+4 / 3+3+2 后剩一个 2 无处可放；精确解为 4+3+2 / 4+3+2
    sizes = {"A": [4], "B": [4], "C": [3], "D": [3], "E": [2], "F": [2]}
    parts = normalize_manifest(make_manifest(sizes))
    result = build_plan(parts, worker_count=2, worker_byte_limit=9)
    assert result.ok
    agg = aggregate_entities(parts)
    for entities in result.shards:
        assert sum(agg[e][0] for e in entities) <= 9


def test_empty_shards_are_kept():
    parts = normalize_manifest(make_manifest({"A": [100]}))
    result = build_plan(parts, worker_count=3, worker_byte_limit=500)
    assert result.ok
    assert len(result.shards) == 3
    assert sorted(len(s) for s in result.shards) == [0, 0, 1]


def test_manifest_validation_errors():
    with pytest.raises(ManifestError):
        normalize_manifest([])
    with pytest.raises(ManifestError):
        normalize_manifest("not-a-list")
    with pytest.raises(ManifestError):
        normalize_manifest([{"partition_key": "p", "entity": "e", "bytes": -1, "records": 0}])
    with pytest.raises(ManifestError):
        normalize_manifest([{"partition_key": "p", "entity": "e", "bytes": True, "records": 0}])
    with pytest.raises(ManifestError):
        normalize_manifest([{"partition_key": "", "entity": "e", "bytes": 1, "records": 0}])
    with pytest.raises(ManifestError):
        normalize_manifest([{"partition_key": "p", "entity": "e", "bytes": 1}])  # 缺 records


def test_entity_limit_40():
    rows = make_manifest({f"E{i}": [1] for i in range(MAX_ENTITIES)})
    assert len(normalize_manifest(rows)) == MAX_ENTITIES
    rows = make_manifest({f"E{i}": [1] for i in range(MAX_ENTITIES + 1)})
    with pytest.raises(ManifestError, match="实体数"):
        normalize_manifest(rows)


def test_numeric_strings_accepted():
    parts = normalize_manifest(
        [{"partition_key": "p", "entity": "e", "bytes": "123", "records": "45"}]
    )
    assert parts[0].bytes == 123
    assert parts[0].records == 45


def test_fingerprint_order_insensitive():
    rows = make_manifest({"A": [1, 2], "B": [3]})
    p1 = normalize_manifest(rows)
    p2 = normalize_manifest(list(reversed(rows)))
    assert manifest_fingerprint(p1, 2, 10) == manifest_fingerprint(p2, 2, 10)
    # 参数不同 -> 指纹不同
    assert manifest_fingerprint(p1, 2, 10) != manifest_fingerprint(p1, 3, 10)
    # 清单不同 -> 指纹不同
    p3 = normalize_manifest(make_manifest({"A": [1, 2], "B": [4]}))
    assert manifest_fingerprint(p1, 2, 10) != manifest_fingerprint(p3, 2, 10)


def test_partition_dataclass():
    p = Partition("k", "e", 1, 2)
    assert (p.partition_key, p.entity, p.bytes, p.records) == ("k", "e", 1, 2)
