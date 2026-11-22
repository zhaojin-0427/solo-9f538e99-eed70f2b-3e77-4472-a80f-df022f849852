# 数据湖分片规划（Shard Planner）

Flask + SQLite 的分片规划网页：数据工程师上传 Parquet 分区清单（最多 40 个实体），
填写 worker 数与每个 worker 的字节上限，系统生成可交给并行作业的分片计划。

## 功能

- **上传清单**：粘贴 JSON 或上传 `.json` / `.csv` 文件（CSV 表头 `partition_key,entity,bytes,records`）。
- **约束**：同一实体的所有分区必须落在同一分片；每个分片总字节数 ≤ 单 worker 上限。
- **只在可行时发布**：
  - 单实体超限（某实体总字节 > 单 worker 上限）→ 说明后不发布；
  - 分片超限（总字节 > worker 数 × 单 worker 上限）→ 说明后不发布；
  - 无可行组合（总量放得下但装箱无解）→ 说明后不发布；
  - 失败时不落库、不发布部分计划。
- **幂等**：同一清单（含参数，行序无关）重试返回原计划；新清单生成新计划，已发布计划只读不改写。
- **展示**：计划页列出每个分片的实体、文件（分区键）、字节与记录数及容量占比；`export.json` 可下载交给并行作业。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

SQLite 数据落在命名卷 `planner_data`（容器内 `/data/plans.db`），重启不丢。

## 本地开发

```bash
pip install -r requirements.txt -r requirements-dev.txt
python wsgi.py            # http://localhost:8000
python -m pytest          # 运行测试
```

数据库路径用环境变量 `PLANNER_DB` 覆盖（默认 Flask instance 目录下的 `plans.db`）；
生产部署请设置 `SECRET_KEY`。

## 清单格式

```json
[
  {"partition_key": "dt=2026-01-01/hour=00", "entity": "orders", "bytes": 734003200, "records": 1200000}
]
```

- `partition_key`：非空字符串（分区/文件标识）
- `entity`：非空字符串，同一清单内实体数 ≤ 40
- `bytes` / `records`：非负整数（数字字符串也可）

## 并行作业消费方式

`GET /plans/<id>/export.json` 返回：

```json
{
  "plan_id": 1,
  "worker_count": 2,
  "worker_byte_limit": 1200000000,
  "shards": [
    {"shard_index": 0, "total_bytes": 1136656384, "total_records": 1840000,
     "entities": ["orders"],
     "partitions": [{"partition_key": "dt=2026-01-01/hour=00", "entity": "orders", "bytes": 734003200, "records": 1200000}]}
  ]
}
```

第 i 个并行任务读取 `shards[i]` 的 `partitions` 即可。

## 实现说明

- 装箱求解：先走 First-Fit-Decreasing 快速路径；失败时用带回溯的精确求解
  （对称去重 + 恰好装满剪枝），保证“有解必发布、无解才报无可行组合”。
  求解设有节点预算（200 万），病态输入下报“求解超时”而不是误判或卡死。
- 幂等键：规范化清单（排序后）+ worker 数 + 字节上限的 SHA-256，
  数据库唯一约束兜底并发重复提交。
- 计划只 INSERT 不 UPDATE，已发布计划天然不可变。

## 已知限制

- 装箱为 NP-hard：实体数 ≤ 40 且设有节点预算，极端病态输入可能返回“求解超时”。
- 字节上限按字节数输入（如 1 GiB 填 1073741824），暂不支持单位后缀。
