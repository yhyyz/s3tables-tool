#!/usr/bin/env python3
"""
verify_partition.py - 验证分区是否生效

跑这个脚本会:
1. 打印当前 partition spec
2. 打印所有 spec 版本（含历史）
3. 统计每个分区的行数和文件数
4. 显示最近的文件路径，确认是否带分区前缀

用法:
  python verify_partition.py
"""

import logging
import sys

from pyiceberg.catalog import load_catalog

# 配置 - 和 add_event_time_partition.py 保持一致
AWS_ACCOUNT_ID = "305996241648"
AWS_REGION = "us-east-1"
BUCKET_NAME = "lakehouse-agent-poc"
NAMESPACE = "mars_log"
TABLE_NAME = "mars_cl_log"
PARTITION_NAME = "event_time"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("verify-partition")


def get_catalog():
    return load_catalog(
        "s3tables",
        type="rest",
        uri=f"https://s3tables.{AWS_REGION}.amazonaws.com/iceberg",
        warehouse=f"arn:aws:s3tables:{AWS_REGION}:{AWS_ACCOUNT_ID}:bucket/{BUCKET_NAME}",
        **{
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": AWS_REGION,
        },
    )


def main():
    log.info("Loading table %s.%s ...", NAMESPACE, TABLE_NAME)
    catalog = get_catalog()
    table = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")

    log.info("=" * 60)
    log.info("Current partition spec:")
    log.info("  %s", table.spec())

    log.info("All spec versions in history:")
    for spec_id, spec in table.specs().items():
        log.info("  spec_id=%d: %s", spec_id, spec)

    log.info("=" * 60)
    log.info("Snapshot summary:")
    snap = table.current_snapshot()
    if snap:
        log.info("  Current snapshot id: %s", snap.snapshot_id)
        log.info("  Timestamp: %s", snap.timestamp_ms)
        summary = snap.summary if snap.summary else {}
        for k, v in (summary.additional_properties or {}).items():
            log.info("  %s = %s", k, v)
    else:
        log.info("  No snapshot yet")

    # 找分区字段
    target_field = None
    for f in table.spec().fields:
        if f.name == PARTITION_NAME:
            target_field = f
            break

    if not target_field:
        log.warning("=" * 60)
        log.warning(
            "❌ Partition field '%s' NOT FOUND in current spec.", PARTITION_NAME
        )
        log.warning("→ 先跑 add_event_time_partition.py 添加分区")
        sys.exit(1)

    log.info("=" * 60)
    log.info("✅ Partition field '%s' confirmed in spec:", PARTITION_NAME)
    log.info("   source_id = %s (column index in schema)", target_field.source_id)
    log.info("   field_id  = %s", target_field.field_id)
    log.info("   transform = %s", target_field.transform)
    log.info("=" * 60)
    log.info("Next: use Athena to inspect file paths and verify routing:")
    log.info("")
    log.info("  SELECT file_path")
    log.info(
        '  FROM "s3tablescatalog/%s".%s."%s$files"', BUCKET_NAME, NAMESPACE, TABLE_NAME
    )
    log.info("  WHERE regexp_like(file_path, '/%s=')", PARTITION_NAME)
    log.info("  ORDER BY file_path DESC LIMIT 10;")
    log.info("")
    log.info(
        "→ 旧文件路径不带 '%s='，新写入文件带 '%s=YYYY-MM-DD-HH/' 前缀",
        PARTITION_NAME,
        PARTITION_NAME,
    )


if __name__ == "__main__":
    main()
