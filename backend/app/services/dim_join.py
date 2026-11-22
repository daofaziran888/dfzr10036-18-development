"""维表关联服务：关联补全后推动水位推进和Schema演进检测。"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.column_evolution import (
    classify_change,
    identify_version,
    value_conforms,
)
from app.store import store


class DimJoinService(BaseModuleService):
    MODULE = "dim_join"
    ENTITY = "关联配置"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["配置编号", "事实表", "维度表", "关联键", "关联类型", "缺失策略", "缓存TTL", "命中率"]
    REQUIRED_FIELDS = ["配置编号", "事实表", "维度表"]
    STATUS_ORDER = ["草稿", "关联中", "缓存失效", "已停止"]
    ACTION_RULES = {"启动关联": "关联中", "重建缓存": "缓存失效", "停止关联": "已停止"}
    NEGATIVE_ACTIONS = []

    def trigger_watermark_and_schema(self, entry_id: int) -> dict[str, Any]:
        """维表关联启动后，触发水位推进和Schema演进检测。

        数据流链路: dim_join -> watermark_mgr + schema_evo
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "关联中":
            return None

        tag = str(entry.get("事实表", ""))
        results: dict[str, Any] = {"watermark": None, "schema": None}

        # 水位推进
        wm_rows = store.rows("watermark_mgr")
        has_wm = [r for r in wm_rows if str(r.get("数据流", "")) == tag]
        if has_wm:
            has_wm[0]["status"] = "已追上"
            has_wm[0]["pending"] = True
            has_wm[0]["abnormal"] = False
            has_wm[0]["当前位点"] = "binlog.001234:891200"
            has_wm[0]["提交位点"] = "binlog.001234:891200"
            has_wm[0]["延迟行数"] = 0
            results["watermark"] = has_wm[0]
        else:
            new_wm_id = max((int(r.get("id", 0)) for r in wm_rows), default=0) + 1
            wm_config = {
                "id": new_wm_id,
                "水位编号": f"WATE-{new_wm_id:04d}",
                "数据流": tag,
                "当前位点": "binlog.001234:891200",
                "提交位点": "binlog.001234:891200",
                "延迟行数": 0,
                "水位类型": "事件时间水位",
                "最后推进": "2026-11-18 10:32:15",
                "追赶进度": "100%",
                "status": "已追上",
                "pending": True,
                "abnormal": False,
            }
            wm_rows.append(wm_config)
            results["watermark"] = wm_config

        # Schema演进检测
        se_rows = store.rows("schema_evo")
        new_se_id = max((int(r.get("id", 0)) for r in se_rows), default=0) + 1
        schema_rec = {
            "id": new_se_id,
            "记录编号": f"SCHE-{new_se_id:04d}",
            "数据流": tag,
            "变更列": "customer_tier(enum→varchar)",
            "列变更前": "ENUM('bronze','silver','gold')",
            "列变更后": "VARCHAR(32)",
            "兼容模式": "向前兼容",
            "下游影响": "仅影响实时打标任务",
            "演进时间": "2026-11-18 10:32:20",
            "status": "兼容通过",
            "pending": True,
            "abnormal": False,
        }
        se_rows.append(schema_rec)
        results["schema"] = schema_rec

        return results

    def import_evolution_package(self, entry_id: int, package: dict[str, Any]) -> dict[str, Any] | None:
        """导入列演进数据包，校验本配置的关联输入（在途批次）。

        数据流链路: ddl_handle(列演进数据包) -> dim_join(关联校验) -> schema_evo(采用登记)

        - 用包内源表、列型及关联键出处证据识别看似同名的不同版本；
        - 不兼容的字段不作为已确认连接键；
        - 校验失败时不改动任何已确认的关联结果；
        - 校验成功后向Schema演进登记实际采用的版本与还需验证的列。
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None

        code = str(entry.get("配置编号", ""))
        fact = str(entry.get("事实表", ""))
        key = str(entry.get("关联键", ""))

        def fail(message: str) -> dict[str, Any]:
            # 导入失败后的恢复口径：早已确认的关联结果原样保留，不做回滚
            return {
                "ok": False,
                "message": f"{message}；导入失败，已保留此前确认的关联结果",
                "配置编号": code,
                "保留的确认结果": {
                    "已确认键列": entry.get("已确认键列", []),
                    "采用版本": entry.get("采用版本", {}),
                },
            }

        if not isinstance(package, dict):
            return fail("列演进数据包格式非法")
        columns = package.get("列演进")
        origins = package.get("关联键出处")
        source_table = str(package.get("源表", "") or "")
        if (
            not source_table
            or not isinstance(columns, list)
            or not columns
            or not isinstance(origins, list)
        ):
            return fail("数据包缺少源表、列演进或关联键出处证据")

        evo_by_name = {
            str(col.get("列名", "")): col for col in columns if isinstance(col, dict)
        }
        origin_hit = any(
            isinstance(origin, dict)
            and str(origin.get("配置编号", "")) == code
            and str(origin.get("关联键", "")) == key
            for origin in origins
        )
        if not origin_hit:
            return fail(f"包内缺少{code}关联键「{key}」的字段出处证据")
        if not any(str(col.get("数据流", "")) == fact for col in evo_by_name.values()):
            return fail(f"包内演进证据与关联输入{fact}不匹配")

        confirmed_keys: list[str] = []
        adopted: dict[str, str] = {}
        incompatible: list[dict[str, Any]] = []
        pending_columns: list[str] = []

        def mark_incompatible(col: str, before: str, after: str) -> None:
            if all(item["列名"] != col for item in incompatible):
                incompatible.append({
                    "列名": col,
                    "变更前列型": before,
                    "变更后列型": after,
                    "原因": "列型收窄，不能作为已确认连接键",
                })

        def mark_pending(col: str, reason: str) -> None:
            note = f"{col}（{reason}）"
            if col and note not in pending_columns:
                pending_columns.append(note)

        # 键列兼容性判定：不兼容的字段不能作为已经确认的连接键
        key_evo = evo_by_name.get(key)
        key_grade = "无证据"
        if key_evo is None:
            mark_pending(key, "包内无该列演进证据")
        else:
            key_before = str(key_evo.get("变更前列型", ""))
            key_after = str(key_evo.get("变更后列型", ""))
            key_grade = classify_change(key_before, key_after)
            if key_grade == "兼容":
                adopted[key] = key_after
            elif key_grade == "不兼容":
                mark_incompatible(key, key_before, key_after)
            else:
                mark_pending(key, "列型变更无法自动判定")

        # 在途批次校验：识别列版本、核验行值，圈定可执行范围与暂停行
        executable_scope: list[dict[str, Any]] = []
        paused_rows: list[dict[str, Any]] = []
        key_readable = False
        for batch in entry.get("在途批次", []) or []:
            batch_no = str(batch.get("批次号", "?"))
            defs = batch.get("列定义", {}) or {}
            rows = batch.get("行", []) or []
            col_status: dict[str, dict[str, Any]] = {}
            for col, batch_type in defs.items():
                name = str(col)
                evo = evo_by_name.get(name)
                if evo is None:
                    col_status[name] = {"状态": "无证据", "原因": "包内无该列演进证据"}
                    mark_pending(name, "包内无该列演进证据")
                    continue
                before = str(evo.get("变更前列型", ""))
                after = str(evo.get("变更后列型", ""))
                version = identify_version(str(batch_type), before, after)
                if version is None:
                    col_status[name] = {
                        "状态": "版本无法印证",
                        "原因": f"批次列型{batch_type}与包内{evo.get('演进记录', '')}变更前后版本均不一致",
                    }
                    mark_pending(name, f"批次{batch_no}版本无法印证")
                    continue
                grade = classify_change(before, after)
                if grade == "不兼容":
                    col_status[name] = {"状态": "不兼容", "采用列型": after, "原因": "列型收窄不兼容"}
                    mark_incompatible(name, before, after)
                elif grade == "待验证":
                    col_status[name] = {"状态": "待验证", "采用列型": after, "原因": "列型变更无法自动判定"}
                    mark_pending(name, "列型变更无法自动判定")
                else:
                    col_status[name] = {"状态": "可读取", "采用列型": after, "批次版本": version}
            batch_keys = [key] if col_status.get(key, {}).get("状态") == "可读取" else []
            key_readable = key_readable or bool(batch_keys)
            paused = 0
            for idx, row in enumerate(rows, start=1):
                row_no = row.get("行号", idx)
                for name, status in col_status.items():
                    reason = ""
                    if status["状态"] != "可读取":
                        reason = str(status["原因"])
                    elif name in row and not value_conforms(row[name], status["采用列型"]):
                        reason = f"值「{row[name]}」不满足采用列型{status['采用列型']}"
                    if reason:
                        paused += 1
                        paused_rows.append({
                            "批次号": batch_no,
                            "行号": row_no,
                            "列名": name,
                            "值": row.get(name),
                            "原因": reason,
                        })
                        break
            executable_scope.append({
                "批次号": batch_no,
                "可读取键列": batch_keys,
                "可执行行数": len(rows) - paused,
                "总行数": len(rows),
            })

        # 包证据确认批次可读取的键列：键列兼容且至少一个批次可读（或无在途批次）
        if key_grade == "兼容" and (key_readable or not entry.get("在途批次")):
            confirmed_keys.append(key)

        to_verify = list(pending_columns)
        for item in incompatible:
            note = f"{item['列名']}（列型收窄不兼容）"
            if note not in to_verify:
                to_verify.append(note)

        entry["已确认键列"] = confirmed_keys
        entry["采用版本"] = adopted
        entry["待验证列"] = to_verify
        entry["最近导入包"] = str(package.get("包编号", ""))
        if incompatible:
            entry["abnormal"] = True

        # 向Schema演进登记实际采用的版本与还需验证的列
        se_rows = store.rows("schema_evo")
        new_se_id = max((int(r.get("id", 0)) for r in se_rows), default=0) + 1
        record = {
            "id": new_se_id,
            "记录编号": f"SE-{new_se_id:04d}",
            "数据流": fact,
            "变更列": "、".join(f"{col}(采用{adopted[col]})" for col in confirmed_keys) or "无已确认列",
            "列变更前": "、".join(
                f"{col}={evo_by_name[col].get('变更前列型', '')}"
                for col in confirmed_keys
                if col in evo_by_name
            ) or "-",
            "列变更后": "、".join(f"{col}={adopted[col]}" for col in confirmed_keys) or "-",
            "兼容模式": "向前兼容",
            "下游影响": (
                f"维表关联{code}采用版本已登记；"
                f"待验证列：{'、'.join(to_verify) if to_verify else '无'}"
            ),
            "演进时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "兼容通过" if not to_verify else "存在风险",
            "pending": True,
            "abnormal": bool(to_verify),
        }
        se_rows.append(record)

        return {
            "ok": True,
            "message": f"列演进数据包{package.get('包编号', '')}导入校验完成",
            "配置编号": code,
            "包编号": package.get("包编号", ""),
            "源表": source_table,
            "已确认键列": confirmed_keys,
            "采用版本": adopted,
            "不兼容字段": incompatible,
            "待验证列": to_verify,
            "可执行范围": executable_scope,
            "暂停行": paused_rows,
            "演进登记": record,
        }

    def handle_cache_invalidation(self, entry_id: int) -> dict[str, Any]:
        """缓存失效时，发出告警并级联影响下游延迟指标。

        数据流链路: dim_join(缓存失效) -> alert_rule + lag_monitor
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "缓存失效":
            return None

        tag = str(entry.get("事实表", ""))
        results = {"alert": None, "lag": None}

        # 创建告警
        alert_rows = store.rows("alert_rule")
        new_alert_id = max((int(r.get("id", 0)) for r in alert_rows), default=0) + 1
        alert = {
            "id": new_alert_id,
            "策略编号": f"ALER-{new_alert_id:04d}",
            "监控指标": "维表缓存命中率",
            "告警条件": "命中率 < 60% 持续5min",
            "告警级别": "P2-警告",
            "通知渠道": "飞书群+电话",
            "聚合窗口": "5min",
            "静默时段": "0-6点",
            "触发次数": 3,
            "status": "已触发",
            "pending": True,
            "abnormal": True,
        }
        alert_rows.append(alert)
        results["alert"] = alert

        # 标记延迟
        for lm in store.rows("lag_monitor"):
            if str(lm.get("数据流", "")) == tag and lm.get("status") in ("正常", "采集中"):
                lm["status"] = "延迟偏高"
                lm["pending"] = True
                lm["abnormal"] = True
                lm["延迟秒数"] = 45
                results["lag"] = lm
                break

        return results