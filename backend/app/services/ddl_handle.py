"""DDL适配服务：DDL变更成功后级联影响解析规则和Schema演进。"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.column_evolution import split_column_change
from app.store import store


class DdlHandleService(BaseModuleService):
    MODULE = "ddl_handle"
    ENTITY = "DDL适配"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["适配编号", "源端", "DDL类型", "影响表", "兼容策略", "自动应用", "人工确认", "处理状态"]
    REQUIRED_FIELDS = ["适配编号", "源端", "DDL类型"]
    STATUS_ORDER = ["待处理", "适配中", "已应用", "需人工"]
    ACTION_RULES = {"自动应用": "适配中", "人工确认": "已应用", "忽略变更": "需人工"}
    NEGATIVE_ACTIONS = ["忽略变更"]

    def trigger_binlog_and_schema_evo(self, entry_id: int) -> dict[str, Any] | None:
        """DDL适配成功后，触发binlog解析规则调整和Schema演进记录。

        数据流链路: ddl_handle -> binlog_parse + schema_evo
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "已应用":
            return None

        tag = str(entry.get("源端", ""))
        table = str(entry.get("影响表", ""))
        ddl_type = str(entry.get("DDL类型", ""))
        results: dict[str, Any] = {"binlog": None, "schema": None}

        # 调整binlog解析规则
        for bp in store.rows("binlog_parse"):
            if str(bp.get("源端", "")) == tag:
                bp["DDL处理"] = ddl_type
                bp["字段映射"] = f"自动适配{ddl_type}@{table}"
                bp["status"] = "解析中"
                bp["pending"] = True
                bp["abnormal"] = False
                results["binlog"] = bp
                break

        # 创建Schema演进记录
        se_rows = store.rows("schema_evo")
        new_se_id = max((int(r.get("id", 0)) for r in se_rows), default=0) + 1
        schema = {
            "id": new_se_id,
            "记录编号": f"SCHE-{new_se_id:04d}",
            "数据流": tag,
            "变更列": f"DDL变更-{ddl_type}",
            "列变更前": "预变更前schema",
            "列变更后": f"应用{ddl_type}后schema",
            "兼容模式": str(entry.get("兼容策略", "向前兼容")),
            "下游影响": f"影响表{table}及下游消费",
            "演进时间": "2026-11-18 10:44:00",
            "status": "已捕获",
            "pending": True,
            "abnormal": False,
        }
        se_rows.append(schema)
        results["schema"] = schema

        return results

    def build_column_evolution_package(self, entry_id: int) -> dict[str, Any] | None:
        """从演进记录的变更前后字段组装列演进数据包，供维表关联核验列型。

        数据流链路: ddl_handle -> schema_evo(列变更前后) + dim_join(关联键出处)

        包内携带演进发生时的源表、列型及关联键字段出处，导入方凭这些证据
        识别看似同名的不同版本列定义。
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        source = str(entry.get("源端", ""))
        table = str(entry.get("影响表", ""))

        # 匹配该DDL适配对应的演进记录：数据流与源端同名、以影响表开头或下游影响点名
        matched: list[dict[str, Any]] = []
        for se in store.rows("schema_evo"):
            flow = str(se.get("数据流", ""))
            impact = str(se.get("下游影响", ""))
            if flow == source or (table and (flow.startswith(table) or table in impact)):
                matched.append(se)
        if not matched:
            return None

        flows = {str(se.get("数据流", "")) for se in matched}

        # 关联键字段出处：涉及这些数据流的维表关联配置
        key_origins: list[dict[str, Any]] = []
        for dj in store.rows("dim_join"):
            fact = str(dj.get("事实表", ""))
            dim = str(dj.get("维度表", ""))
            if fact in flows or (table and (fact.startswith(table) or dim == table)):
                key_origins.append({
                    "配置编号": dj.get("配置编号", ""),
                    "事实表": fact,
                    "维度表": dim,
                    "关联键": dj.get("关联键", ""),
                    "字段出处": f"dim_join#{dj.get('配置编号', '')}.关联键",
                })

        columns: list[dict[str, Any]] = []
        for se in matched:
            name = split_column_change(str(se.get("变更列", "")))
            origins = [item["字段出处"] for item in key_origins if item["关联键"] == name]
            columns.append({
                "列名": name,
                "变更前列型": se.get("列变更前", ""),
                "变更后列型": se.get("列变更后", ""),
                "兼容模式": se.get("兼容模式", ""),
                "演进时间": se.get("演进时间", ""),
                "演进记录": se.get("记录编号", ""),
                "数据流": se.get("数据流", ""),
                "源表": table,
                "关联键出处": origins,
            })

        return {
            "包编号": f"CEP-{entry_id:04d}",
            "包类型": "列演进数据包",
            "包版本": 1,
            "生成时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "适配编号": entry.get("适配编号", ""),
            "源端": source,
            "源表": table,
            "列演进": columns,
            "关联键出处": key_origins,
        }