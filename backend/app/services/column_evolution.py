"""列演进数据包共享工具：列型解析、兼容分级、版本识别和行值核验。

DDL处理导出列演进数据包、维表关联导入校验共用这里同一套列型判读逻辑，
保证两边对"看似同名的不同版本"有一致的证据口径。
"""
from __future__ import annotations

import re
from typing import Any

# 放宽式变更：旧版本的值在新列型下仍然可读，判定为兼容
_WIDENING: dict[str, set[str]] = {
    "ENUM": {"VARCHAR", "CHAR", "TEXT"},
    "CHAR": {"VARCHAR", "TEXT"},
    "VARCHAR": {"TEXT"},
    "INT": {"BIGINT"},
    "FLOAT": {"DOUBLE"},
    "DATE": {"DATETIME"},
}

# 收窄式变更：旧版本的值可能放不进新列型，判定为不兼容
_NARROWING: set[tuple[str, str]] = {
    ("VARCHAR", "ENUM"),
    ("VARCHAR", "CHAR"),
    ("TEXT", "VARCHAR"),
    ("TEXT", "ENUM"),
    ("CHAR", "ENUM"),
    ("BIGINT", "INT"),
    ("DOUBLE", "FLOAT"),
    ("DATETIME", "DATE"),
}


def split_column_change(change_desc: str) -> str:
    """从「customer_tier(ENUM→VARCHAR)」这类变更列描述里取出列名。"""
    text = str(change_desc or "").strip()
    name, _, _ = text.partition("(")
    return name.strip() or text


def _split_enum_values(params: str) -> list[str]:
    values: list[str] = []
    for raw in params.split(","):
        item = raw.strip().strip("'").strip('"').strip()
        if item:
            values.append(item)
    return values


def parse_column_type(type_text: str) -> dict[str, Any]:
    """把列型字符串解析成可比较的结构；无法识别的部分原样保留。"""
    text = str(type_text or "").strip()
    base, _, rest = text.partition("(")
    base = base.strip().upper()
    params = rest.rstrip(")").strip() if rest else ""
    parsed: dict[str, Any] = {"base": base, "raw": text}
    if base == "ENUM":
        parsed["values"] = _split_enum_values(params)
    elif base in {"VARCHAR", "CHAR"}:
        parsed["length"] = int(params) if params.isdigit() else None
    return parsed


def normalize_type(type_text: str) -> str:
    """比较两个列型是否同一版本时使用的归一化形式（忽略空白差异）。"""
    return re.sub(r"\s+", "", str(type_text or ""))


def identify_version(batch_type: str, before: str, after: str) -> str | None:
    """用包内变更前后列型证据，识别批次携带的列定义属于哪个版本。

    看似同名的列可能来自不同版本，只有与演进记录变更前/后列型完全一致
    才算被证据印证；都匹配不上时返回 None，调用方按"版本无法印证"处理。
    """
    text = normalize_type(batch_type)
    if not text:
        return None
    if text == normalize_type(after):
        return "变更后"
    if text == normalize_type(before):
        return "变更前"
    return None


def classify_change(before: str, after: str) -> str:
    """变更前后列型分级：兼容(不变/放宽) / 不兼容(收窄) / 待验证(无法判定)。"""
    if not str(before or "").strip() or not str(after or "").strip():
        return "待验证"
    if normalize_type(before) == normalize_type(after):
        return "兼容"
    old, new = parse_column_type(before), parse_column_type(after)
    old_base, new_base = old["base"], new["base"]
    if not old_base or not new_base:
        return "待验证"
    if old_base == new_base:
        if old_base in {"VARCHAR", "CHAR"}:
            old_len, new_len = old.get("length"), new.get("length")
            if old_len is None or new_len is None:
                return "待验证"
            return "兼容" if new_len >= old_len else "不兼容"
        if old_base == "ENUM":
            old_values = set(old.get("values", []))
            new_values = set(new.get("values", []))
            return "兼容" if old_values <= new_values else "不兼容"
        return "待验证"
    if new_base in _WIDENING.get(old_base, set()):
        return "兼容"
    if (old_base, new_base) in _NARROWING:
        return "不兼容"
    return "待验证"


def value_conforms(value: Any, type_text: str) -> bool:
    """核验行值在给定列型下是否可读；未识别的类型不做行级拦截。"""
    parsed = parse_column_type(type_text)
    base = parsed["base"]
    if base == "ENUM":
        return str(value) in parsed.get("values", [])
    if base in {"VARCHAR", "CHAR"}:
        length = parsed.get("length")
        return length is None or len(str(value)) <= length
    if base in {"TINYINT", "SMALLINT", "INT", "BIGINT"}:
        try:
            int(str(value).strip())
        except (TypeError, ValueError):
            return False
        return True
    return True
