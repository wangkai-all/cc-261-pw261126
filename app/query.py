"""查询解析与确定性分页。

关键问题：用户的排序键可能重复（状态、地区、金额都可能撞值），聚合查询更没有
"最后一行唯一键" 可用。因此续传游标绝不使用业务值，而是用窗口函数
ROW_NUMBER() OVER (用户的 ORDER BY ...) 生成确定性绝对行号 _wb_rn。
同一快照上重放 ORDER BY + ROW_NUMBER 的结果是确定的，行号即可作为唯一游标。
"""
import re

# 只允许纯只读查询；出现写操作/事务控制一律拒绝
_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|REPLACE|CREATE|DROP|ALTER|ATTACH|DETACH|"
    r"PRAGMA|VACUUM|REINDEX|GRANT|REVOKE|BEGIN|COMMIT|ROLLBACK|SAVEPOINT|"
    r"RELEASE)\b",
    re.IGNORECASE,
)


class QueryError(ValueError):
    """用户查询不合法（400 级别的错误）。"""


def _tokenize_top_level(sql):
    """扫描 SQL，返回顶层（括号深度为 0 处）关键字子句的 (起始, 结束, 关键字)。

    正确处理字符串、引号标识符、方括号标识符和行/块注释。
    """
    clauses = []
    depth = 0
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c == "'":  # 字符串字面量，'' 为转义
            i += 1
            while i < n:
                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        if c == '"':  # 双引号标识符
            i += 1
            while i < n and sql[i] != '"':
                i += 1
            i += 1
            continue
        if c == "`":
            i += 1
            while i < n and sql[i] != "`":
                i += 1
            i += 1
            continue
        if c == "[":
            i += 1
            while i < n and sql[i] != "]":
                i += 1
            i += 1
            continue
        if c == "-" and i + 1 < n and sql[i + 1] == "-":  # 行注释
            i += 2
            while i < n and sql[i] != "\n":
                i += 1
            continue
        if c == "/" and i + 1 < n and sql[i + 1] == "*":  # 块注释
            i += 2
            while i + 1 < n and not (sql[i] == "*" and sql[i + 1] == "/"):
                i += 1
            i += 2
            continue
        if c == "(":
            depth += 1
            i += 1
            continue
        if c == ")":
            depth -= 1
            i += 1
            continue
        if depth == 0 and (c.isalpha() or c == "_"):
            m = re.match(r"[A-Za-z_][A-Za-z_0-9]*", sql[i:])
            word = m.group(0)
            upper = word.upper()
            if upper in {"SELECT", "FROM", "WHERE", "GROUP", "HAVING",
                         "ORDER", "BY", "LIMIT", "OFFSET", "UNION",
                         "INTERSECT", "EXCEPT", "WINDOW", "VALUES", "WITH"}:
                clauses.append((i, i + len(word), upper))
            i += len(word)
            continue
        i += 1
    return clauses


def parse_query(sql):
    """校验并拆分用户查询。

    返回 dict：
      core:  去掉顶层尾部 ORDER BY / LIMIT 之后的查询主体
      order: 顶层 ORDER BY 的排序表达式原文（不含 ORDER BY 关键字）
      tail:  顶层尾部 LIMIT/OFFSET 原文（可能为空），分页重算时保持其语义
    强制要求显式 ORDER BY —— 没有稳定顺序就没有可恢复的导出。
    """
    if not sql or not sql.strip():
        raise QueryError("查询为空")
    if ";" in sql.rstrip().rstrip(";"):
        raise QueryError("只允许单条 SELECT 语句（检测到多余的分号）")
    cleaned = sql.strip().rstrip(";").strip()
    if _FORBIDDEN.search(cleaned):
        raise QueryError("只允许只读 SELECT 查询，检测到写操作或事务控制关键字")

    clauses = _tokenize_top_level(cleaned)
    if not clauses or clauses[0][2] not in {"SELECT", "WITH"}:
        raise QueryError("查询必须以 SELECT 或 WITH 开头")

    top = [c for c in clauses]  # 全部为顶层子句（扫描时已按括号深度过滤）
    order_starts = [k for k in top if k[2] == "ORDER"]
    if not order_starts:
        raise QueryError("导出查询必须包含显式 ORDER BY，否则断线后无法保证顺序一致")
    if len(order_starts) > 1:
        raise QueryError("顶层存在多个 ORDER BY，请调整查询写法")

    order_kw = order_starts[-1]
    # ORDER 与 BY 是两个关键字 token，排序表达式从 BY 之后开始；
    # 再往后若还有顶层子句（预期只有 LIMIT），表达式在其前面结束。
    later = [k for k in top if k[0] > order_kw[0]]
    if not later or later[0][2] != "BY":
        raise QueryError("ORDER 后缺少 BY")
    by_token = later[0]
    following = later[1:]
    order_end = following[0][0] if following else len(cleaned)
    order_text = cleaned[by_token[1]:order_end].strip()
    if not order_text:
        raise QueryError("ORDER BY 后缺少排序表达式")

    limit_kw = next((k for k in following if k[2] == "LIMIT"), None)
    if limit_kw is not None:
        # LIMIT 可能出现在 ORDER BY 之前（语法不允许，防御一下）
        if limit_kw[0] < order_kw[0]:
            raise QueryError("不支持的 LIMIT 位置")
        tail = cleaned[limit_kw[0]:].strip()
    else:
        tail = ""
    core = cleaned[:order_kw[0]].strip()
    return {"core": core, "order": order_text, "tail": tail}


def extract_limit_offset(tail):
    """解析顶层 LIMIT/OFFSET 原文，返回 (limit|None, offset)。支持逗号写法。"""
    if not tail:
        return None, 0
    nums = [int(x) for x in re.findall(r"\b\d+\b", tail)]
    if "OFFSET" in tail.upper():
        limit = nums[0]
        offset = nums[1] if len(nums) > 1 else 0
    elif len(nums) == 2:  # LIMIT offset, count 形式
        offset, limit = nums
    else:
        limit = nums[0] if nums else None
        offset = 0
    return limit, offset


def window_sql(parsed):
    """包装成带确定性行号的派生表（覆盖 core 的全部行）。"""
    return (
        "SELECT * FROM ("
        "SELECT base.*, ROW_NUMBER() OVER (ORDER BY "
        f"{parsed['order']}) AS _wb_rn "
        f"FROM ({parsed['core']}) AS base"
        ") AS wb_window"
    )


def _limit_filter(parsed):
    """把用户的 LIMIT/OFFSET 转成对窗口行号的 WHERE 过滤（行号从 1 开始）。"""
    limit, offset = extract_limit_offset(parsed["tail"])
    clauses = [f"_wb_rn > {int(offset)}"]
    if limit is not None:
        clauses.append(f"_wb_rn <= {int(offset + limit)}")
    return " AND ".join(clauses)


def effective_sql(parsed):
    """用户查询的「有效结果集」：已应用 LIMIT/OFFSET，按确定性行号排序。

    所有取数入口（流式产出、分页重算、计数、校验和）都走这一层，
    保证 result 与 snapshot 两种模式、带不带 LIMIT 都产出同一份字节。
    """
    return (
        f"SELECT * FROM ({window_sql(parsed)}) AS wb_eff0 "
        f"WHERE {_limit_filter(parsed)} ORDER BY _wb_rn"
    )


def page_sql(parsed, limit, offset):
    """快照模式分页重算：在有效结果集上按相对位置取一页。"""
    return (
        f"SELECT * FROM ({effective_sql(parsed)}) AS wb_eff "
        f"ORDER BY _wb_rn LIMIT {int(limit)} OFFSET {int(offset)}"
    )


def count_sql(parsed):
    """有效结果集的总行数（用户 LIMIT/OFFSET 已在 WHERE 中生效）。"""
    return f"SELECT COUNT(*) AS c FROM ({effective_sql(parsed)}) AS wb_count"


def full_stream_sql(parsed):
    """结果模式单次流式执行的查询。

    与分页重算走完全相同的有效结果集，保证两种保留策略字节级一致；
    最后一列 _wb_rn 由调用方剔除，不写入 CSV。
    """
    return effective_sql(parsed)
