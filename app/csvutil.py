"""CSV 行编码的唯一入口：客户端与服务端必须用同一种规范，才能逐字节校对。"""
import csv
import io


def encode_rows(columns, rows):
    """把若干行编码为 CSV 文本（含 CRLF 行结束，RFC4180 风格）。

    columns: 列名列表；rows: 与 columns 等长的 tuple/list 序列。
    """
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\r\n")
    for row in rows:
        writer.writerow(["" if v is None else v for v in row])
    return buf.getvalue()


def encode_header(columns):
    buf = io.StringIO(newline="")
    csv.writer(buf, lineterminator="\r\n").writerow(columns)
    return buf.getvalue()
