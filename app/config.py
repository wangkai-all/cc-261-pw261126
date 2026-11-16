"""集中配置：路径、TTL、分片/帧大小等。所有数据都放在数据目录下，删掉即可从零重建。"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("WB_DATA_DIR", BASE_DIR / "data"))
SOURCE_DB = DATA_DIR / "source.db"
EXPORTS_DIR = DATA_DIR / "exports"
WEB_DIR = BASE_DIR / "web"

# 两种保留策略的默认保留期限（秒），创建导出时可覆盖（有最小值，防止演示时立刻过期）
DEFAULT_TTL_RESULT = 3600          # 结果分片：占空间大，默认 1 小时
DEFAULT_TTL_SNAPSHOT = 7200        # 输入快照：恢复要重算，给更长保留期
MIN_TTL_SECONDS = 30               # 允许的最小 TTL（测试与演示用）

# 结果模式：每个分片多少行（按行流式写出，不依赖业务键唯一）
SHARD_ROWS = 500
# 传输帧：每帧最多多少数据行（两种模式统一）
FRAME_ROWS = 200
# 快照模式：keyset 分页每批行数
PAGE_ROWS = 500

# 连接保活：下载连接静默多久视为掉线（秒）；客户端应在此周期内 ack
CONNECTION_IDLE_TIMEOUT = float(os.environ.get("WB_IDLE_TIMEOUT", "120"))

# 样例数据规模（rebuild 时可覆盖）
DEFAULT_SOURCE_ROWS = 20000
