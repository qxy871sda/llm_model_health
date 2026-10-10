"""宿主路径定位、日志时间戳解析与模型尝试失败统计。

⚠️ 私有实现依赖与失效风险说明
--------------------------------
1. **目录依赖**：插件标准数据目录为 ``<宿主根目录>/data/plugins/<插件ID>``，
   本模块据此逐级校验后上溯定位宿主 ``logs/`` 与 ``logs/maisaka_prompt/``。
   宿主改动目录层级会使定位失败（会显式抛错，不会静默读错目录）。
2. **文件名依赖**：宿主主进程日志为 ``logs/app_*.log.jsonl``。插件 Runner 子进程也会在同一目录
   写同名模式的日志，因此扫描时按文件修改时间倒序并对内容做事件匹配，不含模型事件的文件不会贡献数据。
3. **文案依赖**：宿主在模型尝试失败并切换下一个模型时输出
   ``模型 '<名称>' 尝试失败，切换到下一个模型``（``src/llm_models/utils_model.py`` 的 WARNING 日志）。
   本模块据此统计失败次数；宿主修改该文案后需要同步更新此处的匹配规则。
4. **时间戳依赖**：宿主主进程写 ISO-8601（UTC，带 ``Z`` 后缀，例如 ``2026-10-10T10:29:44.613669Z``），
   插件 Runner 写 ``MM-DD HH:MM:SS``（本地时间）。两者都要能解析成本地时间，否则统计窗口会失效。
5. 本模块只读取日志中的模型名称与事件次数，不读取任何聊天内容或用户隐私。
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, List, Optional

import glob
import json
import re

# 宿主模型尝试失败并切换下一个模型时输出的固定文案（私有实现依赖）
FAILURE_EVENT_MARKER = "尝试失败，切换到下一个模型"
FAILURE_MODEL_PATTERN = re.compile(r"模型\s*'([^']+)'\s*尝试失败")
# 宿主日志文件命名模式
LOG_FILE_GLOB = "app_*.log.jsonl"
# 反向读取日志时的单次块大小
READ_BLOCK_BYTES = 1024 * 1024


@dataclass(slots=True, frozen=True)
class FailureEvent:
    """一次「模型尝试失败并切换」事件。"""

    model: str
    occurred_at: Optional[datetime]


@dataclass(slots=True)
class LogFailureScan:
    """日志扫描结果。

    Attributes:
        available: 宿主日志目录是否存在。
        failures: 窗口内的失败事件。
        bytes_read: 本次实际读取的字节数。
        files_scanned: 实际读取过的日志文件数量。
        unparsed_timestamps: 缺少可解析时间戳但已计入的失败记录条数。
        window_complete: 是否读到窗口下界（``True`` 表示窗口内失败数据完整）。
    """

    available: bool = False
    failures: List[FailureEvent] = field(default_factory=list)
    bytes_read: int = 0
    files_scanned: int = 0
    unparsed_timestamps: int = 0
    window_complete: bool = False


class _ByteBudget:
    """跨文件共享的读取预算。"""

    __slots__ = ("remaining", "exhausted")

    def __init__(self, limit: int) -> None:
        self.remaining = max(int(limit), 0)
        self.exhausted = False

    def take(self, size: int) -> int:
        """申请 ``size`` 字节读取额度，返回实际可用额度。"""

        if self.remaining <= 0:
            self.exhausted = True
            return 0
        granted = min(size, self.remaining)
        self.remaining -= granted
        if granted < size:
            self.exhausted = True
        return granted


def resolve_host_root(data_dir: Path, plugin_id: str) -> Path:
    """严格校验标准数据目录布局后返回宿主根目录。

    Args:
        data_dir: ``ctx.paths.data_dir``。
        plugin_id: 当前插件 ID。

    Returns:
        Path: 宿主根目录。

    Raises:
        RuntimeError: 运行时路径不符合 ``<MaiBot>/data/plugins/<plugin_id>`` 标准布局时抛出。
    """

    resolved_dir = data_dir.resolve()
    if (
        resolved_dir.name != plugin_id
        or resolved_dir.parent.name != "plugins"
        or resolved_dir.parent.parent.name != "data"
    ):
        raise RuntimeError(
            "无法从 ctx.paths.data_dir 安全定位宿主日志目录："
            "运行时路径不符合 <MaiBot>/data/plugins/<plugin_id> 标准布局"
        )
    return resolved_dir.parents[2]


def resolve_logs_dir(data_dir: Path, plugin_id: str) -> Path:
    """定位宿主日志目录 ``<宿主根目录>/logs``。"""

    return resolve_host_root(data_dir, plugin_id) / "logs"


def resolve_snapshot_root(data_dir: Path, plugin_id: str) -> Path:
    """定位宿主 Prompt 快照目录 ``<宿主根目录>/logs/maisaka_prompt``。"""

    return resolve_host_root(data_dir, plugin_id) / "logs" / "maisaka_prompt"


def parse_log_timestamp(timestamp_text: str, now: datetime) -> Optional[datetime]:
    """把宿主日志时间戳解析成本地 naive 时间。

    支持以下格式：

    - ISO-8601（宿主主进程，UTC 带 ``Z`` 后缀或带时区偏移）
    - ``YYYY-MM-DD HH:MM:SS``
    - ``MM-DD HH:MM:SS``（宿主不写年份，按接近当前时间推断年份）

    Args:
        timestamp_text: 日志里的 ``timestamp`` 字段。
        now: 当前本地时间，用于推断缺失的年份。

    Returns:
        Optional[datetime]: 解析成功时返回本地 naive 时间，无法识别时返回 ``None``。
    """

    raw_text = str(timestamp_text or "").strip()
    if not raw_text:
        return None

    if "T" in raw_text:
        try:
            parsed_iso = datetime.fromisoformat(raw_text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed_iso.tzinfo is not None:
            return parsed_iso.astimezone().replace(tzinfo=None)
        return parsed_iso

    # Runner 日志的 ``MM-DD HH:MM:SS``（宿主不写年份，按接近当前时间推断年份）
    segments = raw_text.split(" ")
    if len(segments) == 2 and len(segments[0].split("-")) == 2:
        try:
            month, day = (int(item) for item in segments[0].split("-"))
            hour, minute, second = (int(item) for item in segments[1].split(":"))
            parsed = datetime(now.year, month, day, hour, minute, second)
        except ValueError:
            return None
        # 比当前时间晚一天以上视为去年的记录（跨年窗口）
        if parsed > now + timedelta(days=1):
            parsed = parsed.replace(year=now.year - 1)
        return parsed

    try:
        return datetime.strptime(raw_text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _safe_mtime(path: Path) -> float:
    """读取文件修改时间，失败时返回 0。"""

    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _iter_lines_descending(path: Path, budget: _ByteBudget) -> Iterator[str]:
    """从文件尾部按块倒序产出文本行（新 → 旧）。

    逐块读取可以把「只看最近一段窗口」的开销限制在需要的字节数上，
    避免为了统计几分钟而读出整个日志文件。
    """

    try:
        file_size = path.stat().st_size
    except OSError:
        return
    if file_size <= 0:
        return

    offset = file_size
    remainder = b""
    try:
        with path.open("rb") as handle:
            while offset > 0:
                block_size = min(READ_BLOCK_BYTES, offset)
                granted = budget.take(block_size)
                if granted <= 0:
                    break
                offset -= granted
                handle.seek(offset)
                chunk = handle.read(granted)
                data = chunk + remainder
                parts = data.split(b"\n")
                # 首段可能是被截断的半行，留给下一轮（更早的内容）拼接
                remainder = parts[0]
                for raw_line in reversed(parts[1:]):
                    if raw_line:
                        yield raw_line.decode("utf-8", "ignore")
    except OSError:
        return
    if remainder:
        yield remainder.decode("utf-8", "ignore")


def _parse_failure_line(line_text: str, now: datetime) -> Optional[FailureEvent]:
    """从单行日志里解析失败事件，非失败行返回 ``None``。"""

    if FAILURE_EVENT_MARKER not in line_text:
        return None
    try:
        payload = json.loads(line_text)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    matched = FAILURE_MODEL_PATTERN.search(str(payload.get("event") or ""))
    if matched is None:
        return None
    model_name = matched.group(1).strip()
    if not model_name:
        return None
    return FailureEvent(
        model=model_name,
        occurred_at=parse_log_timestamp(str(payload.get("timestamp") or ""), now),
    )


def scan_failure_events(
    logs_dir: Path,
    window_start: datetime,
    *,
    read_budget_bytes: int,
    now: Optional[datetime] = None,
) -> LogFailureScan:
    """统计窗口 ``[window_start, now]`` 内的模型尝试失败事件。

    Args:
        logs_dir: 宿主日志目录。
        window_start: 统计窗口起点（本地时间）。
        read_budget_bytes: 本次扫描允许读取的最大字节数。
        now: 当前本地时间，缺省取系统时间。

    Returns:
        LogFailureScan: 扫描结果；``window_complete`` 为 ``False`` 表示窗口内数据可能不完整。
    """

    current_time = now or datetime.now()
    scan = LogFailureScan()
    if not logs_dir.is_dir():
        return scan

    scan.available = True
    budget = _ByteBudget(read_budget_bytes)
    candidate_files = [Path(path) for path in glob.glob(str(logs_dir / LOG_FILE_GLOB))]
    # 文件最后写入时间早于窗口起点时，文件内不可能存在窗口内事件
    window_timestamp = window_start.timestamp()
    candidate_files = [path for path in candidate_files if _safe_mtime(path) >= window_timestamp]
    candidate_files.sort(key=_safe_mtime, reverse=True)

    reached_window_start = False
    boundary_mtime: Optional[float] = None
    for path in candidate_files:
        if budget.exhausted:
            break
        file_mtime = _safe_mtime(path)
        # 已读到窗口下界后，mtime 相同的文件属于同一写入批次、先后顺序不确定，需要一并扫完
        if reached_window_start and boundary_mtime is not None and file_mtime < boundary_mtime:
            break
        scan.files_scanned += 1
        for line_text in _iter_lines_descending(path, budget):
            event = _parse_failure_line(line_text, current_time)
            if event is None:
                continue
            if event.occurred_at is not None and event.occurred_at < window_start:
                # 日志按时间顺序写入，读到窗口以前的记录即可停止该文件及更旧的日志
                reached_window_start = True
                boundary_mtime = file_mtime
                break
            if event.occurred_at is None:
                scan.unparsed_timestamps += 1
            scan.failures.append(event)

    scan.bytes_read = read_budget_bytes - budget.remaining
    scan.window_complete = reached_window_start or not budget.exhausted
    return scan
