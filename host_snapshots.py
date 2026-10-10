"""宿主 Prompt 快照读取：窗口内的失败原因与「模型自身耗时」。

数据来源（宿主私有实现依赖）
--------------------------------
宿主把每个逻辑请求的逐次尝试写进两类 JSON：

- ``logs/maisaka_prompt/<类别>/<会话>/<毫秒时间戳>.json``：请求预览（成功调用也会写）
- ``logs/maisaka_prompt/llm_error/<会话>/<首次失败时刻>_<request_id>.json``：失败快照（只在失败时写，
  且会被宿主按 ``log.llm_request_snapshot_limit`` 裁剪，因此不适合做长期统计）

其中 ``generation_attempts[]`` 逐条记录 ``model``（供应商模型标识）、``provider``（API 供应商）、
``started_at``、``duration_ms``、``status``（``succeeded`` / ``failed`` / ``switching_model`` / ``retrying``）
与 ``error``。供应商模型标识到「分配名」的映射由官方能力读到的 ``llm_usage`` 记录提供，
因此本模块不读取宿主模型配置文件。

⚠️ 覆盖范围限制：只有会写 Prompt 预览的模块（回复、表达/黑话学习、情绪、行为等）才有快照，
   ``A_Memorix.*`` 等模块不会写。因此本模块的数据只用于「失败原因」与「模型自身耗时」这类增量信息，
   **绝不参与成功率的分子/分母**，否则会把高频但无快照的模块整体漏掉。
"""

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Counter as CounterType, Dict, List, Mapping, Optional, Tuple

import json
import os
import statistics
import time

# 单次摘要允许解析的快照文件数量与耗时上限，避免异常目录结构下开销失控
MAX_SNAPSHOT_FILES = 4000
MAX_SNAPSHOT_SECONDS = 5.0
# 失败原因展示时每条原因最多附带几个模型名
MAX_REASON_MODELS = 3


@dataclass(slots=True)
class SnapshotSummary:
    """窗口内的快照摘要。

    Attributes:
        available: 快照目录是否存在。
        attempts: 去重后纳入统计的尝试条数。
        truncated: 是否因文件数量/耗时上限提前停止。
        reasons: 失败原因按次数倒序排列的列表。
        reason_models: 失败原因 → 该原因涉及的模型名计数。
        self_median_seconds: 模型标签 → 成功尝试的自身耗时中位数（秒）。
        self_sample_counts: 模型标签 → 成功尝试样本数。
    """

    available: bool = False
    attempts: int = 0
    truncated: bool = False
    reasons: List[Tuple[str, int]] = field(default_factory=list)
    reason_models: Dict[str, CounterType[str]] = field(default_factory=dict)
    self_median_seconds: Dict[str, float] = field(default_factory=dict)
    self_sample_counts: Dict[str, int] = field(default_factory=dict)


def compact_failure_reason(error: Any) -> str:
    """把宿主记录的错误对象压成一句可读的原因。"""

    if not isinstance(error, Mapping):
        return "未记录原因"
    message = " ".join(str(error.get("message") or "").split())
    lowered = message.lower()
    status_code = error.get("status_code")
    if isinstance(status_code, int):
        if status_code == 429:
            return "429 限流"
        if status_code in {401, 403}:
            return f"HTTP {status_code}（鉴权/权限）"
        if status_code == 400:
            return "HTTP 400（请求被拒）"
        if 500 <= status_code < 600:
            return f"HTTP {status_code}（服务端错误）"
        return f"HTTP {status_code}"
    if "timeout" in lowered or "timed out" in lowered:
        return "超时"
    if "reset" in lowered and "connection" in lowered:
        return "连接被重置"
    if "connection" in lowered:
        return "连接失败"
    if "empty_response" in lowered or "空回复" in message:
        return "空回复"
    if "rate limit" in lowered:
        return "限流"
    if not message:
        return "未记录原因"
    return message[:30] + "…" if len(message) > 30 else message


def _extract_stem_ms(file_name: str) -> Optional[int]:
    """从快照文件名里取出毫秒时间戳。"""

    if not file_name.endswith(".json"):
        return None
    stem = file_name[: -len(".json")]
    if stem.isdigit():
        return int(stem)
    # 失败快照命名为 YYYYmmdd_HHMMSS_ffffff_<request_id>.json
    parts = stem.split("_")
    if len(parts) < 3 or not parts[0].isdigit() or len(parts[0]) != 8 or not parts[2].isdigit():
        return None
    try:
        moment = datetime.strptime(f"{parts[0]}_{parts[1]}_{parts[2]}", "%Y%m%d_%H%M%S_%f")
    except ValueError:
        return None
    return int(moment.timestamp() * 1000)


def _parse_iso_time(value: Any) -> Optional[datetime]:
    """解析快照里的 ISO 本地时间字符串。"""

    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _iter_snapshot_files(snapshot_root: Path, window_start_ms: int) -> List[Path]:
    """收集窗口中可能包含尝试明细的快照文件（按时间倒序）。"""

    entries: List[Tuple[int, Path]] = []
    with os.scandir(snapshot_root) as categories:
        for category in categories:
            if not category.is_dir():
                continue
            with os.scandir(category.path) as chat_dirs:
                for chat_dir in chat_dirs:
                    if not chat_dir.is_dir():
                        continue
                    with os.scandir(chat_dir.path) as files:
                        for entry in files:
                            if not entry.is_file():
                                continue
                            stem_ms = _extract_stem_ms(entry.name)
                            if stem_ms is None:
                                continue
                            # 失败快照会被追加剧次尝试（文件名保持首次失败时刻），因此同时看 mtime
                            try:
                                mtime_ms = int(entry.stat().st_mtime * 1000)
                            except OSError:
                                mtime_ms = stem_ms
                            if stem_ms >= window_start_ms or mtime_ms >= window_start_ms:
                                entries.append((stem_ms, Path(entry.path)))
    entries.sort(key=lambda item: item[0], reverse=True)
    return [path for _, path in entries]


@dataclass(slots=True, frozen=True)
class _AttemptRecord:
    """从快照里读出的单次尝试。"""

    attempt_key: str
    label: str
    succeeded: bool
    duration_seconds: float
    occurred_at: Optional[datetime]
    reason: str


def _resolve_label(identifier: str, provider: str, model_labels: Mapping[str, str]) -> str:
    """把尝试里的供应商模型标识映射成用户配置里的分配名。

    映射表由调用方用 ``llm_usage`` 记录构造（见 ``report.build_model_label_map``），
    因此这里不依赖宿主模型配置文件。
    """

    matched = model_labels.get(f"{identifier}|{provider}") or model_labels.get(f"{identifier}|")
    return matched or identifier or "未知模型"


def _parse_snapshot_attempts(
    payload: Any,
    *,
    model_labels: Mapping[str, str],
    source_path: Path,
) -> List[_AttemptRecord]:
    """从单个快照载荷里提取尝试明细。"""

    if not isinstance(payload, dict):
        return []
    raw_attempts = payload.get("generation_attempts")
    if not isinstance(raw_attempts, list):
        return []
    records: List[_AttemptRecord] = []
    for index, item in enumerate(raw_attempts):
        if not isinstance(item, dict):
            continue
        duration_ms = item.get("duration_ms")
        if not isinstance(duration_ms, (int, float)):
            continue
        identifier = str(item.get("model") or "").strip()
        provider = str(item.get("provider") or "").strip()
        status = str(item.get("status") or "").strip()
        attempt_id = str(item.get("attempt_id") or "").strip()
        started_at = _parse_iso_time(item.get("started_at"))
        attempt_key = attempt_id or f"{source_path.as_posix()}#{index}#{identifier}#{started_at}"
        records.append(
            _AttemptRecord(
                attempt_key=attempt_key,
                label=_resolve_label(identifier, provider, model_labels),
                succeeded=status == "succeeded",
                duration_seconds=max(0.0, float(duration_ms) / 1000.0),
                occurred_at=started_at,
                reason="" if status == "succeeded" else compact_failure_reason(item.get("error")),
            )
        )
    return records


def summarize_snapshots(
    snapshot_root: Path,
    window_start: datetime,
    *,
    model_labels: Optional[Mapping[str, str]] = None,
) -> SnapshotSummary:
    """汇总窗口内的失败原因与「模型自身耗时中位数」。

    Args:
        snapshot_root: ``logs/maisaka_prompt`` 目录。
        window_start: 统计窗口起点（本地时间）。
        model_labels: 「供应商模型标识|API 供应商 -> 分配名」映射，
            由调用方用 ``llm_usage`` 记录构造（见 ``report.build_model_label_map``）。

    Returns:
        SnapshotSummary: 摘要结果；目录不存在时为 ``available=False`` 的空摘要。
    """

    summary = SnapshotSummary()
    if not snapshot_root.is_dir():
        return summary
    summary.available = True

    labels = dict(model_labels or {})
    window_start_ms = int(window_start.timestamp() * 1000)
    candidate_files = _iter_snapshot_files(snapshot_root, window_start_ms)

    reason_counter: CounterType[str] = Counter()
    reason_models: Dict[str, CounterType[str]] = {}
    self_samples: Dict[str, List[float]] = {}
    seen_attempts: set[str] = set()
    files_scanned = 0
    deadline = time.monotonic() + MAX_SNAPSHOT_SECONDS

    for path in candidate_files:
        if files_scanned >= MAX_SNAPSHOT_FILES or time.monotonic() > deadline:
            summary.truncated = True
            break
        files_scanned += 1
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, ValueError, TypeError):
            continue
        for record in _parse_snapshot_attempts(payload, model_labels=labels, source_path=path):
            if record.attempt_key in seen_attempts:
                continue
            seen_attempts.add(record.attempt_key)
            if record.occurred_at is not None and record.occurred_at < window_start:
                continue
            summary.attempts += 1
            if record.succeeded:
                self_samples.setdefault(record.label, []).append(record.duration_seconds)
                continue
            reason_counter[record.reason] += 1
            reason_models.setdefault(record.reason, Counter())[record.label] += 1

    summary.reasons = sorted(reason_counter.items(), key=lambda item: (-item[1], item[0]))
    summary.reason_models = reason_models
    summary.self_median_seconds = {
        label: statistics.median(samples) for label, samples in self_samples.items() if samples
    }
    summary.self_sample_counts = {label: len(samples) for label, samples in self_samples.items() if samples}
    return summary
