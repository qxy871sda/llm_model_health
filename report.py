"""窗口统计聚合与报告文本渲染（纯函数，不调用任何宿主能力）。

成功率口径
----------
本插件的「近期运行状态」由两个互补数据源合成，两者都覆盖全部模块：

- **成功数**：宿主 ``llm_usage`` 表（每次成功完成的逻辑请求都会入账），
  经 ``database.query`` 读取后按时间窗口在插件侧过滤。
- **失败数**：宿主日志里的 ``模型 '<分配名>' 尝试失败，切换到下一个模型``（WARNING）。

因此 ``尝试数 = 成功数 + 失败数``、``成功率 = 成功 / 尝试数``，
既不受宿主文件日志级别（DEBUG/INFO）影响，也不依赖任何启发式补偿。
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .host_logs import FailureEvent
from .host_snapshots import SnapshotSummary

# 失败原因最多附带多少个模型名
_REASON_MODEL_LIMIT = 3


@dataclass(slots=True)
class ModelWindowStat:
    """窗口内单个模型的统计结果。

    Attributes:
        label: 模型分配名（与 llm_usage、日志文案保持一致的口径）。
        success: 窗口内成功完成的请求数。
        failure: 窗口内被宿主判定失败并切换的尝试数。
        request_seconds_total: 成功请求的整轮耗时之和（秒）。
        total_tokens: 成功请求的 token 总量。
        total_cost: 成功请求的累计费用。
        self_median_seconds: 快照里成功尝试的自身耗时中位数（秒），无快照时为 ``None``。
        self_samples: 自身耗时样本数。
    """

    label: str
    success: int = 0
    failure: int = 0
    request_seconds_total: float = 0.0
    total_tokens: int = 0
    total_cost: float = 0.0
    self_median_seconds: Optional[float] = None
    self_samples: int = 0

    @property
    def attempts(self) -> int:
        """窗口内的模型尝试总数（成功 + 失败）。"""

        return self.success + self.failure

    @property
    def success_rate(self) -> Optional[float]:
        """成功率百分比；窗口内没有任何尝试时返回 ``None``。"""

        if self.attempts <= 0:
            return None
        return self.success / self.attempts * 100

    @property
    def avg_request_seconds(self) -> float:
        """成功请求的整轮耗时均值（含同一次请求里其它模型的失败等待）。"""

        if self.success <= 0:
            return 0.0
        return self.request_seconds_total / self.success


def _safe_float(value: Any) -> float:
    """把任意值安全转成 float。"""

    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    """把任意值安全转成 int。"""

    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def row_timestamp(row: Mapping[str, Any]) -> Optional[datetime]:
    """解析 ``llm_usage`` 记录的时间戳（宿主写本地 ISO 字符串）。"""

    raw_value = row.get("timestamp")
    if isinstance(raw_value, datetime):
        return raw_value
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def row_label(row: Mapping[str, Any]) -> str:
    """取记录对应的模型展示名（分配名优先，与日志文案口径一致）。"""

    assign_name = str(row.get("model_assign_name") or "").strip()
    if assign_name:
        return assign_name
    return str(row.get("model_name") or "").strip() or "未知模型"


def build_model_label_map(rows: Iterable[Mapping[str, Any]]) -> Dict[str, str]:
    """用 ``llm_usage`` 记录构造「供应商模型标识|API 供应商 -> 分配名」映射。

    Prompt 快照里的尝试只记录供应商模型标识（``model``）与 API 供应商（``provider``），
    而 ``llm_usage`` 的同一行同时带 ``model_name``、``model_api_provider_name`` 与
    ``model_assign_name``，因此可以直接复用官方能力读到的记录做映射，
    不需要再读宿主 ``config/model_config.toml``。

    Args:
        rows: 窗口内的 ``llm_usage`` 记录。

    Returns:
        Dict[str, str]: 映射表；同一个标识被多个分配名共用时以先出现的记录为准。
    """

    labels: Dict[str, str] = {}
    for row in rows:
        assign_name = str(row.get("model_assign_name") or "").strip()
        identifier = str(row.get("model_name") or "").strip()
        if not assign_name or not identifier:
            continue
        provider = str(row.get("model_api_provider_name") or "").strip()
        labels.setdefault(f"{identifier}|{provider}", assign_name)
        labels.setdefault(f"{identifier}|", assign_name)
    return labels


def build_window_stats(
    success_rows: Sequence[Mapping[str, Any]],
    failures: Iterable[FailureEvent],
    snapshot: Optional[SnapshotSummary] = None,
) -> List[ModelWindowStat]:
    """把成功记录与失败事件合成按模型的窗口统计。

    Args:
        success_rows: 窗口内的 ``llm_usage`` 记录。
        failures: 窗口内的失败事件。
        snapshot: 可选的快照摘要，用于补充模型自身耗时。

    Returns:
        List[ModelWindowStat]: 按尝试数倒序排列的统计结果。
    """

    stats_by_label: dict[str, ModelWindowStat] = {}
    for row in success_rows:
        label = row_label(row)
        stat = stats_by_label.setdefault(label, ModelWindowStat(label=label))
        stat.success += 1
        stat.request_seconds_total += _safe_float(row.get("time_cost"))
        stat.total_tokens += _safe_int(row.get("total_tokens"))
        stat.total_cost += _safe_float(row.get("cost"))

    for event in failures:
        label = event.model or "未知模型"
        stat = stats_by_label.setdefault(label, ModelWindowStat(label=label))
        stat.failure += 1

    if snapshot is not None:
        for label, median_seconds in snapshot.self_median_seconds.items():
            stat = stats_by_label.get(label)
            if stat is None:
                # 快照里出现但窗口内没有正式记录的模型不单独展示，避免与成功率口径混淆
                continue
            stat.self_median_seconds = median_seconds
            stat.self_samples = snapshot.self_sample_counts.get(label, 0)

    return sorted(stats_by_label.values(), key=lambda item: (-item.attempts, item.label))


def select_count_window(
    success_rows: Sequence[Mapping[str, Any]],
    failures: Sequence[FailureEvent],
    *,
    count: int,
) -> Tuple[List[Mapping[str, Any]], List[FailureEvent], Optional[datetime]]:
    """取「最近 ``count`` 次模型尝试」作为统计窗口。

    成功与失败按发生时间合并排序后截取，因此失败同样计入「请求次数」口径。

    Args:
        success_rows: 已读取的成功记录（可多于窗口需要的条数）。
        failures: 已扫描到的失败事件。
        count: 窗口内期望的尝试次数。

    Returns:
        Tuple[List[Mapping[str, Any]], List[FailureEvent], Optional[datetime]]:
        窗口内成功记录、窗口内失败事件、窗口起点；无可用记录时窗口起点为 ``None``。
    """

    entries: List[Tuple[datetime, int, Any]] = []
    for row in success_rows:
        timestamp = row_timestamp(row)
        if timestamp is not None:
            entries.append((timestamp, 0, row))
    for event in failures:
        if event.occurred_at is not None:
            entries.append((event.occurred_at, 1, event))

    entries.sort(key=lambda item: item[0], reverse=True)
    selected = entries[: max(int(count), 1)]
    if not selected:
        return [], [], None

    window_start = selected[-1][0]
    selected_rows = [item[2] for item in selected if item[1] == 0]
    selected_failures = [item[2] for item in selected if item[1] == 1]
    return selected_rows, selected_failures, window_start


def format_duration(seconds: float) -> str:
    """把秒数格式化成紧凑的可读文本。"""

    if seconds <= 0:
        return "0.00s"
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes, remainder = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m{remainder:.0f}s"
    hours, remainder_minutes = divmod(minutes, 60)
    return f"{int(hours)}h{int(remainder_minutes)}m"


def format_tokens(total_tokens: int) -> str:
    """把 token 数量格式化成紧凑的可读文本。"""

    if total_tokens >= 1_000_000:
        return f"{total_tokens / 1_000_000:.2f}M"
    if total_tokens >= 1_000:
        return f"{total_tokens / 1_000:.1f}k"
    return str(total_tokens)


def format_cost(cost: float) -> str:
    """把费用格式化成带货币符号的可读文本。"""

    return f"￥{cost:.4f}" if cost < 1 else f"￥{cost:.2f}"


def render_window_lines(stats: Sequence[ModelWindowStat], *, limit: int) -> List[str]:
    """渲染「近期运行状态」明细行。"""

    lines: List[str] = []
    for stat in stats[:limit]:
        success_rate = stat.success_rate
        rate_text = f"{success_rate:.1f}%" if success_rate is not None else "—"
        parts = [
            f"请求 {stat.attempts}",
            f"成功 {stat.success}",
            f"失败 {stat.failure}",
            f"成功率 {rate_text}",
        ]
        if stat.success > 0:
            parts.append(f"整轮耗时 {format_duration(stat.avg_request_seconds)}")
        if stat.self_median_seconds is not None:
            parts.append(f"自身耗时 {format_duration(stat.self_median_seconds)}")
        if stat.total_tokens > 0:
            parts.append(f"{format_tokens(stat.total_tokens)} tokens")
        lines.append(f"- **{stat.label}**：" + " | ".join(parts))
    if len(stats) > limit:
        lines.append(f"- 其余 {len(stats) - limit} 个模型未展示，可调高「近期模型展示条数」")
    return lines


def render_failure_reason_lines(snapshot: SnapshotSummary, *, limit: int) -> List[str]:
    """渲染失败原因明细行。"""

    lines: List[str] = []
    for reason, count in snapshot.reasons[:limit]:
        model_counter = snapshot.reason_models.get(reason)
        suffix = ""
        if model_counter:
            names = [name for name, _ in model_counter.most_common(_REASON_MODEL_LIMIT)]
            suffix = "（" + "、".join(names) + "）"
        lines.append(f"- {reason} × {count}{suffix}")
    return lines


def render_history_lines(models: Sequence[Mapping[str, Any]], *, show_cost: bool) -> List[str]:
    """渲染历史累计汇总行。

    Args:
        models: ``statistics.local.models`` 返回的记录。
        show_cost: 是否展示累计费用（由配置项「显示模型费用」控制，默认关闭）。
    """

    lines: List[str] = []
    for item in models:
        model_name = str(item.get("model_name") or "").strip()
        request_count = _safe_int(item.get("request_count"))
        if not model_name or request_count <= 0:
            continue
        parts = [
            f"请求 {request_count} 次",
            f"整轮耗时 {format_duration(_safe_float(item.get('avg_response_time')))}",
        ]
        total_tokens = _safe_int(item.get("total_tokens"))
        if total_tokens > 0:
            parts.append(f"{format_tokens(total_tokens)} tokens")
        total_cost = _safe_float(item.get("total_cost"))
        if show_cost and total_cost > 0:
            parts.append(f"费用 {format_cost(total_cost)}")
        lines.append(f"- **{model_name}**：" + " | ".join(parts))
    return lines
