"""LLM 模型健康度统计插件（成功率 / 使用总量 / 失败归因）。

数据来源与口径
--------------
- **成功数**：宿主 ``llm_usage`` 表（经 ``database.query`` 能力读取，按窗口时间过滤），覆盖全部模块。
- **失败数**：宿主日志 ``模型 '<分配名>' 尝试失败，切换到下一个模型``（WARNING），覆盖全部模块。
- **成功率**：``成功 / (成功 + 失败)``，不依赖宿主文件日志级别，也不使用任何启发式补偿。
- **失败原因 / 自身耗时**：宿主 Prompt 快照的 ``generation_attempts``（默认关闭，仅覆盖写预览的模块）。
- **历史汇总**：SDK 的 ``statistics.local.models``（默认关闭，是否展示费用另有开关）。
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import asyncio
import re

from maibot_sdk import Command, MaiBotPlugin

from .config import LLMMonitorPluginConfig, StatsSectionConfig
from .host_logs import (
    FailureEvent,
    LogFailureScan,
    resolve_logs_dir,
    resolve_snapshot_root,
    scan_failure_events,
)
from .host_snapshots import SnapshotSummary, summarize_snapshots
from .report import (
    ModelWindowStat,
    build_model_label_map,
    build_window_stats,
    render_failure_reason_lines,
    render_history_lines,
    render_window_lines,
    row_timestamp,
    select_count_window,
)

# 历史汇总最多展示的模型数量
SDK_MODEL_LIMIT = 50
# count 模式下逐级放大的成功记录读取阶梯（最终不超过配置的读取上限）
COUNT_FETCH_LADDER = (500, 2000)
# 报告分隔线
SEPARATOR = "━━━━━━━━━━━━━━━━━━━━━━"
# count 模式下数据库没有成功记录时的提示
NO_SUCCESS_ROWS_NOTE = (
    "未能从宿主 llm_usage 读取到成功记录，无法按请求次数界定窗口，请检查数据库读取或调小「统计范围数值」。"
)


@dataclass(slots=True)
class WindowCollection:
    """一次采集到的窗口数据。

    Attributes:
        range_desc: 窗口的可读描述（例如「最近 60 分钟」）。
        stats: 按模型聚合的窗口统计。
        success_total: 窗口内成功请求总数。
        failure_total: 窗口内失败尝试总数。
        coverage_notes: 数据完整性提示（读取上限、预算不足等）。
        snapshot: 快照摘要，未启用或不可用时为 ``None``。
    """

    range_desc: str
    stats: List[ModelWindowStat] = field(default_factory=list)
    success_total: int = 0
    failure_total: int = 0
    coverage_notes: List[str] = field(default_factory=list)
    snapshot: Optional[SnapshotSummary] = None

    @property
    def attempts_total(self) -> int:
        """窗口内的模型尝试总数。"""

        return self.success_total + self.failure_total

    @property
    def success_rate(self) -> Optional[float]:
        """窗口整体成功率百分比；没有尝试时返回 ``None``。"""

        if self.attempts_total <= 0:
            return None
        return self.success_total / self.attempts_total * 100


def _forward_node(user_id: str, nickname: str, text: str) -> Dict[str, Any]:
    """构建一个合并转发节点。"""

    return {
        "user_id": user_id,
        "nickname": nickname,
        "segments": [{"type": "text", "content": text}],
    }


class LLMMonitorPlugin(MaiBotPlugin):
    """LLM 模型健康度统计插件"""

    config_model = LLMMonitorPluginConfig

    def get_components(self) -> List[Dict[str, Any]]:
        """根据当前配置注册精确的统计命令，避免占用其它插件的斜杠指令。"""

        components = super().get_components()
        command_trigger = self.config.plugin.command_trigger.strip()
        if not command_trigger.startswith("/"):
            command_trigger = f"/{command_trigger}"
        command_name = command_trigger[1:]
        command_pattern = rf"^/(?P<command>{re.escape(command_name)})(?:\s+.*)?$"

        for component in components:
            if component.get("name") != "llm_stats" or component.get("type") != "COMMAND":
                continue

            metadata = component.get("metadata")
            if isinstance(metadata, dict):
                metadata["command_pattern"] = command_pattern
        return components

    async def on_load(self) -> None:
        self.ctx.logger.info("LLM 模型健康度统计插件已成功加载")

    async def on_unload(self) -> None:
        self.ctx.logger.info("LLM 模型健康度统计插件已卸载")

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        """处理配置更新"""

        del scope
        del config_data
        del version

    @Command(
        "llm_stats",
        description="统计并展示 LLM 模型健康度、成功率与历史总量",
        pattern=r"^/(?P<command>模型健康度)(?:\s+.*)?$",
    )
    async def handle_llm_stats(
        self,
        stream_id: str = "",
        user_id: str = "",
        matched_groups: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> Tuple[bool, Optional[str], bool]:
        del kwargs
        if not self.config.plugin.enabled:
            return True, "", True

        # 检查指令触发器（忽略大小写，去除首尾空格）
        cmd = matched_groups.get("command", "").strip() if matched_groups else ""
        user_trigger = self.config.plugin.command_trigger.strip()
        if f"/{cmd}".lower() != user_trigger.lower():
            return False, "", False

        # 解析管理员 QQ 列表，未配置管理员时默认拒绝所有调用。
        raw_admins = self.config.plugin.admin_qq.replace("，", ",")
        admin_list = [admin.strip() for admin in raw_admins.split(",") if admin.strip()]
        if not admin_list:
            await self.ctx.send.text("当前未配置管理员，请先填写 admin_qq。", stream_id)
            return True, None, True
        if str(user_id) not in admin_list:
            await self.ctx.send.text("抱歉，您没有权限使用此指令。", stream_id)
            return True, None, True

        nodes = await self._build_report_nodes()
        if not nodes:
            return True, "📊 **LLM 模型健康度**\n\n暂无任何模型请求或使用记录。", True

        await self.ctx.send.forward(nodes, stream_id)
        return True, "模型健康度报告已通过合并转发消息发送喵！", True

    async def _build_report_nodes(self) -> List[Dict[str, Any]]:
        """采集窗口、历史汇总与趋势数据并组装合并转发节点。"""

        config = self.config.stats
        window = await self._collect_window()
        history_models = await self._fetch_history_models()
        if not window.stats and not history_models:
            return []

        bot_qq = str(await self.ctx.config.get("bot.qq_account", "0"))
        bot_name = str(await self.ctx.config.get("bot.nickname", "麦麦"))
        nodes: List[Dict[str, Any]] = []

        # 1. 标题页：统计窗口、整体成功率与数据完整性提示
        header_lines = [
            "📊 **MaiBot LLM 模型健康度报告**",
            SEPARATOR,
            f"⏰ 统计窗口：{window.range_desc}",
        ]
        if window.attempts_total > 0:
            success_rate = window.success_rate
            rate_text = f"{success_rate:.1f}%" if success_rate is not None else "—"
            header_lines.append(
                f"🔢 窗口内请求：{window.attempts_total} 次"
                f"（成功 {window.success_total} | 失败 {window.failure_total} | 成功率 {rate_text}）"
            )
        if config.enable_sdk_statistics:
            header_lines.append(f"📚 历史汇总：最近 {config.sdk_statistics_days} 天")
        header_lines.append(SEPARATOR)
        header_lines.extend(f"⚠️ {note}" for note in window.coverage_notes)
        nodes.append(_forward_node(bot_qq, bot_name, "\n".join(header_lines)))

        # 2. 近期运行状态：按模型的成功率明细
        if window.stats:
            detail_lines = [f"**近期运行状态（{window.range_desc}）**", SEPARATOR]
            detail_lines.extend(render_window_lines(window.stats, limit=config.top_model_rows))
            detail_lines.append("")
            detail_lines.append("* 整轮耗时含同一次请求里其它模型的失败等待。")
            if config.enable_snapshot_detail and window.snapshot is not None:
                detail_lines.append("* 自身耗时来自宿主 Prompt 快照，只衡量该模型自己的耗时。")
            nodes.append(_forward_node(bot_qq, "近期运行状态", "\n".join(detail_lines)))

        # 2.1 失败原因：单独一页，跟在近期运行状态之后
        snapshot = window.snapshot
        if snapshot is not None and snapshot.reasons:
            reason_lines = [f"**失败原因（窗口内 {snapshot.attempts} 次可见尝试）**", SEPARATOR]
            reason_lines.extend(render_failure_reason_lines(snapshot, limit=config.snapshot_reason_rows))
            if snapshot.truncated:
                reason_lines.append("")
                reason_lines.append("- 快照解析达到上限，原因统计可能不完整")
            nodes.append(_forward_node(bot_qq, "失败原因", "\n".join(reason_lines)))

        # 3. 历史累计汇总（宿主本机统计数据，只含成功请求）
        if history_models:
            history_lines = [f"**最近 {config.sdk_statistics_days} 天模型汇总（宿主统计数据）**", SEPARATOR]
            history_lines.extend(render_history_lines(history_models, show_cost=config.show_model_cost))
            history_lines.append("")
            history_lines.append("* 历史汇总只包含成功完成的请求，因此不展示成功率。")
            nodes.append(_forward_node(bot_qq, "历史累计统计", "\n".join(history_lines)))

        # 4. 口径说明
        nodes.append(
            _forward_node(
                bot_qq,
                "统计口径",
                "\n".join(
                    [
                        "**统计口径**",
                        SEPARATOR,
                        "- 成功率 = 窗口内成功请求 / (成功 + 失败尝试)",
                        "- 成功数：宿主 llm_usage 表，覆盖全部模块",
                        "- 失败数：宿主 WARNING 日志「尝试失败，切换到下一个模型」，覆盖全部模块",
                        "- 自身耗时与失败原因：宿主 Prompt 快照（需开启「失败原因与自身耗时」），仅覆盖会写预览的模块",
                        "- 历史汇总：宿主本机统计（需开启「启用历史模型汇总」），只含成功请求",
                        SEPARATOR,
                        f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    ]
                ),
            )
        )
        return nodes

    async def _collect_window(self) -> WindowCollection:
        """采集统计窗口内的成功与失败数据。"""

        config = self.config.stats
        coverage_notes: List[str] = []
        logs_dir = self._logs_dir()
        log_scan = LogFailureScan()

        if config.range_type == "minutes":
            range_desc = f"最近 {config.range_value} 分钟"
            window_start = datetime.now() - timedelta(minutes=config.range_value)
            raw_rows = await self._fetch_success_rows(config.db_scan_max_rows)
            success_rows = [row for row in raw_rows if self._row_since(row, window_start)]
            self._note_window_shortfall(raw_rows, window_start, config.db_scan_max_rows, coverage_notes)
            log_scan = await asyncio.to_thread(
                scan_failure_events,
                logs_dir,
                window_start,
                read_budget_bytes=max(1, config.log_read_budget_mb) * 1024 * 1024,
            )
            failures = list(log_scan.failures)
        else:
            range_desc = f"最近 {config.range_value} 次请求"
            success_rows, failures, window_start, log_scan = await self._collect_count_window(config, coverage_notes)

        missing_rows = sum(1 for row in success_rows if row_timestamp(row) is None)
        if missing_rows:
            coverage_notes.append(f"{missing_rows} 条成功记录缺少可解析的时间戳，已按窗口内计入。")
        self._note_log_coverage(log_scan, logs_dir, coverage_notes)
        if not success_rows and failures:
            coverage_notes.append("窗口内只读到失败记录、没有读到任何成功记录，请检查宿主数据库是否可读。")

        snapshot: Optional[SnapshotSummary] = None
        if config.enable_snapshot_detail and window_start is not None:
            snapshot = await asyncio.to_thread(
                summarize_snapshots,
                self._snapshot_root(),
                window_start,
                model_labels=build_model_label_map(success_rows),
            )
        self._note_snapshot_cross_check(failures, log_scan, snapshot, coverage_notes)

        return WindowCollection(
            range_desc=range_desc,
            stats=build_window_stats(success_rows, failures, snapshot),
            success_total=len(success_rows),
            failure_total=len(failures),
            coverage_notes=coverage_notes,
            snapshot=snapshot,
        )

    async def _collect_count_window(
        self,
        config: StatsSectionConfig,
        coverage_notes: List[str],
    ) -> Tuple[List[Dict[str, Any]], List[FailureEvent], Optional[datetime], LogFailureScan]:
        """count 模式采集：阶梯式放大读取范围，直到凑满 N 次尝试。"""

        fetch_limits = sorted(
            {limit for limit in (*COUNT_FETCH_LADDER, config.db_scan_max_rows) if 0 < limit <= config.db_scan_max_rows}
        )
        if not fetch_limits:
            fetch_limits = [config.db_scan_max_rows]

        logs_dir = self._logs_dir()
        budget_bytes = max(1, config.log_read_budget_mb) * 1024 * 1024
        selected_rows: List[Dict[str, Any]] = []
        selected_failures: List[FailureEvent] = []
        window_start: Optional[datetime] = None
        log_scan = LogFailureScan()

        for limit in fetch_limits:
            raw_rows = await self._fetch_success_rows(limit)
            if not raw_rows:
                coverage_notes.append(NO_SUCCESS_ROWS_NOTE)
                return selected_rows, selected_failures, window_start, log_scan

            # 失败事件只需扫描到本次取回的记录下界之前：更早的失败不可能落进窗口
            scan_lower_bound = self._oldest_row_timestamp(raw_rows) or datetime.now()
            log_scan = await asyncio.to_thread(
                scan_failure_events,
                logs_dir,
                scan_lower_bound,
                read_budget_bytes=budget_bytes,
            )
            selected_rows, selected_failures, window_start = select_count_window(
                raw_rows,
                log_scan.failures,
                count=config.range_value,
            )
            selected_total = len(selected_rows) + len(selected_failures)
            if selected_total >= config.range_value:
                break
            if len(raw_rows) < limit:
                coverage_notes.append(
                    f"宿主数据库内可用的成功记录不足 {config.range_value} 次，窗口按现有 {selected_total} 次尝试计算。"
                )
                break

        return selected_rows, selected_failures, window_start, log_scan

    async def _fetch_success_rows(self, limit: int) -> List[Dict[str, Any]]:
        """按主键倒序读取最新的 llm_usage 记录。"""

        rows = await self.ctx.db.query("ModelUsage", order_by=["-id"], limit=limit)
        if not isinstance(rows, list):
            return []
        return [row for row in rows if isinstance(row, dict)]

    async def _fetch_history_models(self) -> List[Dict[str, Any]]:
        """读取宿主本机历史模型汇总（只包含成功完成的请求）。"""

        config = self.config.stats
        if not config.enable_sdk_statistics:
            return []
        models = await self.ctx.statistics.local.models(
            days=config.sdk_statistics_days,
            limit=SDK_MODEL_LIMIT,
        )
        if not isinstance(models, list):
            return []
        return [item for item in models if isinstance(item, dict)]

    @staticmethod
    def _row_since(row: Dict[str, Any], window_start: datetime) -> bool:
        """判断记录是否落在窗口内；缺少时间戳的记录先按窗口内计入并在报告中提示。"""

        timestamp = row_timestamp(row)
        return timestamp is None or timestamp >= window_start

    @staticmethod
    def _oldest_row_timestamp(rows: List[Dict[str, Any]]) -> Optional[datetime]:
        """取记录列表里最早的时间戳（列表按主键倒序，因此从尾部开始找）。"""

        for row in reversed(rows):
            timestamp = row_timestamp(row)
            if timestamp is not None:
                return timestamp
        return None

    @staticmethod
    def _note_window_shortfall(
        raw_rows: List[Dict[str, Any]],
        window_start: datetime,
        limit: int,
        coverage_notes: List[str],
    ) -> None:
        """记录「成功记录读取未覆盖整个窗口」的提示。"""

        if len(raw_rows) < limit:
            return
        oldest_timestamp = LLMMonitorPlugin._oldest_row_timestamp(raw_rows)
        if oldest_timestamp is None or oldest_timestamp <= window_start:
            return
        coverage_notes.append(
            f"窗口内成功记录达到读取上限 {limit} 条，较早的窗口记录可能未计入，可调高「成功记录读取上限」。"
        )

    @staticmethod
    def _note_log_coverage(log_scan: LogFailureScan, logs_dir: Path, coverage_notes: List[str]) -> None:
        """记录失败日志读取的完整性提示。"""

        if not log_scan.available:
            coverage_notes.append(f"未找到宿主日志目录（{logs_dir}），失败数与成功率不可用。")
            return
        if not log_scan.window_complete:
            read_mb = log_scan.bytes_read / (1024 * 1024)
            coverage_notes.append(
                f"失败日志读取预算不足（已读 {read_mb:.1f}MB），窗口内失败数可能不完整，可调高「失败日志读取预算」。"
            )
        if log_scan.unparsed_timestamps:
            coverage_notes.append(f"{log_scan.unparsed_timestamps} 条失败记录缺少可解析的时间戳，已按窗口内计入。")

    @staticmethod
    def _note_snapshot_cross_check(
        failures: List[FailureEvent],
        log_scan: LogFailureScan,
        snapshot: Optional[SnapshotSummary],
        coverage_notes: List[str],
    ) -> None:
        """用 Prompt 快照交叉校验失败数：日志一条都没匹配到却有快照失败，说明日志文案可能已失效。"""

        if failures or snapshot is None:
            return
        snapshot_failures = sum(count for _, count in snapshot.reasons)
        if snapshot_failures <= 0:
            return
        if not log_scan.available or not log_scan.window_complete:
            # 日志本身不可用或窗口没读全时，失败数缺失已有对应提示，不重复报
            return
        coverage_notes.append(
            f"宿主日志里没有匹配到任何失败记录，但 Prompt 快照中有 {snapshot_failures} 次失败尝试；"
            "宿主日志文案或日志级别可能已变更，失败数与成功率可能不完整。"
        )

    def _logs_dir(self) -> Path:
        """定位宿主日志目录。"""

        return resolve_logs_dir(self.ctx.paths.data_dir, self.ctx.plugin_id)

    def _snapshot_root(self) -> Path:
        """定位宿主 Prompt 快照目录。"""

        return resolve_snapshot_root(self.ctx.paths.data_dir, self.ctx.plugin_id)


def create_plugin() -> LLMMonitorPlugin:
    """创建插件实例"""

    return LLMMonitorPlugin()
