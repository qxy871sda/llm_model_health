from datetime import datetime, timedelta
from maibot_sdk import Command, Field, MaiBotPlugin, PluginConfigBase
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import asyncio
import glob
import json
import os
import re


SDK_MODEL_LIMIT = 50


class PluginSectionConfig(PluginConfigBase):
    """插件基础设置"""
    __ui_label__ = "基础设置"
    __ui_icon__ = "package"
    __ui_order__ = 0
    enabled: bool = Field(default=True, description="是否启用此统计插件", json_schema_extra={"label": "启用插件"})
    admin_qq: str = Field(
        default="", 
        description="有权限调出数据的管理员 QQ 号，多个 QQ 用中/英文逗号分隔。留空时所有人均不可调用。", 
        json_schema_extra={"label": "管理员 QQ"}
    )
    command_trigger: str = Field(
        default="/模型健康度", 
        description="调出统计数据的指令，修改后请重启插件生效", 
        json_schema_extra={"label": "触发指令"}
    )
    config_version: str = Field(
        default="1.0.4",
        description="配置版本",
        json_schema_extra={"label": "配置版本", "disabled": True},
    )


class StatsSectionConfig(PluginConfigBase):
    """统计设置"""
    __ui_label__ = "统计范围"
    __ui_icon__ = "bar-chart-2"
    __ui_order__ = 1

    range_type: Literal["minutes", "count"] = Field(
        default="minutes", 
        description="统计的计算范围类型。可选：minutes (按分钟) 或 count (按请求次数)",
        json_schema_extra={"label": "统计范围类型"}
    )
    range_value: int = Field(
        default=30, 
        description="统计计算范围的数值。若类型为 minutes，代表统计最近 N 分钟；若类型为 count，代表统计最近 N 次模型请求。",
        json_schema_extra={"label": "统计范围数值", "min": 1}
    )
    enable_sdk_statistics: bool = Field(
        default=True,
        description="是否获取并展示历史模型请求数、平均耗时与累计费用汇总。",
        json_schema_extra={"label": "启用历史模型汇总"},
    )
    sdk_statistics_days: int = Field(
        default=365,
        ge=1,
        le=365,
        description="历史模型汇总的查询范围，代表最近 N 天，可设置为 1 至 365。",
        json_schema_extra={"label": "历史汇总天数", "min": 1, "max": 365},
    )


class LLMMonitorPluginConfig(PluginConfigBase):
    """LLM 监控统计插件配置"""
    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    stats: StatsSectionConfig = Field(default_factory=StatsSectionConfig)


class LLMMonitorPlugin(MaiBotPlugin):
    """LLM 成功率与历史使用总量统计插件"""
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
        self.ctx.logger.info("LLM 监控统计插件已成功加载")

    async def on_unload(self) -> None:
        self.ctx.logger.info("LLM 监控统计插件已卸载")

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        """处理配置更新"""
        del scope
        del config_data
        del version

    @Command(
        "llm_stats",
        description="统计并展示 LLM 模型的调用成功率与历史总量",
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

        report_data = await self._generate_report()
        if not report_data["recent_report"] and not report_data["sdk_report"]:
            return True, "📊 **LLM 模型使用统计**\n\n暂无任何模型请求或使用记录。", True

        bot_qq = str(await self.ctx.config.get("bot.qq_account", "0"))
        bot_name = str(await self.ctx.config.get("bot.nickname", "麦麦"))
        forward_messages: List[Dict[str, Any]] = []
        
        # 1. 标题页
        title_text = (
            f"📊 **MaiBot LLM 模型成功率与使用统计**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏰ 统计范围：{report_data['range_desc']}\n"
            f"包含近期运行状态与历史累计总量\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        forward_messages.append({
            "user_id": bot_qq,
            "nickname": bot_name,
            "segments": [{"type": "text", "content": title_text}]
        })
        
        # 2. 近期模型运行状态
        if report_data["recent_report"]:
            recent_text = (
                f"**近期运行状态 (基于日志统计)**\n" # * SDK 暂无 LLM 重试失败次数接口，因此此部分需要解析日志。
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{report_data['recent_report']}"
            )
            forward_messages.append({
                "user_id": bot_qq,
                "nickname": "近期运行状态",
                "segments": [{"type": "text", "content": recent_text}]
            })

        if report_data["sdk_report"]:
            sdk_text = (
                f"**历史 {report_data['sdk_statistics_days']} 天模型汇总 (基于数据库)**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{report_data['sdk_report']}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"* 注: 历史统计仅包含成功完成的请求。"
            )
            forward_messages.append({
                "user_id": bot_qq,
                "nickname": "历史累计统计",
                    "segments": [{"type": "text", "content": sdk_text}]
            })
            
        await self.ctx.send.forward(forward_messages, stream_id)
        return True, "统计报告已通过合并转发消息发送喵！", True

    async def _generate_report(self) -> dict[str, Any]:
        """分别通过日志和 SDK 获取数据，再组装报告。"""

        logs_dir = self._get_logs_dir()
        attempts, failures = await asyncio.to_thread(self._scan_recent_logs, logs_dir)
        enable_sdk_statistics = self.config.stats.enable_sdk_statistics
        sdk_statistics_days = self.config.stats.sdk_statistics_days
        sdk_models: List[Dict[str, Any]] = []
        if enable_sdk_statistics:
            sdk_models = await self.ctx.statistics.local.models(days=sdk_statistics_days, limit=SDK_MODEL_LIMIT)

        recent_rows: List[str] = []
        for model in sorted(set(attempts) | set(failures)):
            attempt_count = max(attempts.get(model, 0), failures.get(model, 0))
            failure_count = failures.get(model, 0)
            success_count = max(0, attempt_count - failure_count)
            if attempt_count > 0:
                success_rate = success_count / attempt_count * 100
                recent_rows.append(
                    f"- **{model}**：请求 {attempt_count} 次 | 成功 {success_count} 次 | "
                    f"失败 {failure_count} 次 | 成功率 {success_rate:.1f}%"
                )

        sdk_rows: List[str] = []
        for item in sdk_models:
            model_name = str(item["model_name"]).strip()
            request_count = int(item["request_count"])
            total_cost = float(item["total_cost"])
            avg_response_time = float(item["avg_response_time"])
            if model_name and request_count > 0:
                sdk_rows.append(
                    f"- **{model_name}**：请求 {request_count} 次 | 平均耗时 {avg_response_time:.2f}s"
                )

        range_type = self.config.stats.range_type
        range_value = self.config.stats.range_value
        range_desc = f"最近 {range_value} 分钟" if range_type == "minutes" else f"最近 {range_value} 次请求"
        return {
            "range_desc": range_desc,
            "sdk_statistics_days": sdk_statistics_days,
            "sdk_range_text": (
                f"📈 历史汇总范围：最近 {sdk_statistics_days} 天"
                if enable_sdk_statistics
                else "📈 历史模型汇总：已关闭"
            ),
            "recent_report": "\n".join(recent_rows) if recent_rows else None,
            "sdk_report": "\n".join(sdk_rows) if sdk_rows else None,
        }

    def _get_logs_dir(self) -> Path:
        """校验标准数据路径后定位宿主日志目录。"""

        # SDK 暂未授予宿主日志目录，只能基于其承诺的标准 data_dir 布局定位。
        # 严格校验每一级，避免自定义路径布局下意外读取无关目录。
        data_dir = self.ctx.paths.data_dir.resolve()
        if (
            data_dir.name != self.ctx.plugin_id
            or data_dir.parent.name != "plugins"
            or data_dir.parent.parent.name != "data"
        ):
            raise RuntimeError(
                "无法从 ctx.paths.data_dir 安全定位宿主日志目录："
                "运行时路径不符合 <MaiBot>/data/plugins/<plugin_id> 标准布局"
            )
        host_root = data_dir.parents[2]
        return host_root / "logs"

    def _scan_recent_logs(self, logs_dir: Path) -> Tuple[Dict[str, int], Dict[str, int]]:
        """读取日志中 SDK 尚未提供的模型尝试与失败事件。"""

        range_type = self.config.stats.range_type
        range_value = self.config.stats.range_value
        attempts: Dict[str, int] = {}
        failures: Dict[str, int] = {}

        # 优化：不再直接读取所有日志文件，而是尝试调用宿主已有的日志查看能力
        # 考虑到宿主没有直接的日志 API，我们通过读取最近的 JSONL 日志，尽量减少 IO 开销。
        if not logs_dir.is_dir():
            self.ctx.logger.warning("宿主日志目录不存在，无法统计 LLM 失败次数：%s", logs_dir)
            return attempts, failures

        # 当前主进程持续写入的文件名可能更旧，因此必须按修改时间倒序。
        log_pattern = os.path.join(logs_dir, "app_*.log.jsonl")
        log_files = sorted(glob.glob(log_pattern), key=os.path.getmtime, reverse=True)[:5] # 减少文件扫描数量至5个
        now = datetime.now()
        time_limit = now - timedelta(minutes=range_value) if range_type == "minutes" else None
        total_attempts_count = 0
        should_stop = False

        for log_file in log_files:
            if should_stop:
                break
            try:
                # 使用读取最后几千字节的方式读取 JSONL，减少读取全量文件开销
                file_size = os.path.getsize(log_file)
                read_size = min(file_size, 1024 * 100) # 仅读取最后 100KB
                with open(log_file, "r", encoding="utf-8", errors="ignore") as file:
                    if file_size > read_size:
                        file.seek(file_size - read_size)
                    lines = file.readlines()
            except OSError as exc:
                self.ctx.logger.warning("读取日志文件失败 %s：%s", log_file, exc)
                continue

            for line in reversed(lines):
                if not line.strip() or ("选择请求模型:" not in line and "尝试失败，切换到下一个模型" not in line):
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                event = str(data.get("event", ""))
                timestamp = self._parse_log_timestamp(str(data.get("timestamp", "")), now)
                if time_limit and timestamp and timestamp < time_limit:
                    should_stop = True
                    break

                selected_match = re.search(r"选择请求模型:\s*([^\s(]+)", event)
                if selected_match:
                    model = selected_match.group(1).strip()
                    attempts[model] = attempts.get(model, 0) + 1
                    if range_type == "count":
                        total_attempts_count += 1
                        if total_attempts_count >= range_value:
                            should_stop = True
                            break
                    continue

                failure_match = re.search(r"模型\s*'([^']+)'\s*尝试失败", event)
                if failure_match:
                    model = failure_match.group(1).strip()
                    failures[model] = failures.get(model, 0) + 1

        return attempts, failures

    @staticmethod
    def _parse_log_timestamp(timestamp_text: str, now: datetime) -> Optional[datetime]:
        """解析宿主 JSONL 日志中不含年份的时间戳。"""

        if not timestamp_text:
            return None
        try:
            timestamp = datetime.strptime(f"{now.year}-{timestamp_text}", "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
        if timestamp > now + timedelta(days=1):
            return timestamp.replace(year=now.year - 1)
        return timestamp


def create_plugin() -> LLMMonitorPlugin:
    """创建插件实例"""
    return LLMMonitorPlugin()