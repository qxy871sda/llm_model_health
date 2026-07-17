from datetime import datetime, timedelta
from maibot_sdk import Command, Field, MaiBotPlugin, PluginConfigBase
from pathlib import Path
from typing import Any, Dict, List, Literal

import asyncio
import glob
import json
import os
import re
import sqlite3


class PluginSectionConfig(PluginConfigBase):
    """插件基础设置"""
    __ui_label__ = "基础设置"
    __ui_icon__ = "package"
    __ui_order__ = 0
    enabled: bool = Field(default=True, description="是否启用此统计插件", json_schema_extra={"label": "启用插件"})
    admin_qq: str = Field(
        default="", 
        description="有权限调出数据的管理员 QQ 号，多个 QQ 用中/英文逗号分隔。留空则允许所有用户调用。", 
        json_schema_extra={"label": "管理员 QQ"}
    )
    command_trigger: str = Field(
        default="/模型健康度", 
        description="调出统计数据的指令，修改后请重启插件生效", 
        json_schema_extra={"label": "指令触发器"}
    )
    config_version: str = Field(default="1.0.0", description="配置版本", json_schema_extra={"label": "配置版本", "disabled": True})


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
    async def handle_llm_stats(self, stream_id: str = "", user_id: str = "", matched_groups: dict = None, **kwargs: Any) -> tuple[bool, str, bool]:
        del kwargs
        if not self.config.plugin.enabled:
            return True, "", True
        
        # 检查指令触发器（忽略大小写，去除首尾空格）
        cmd = matched_groups.get("command", "").strip() if matched_groups else ""
        user_trigger = self.config.plugin.command_trigger.strip()
        if f"/{cmd}".lower() != user_trigger.lower():
            return False, "", False
        
        # 解析管理员 QQ 列表（兼容中英文逗号，留空则允许所有人调用）
        raw_admins = self.config.plugin.admin_qq.replace("，", ",")
        admin_list = [x.strip() for x in raw_admins.split(",") if x.strip()]
        if admin_list and str(user_id) not in admin_list:
            await self.ctx.send.text("抱歉，您没有权限使用此指令。", stream_id)
            return True, None, True
        
        report_data = await self._generate_report()

        if not report_data["recent_report"] and not report_data["history_report"]:
            return True, "📊 **LLM 模型使用统计**\n\n暂无任何模型请求或使用记录。", True

        bot_qq = str(await self.ctx.config.get("bot.qq_account", "0") or "0")
        bot_name = str(await self.ctx.config.get("bot.nickname", "麦麦") or "麦麦")
        
        forward_messages = []
        
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
                f"**近期运行状态 (基于日志统计)**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{report_data['recent_report']}"
            )
            forward_messages.append({
                "user_id": bot_qq,
                "nickname": "近期运行状态",
                "segments": [{"type": "text", "content": recent_text}]
            })
            
        # 3. 历史累计统计
        if report_data["history_report"]:
            history_text = (
                f"**历史累计统计 (基于数据库)**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{report_data['history_report']}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"* 注: 历史统计仅包含成功完成的请求。"
            )
            forward_messages.append({
                "user_id": bot_qq,
                "nickname": "历史累计统计",
                "segments": [{"type": "text", "content": history_text}]
            })
            
        await self.ctx.send.forward(forward_messages, stream_id)
        return True, "统计报告已通过合并转发消息发送喵！", True

    async def _generate_report(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._build_report_sync)

    def _build_report_sync(self) -> dict[str, Any]:
        project_root = Path(__file__).resolve().parents[2]
        logs_dir = project_root / "logs"
        db_path = project_root / "data" / "MaiBot.db"

        # 获取配置中的统计范围和类型
        range_type = self.config.stats.range_type
        range_value = self.config.stats.range_value

        attempts: Dict[str, int] = {}
        failures: Dict[str, int] = {}

        # 1. 扫描日志文件以获取近期模型请求和失败统计
        # 必须按修改时间倒序：当前主进程持续写入的日志文件名可能更旧，
        # 而大量短生命周期启动日志文件名更新却几乎不含模型请求。
        # 若按文件名字典序取前几个，会扫到空壳日志，导致“近期运行状态”为空。
        log_pattern = os.path.join(logs_dir, "app_*.log.jsonl")
        log_files = sorted(
            glob.glob(log_pattern),
            key=lambda path: os.path.getmtime(path),
            reverse=True,
        )
        # 扩大扫描上限；真正的停止条件由 minutes/count 控制
        log_files = log_files[:30]

        now = datetime.now()
        time_limit = None
        if range_type == "minutes":
            time_limit = now - timedelta(minutes=range_value)

        total_attempts_count = 0
        should_stop = False

        for log_file in log_files:
            if should_stop:
                break
            try:
                with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()
                
                for line in reversed(lines):
                    if not line.strip():
                        continue
                    try:
                        if "选择请求模型:" in line or "尝试失败，切换到下一个模型" in line:
                            data = json.loads(line)
                            event = str(data.get("event", ""))
                            timestamp_str = str(data.get("timestamp", ""))

                            # 进行时间范围校验
                            dt = None
                            if timestamp_str:
                                try:
                                    dt = datetime.strptime(f"{now.year}-{timestamp_str}", "%Y-%m-%d %H:%M:%S")
                                    if dt > now + timedelta(days=1):
                                        dt = dt.replace(year=now.year - 1)
                                except Exception:
                                    pass

                            if time_limit and dt and dt < time_limit:
                                should_stop = True
                                break

                            if "选择请求模型:" in event:
                                match = re.search(r"选择请求模型:\s*([^\s(]+)", event)
                                if match:
                                    model = match.group(1).strip()
                                    attempts[model] = attempts.get(model, 0) + 1

                                    if range_type == "count":
                                        total_attempts_count += 1
                                        if total_attempts_count >= range_value:
                                            should_stop = True
                                            break

                            elif "尝试失败，切换到下一个模型" in event:
                                match = re.search(r"模型\s*'([^']+)'\s*尝试失败", event)
                                if match:
                                    model = match.group(1).strip()
                                    failures[model] = failures.get(model, 0) + 1
                    except Exception:
                        continue
            except Exception:
                continue

        # 2. 从 SQLite 数据库读取历史累计成功请求
        db_successes: Dict[str, int] = {}
        db_costs: Dict[str, float] = {}
        db_times: Dict[str, float] = {}

        if db_path.exists():
            try:
                conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT model_assign_name, COUNT(*), SUM(cost), AVG(time_cost) 
                    FROM llm_usage 
                    GROUP BY model_assign_name
                """)
                for row in cursor.fetchall():
                    if row[0]:
                        model = str(row[0]).strip()
                        db_successes[model] = int(row[1])
                        db_costs[model] = float(row[2] or 0.0)
                        db_times[model] = float(row[3] or 0.0)
                conn.close()
            except Exception as e:
                self.ctx.logger.warning(f"读取数据库 llm_usage 失败: {e}")

        # 3. 汇总所有出现的模型名称并进行排序
        all_models = sorted(list(set(attempts.keys()) | set(failures.keys()) | set(db_successes.keys())))

        range_desc = f"最近 {range_value} 分钟" if range_type == "minutes" else f"最近 {range_value} 次请求"

        if not all_models:
            return {
                "range_desc": range_desc,
                "recent_report": None,
                "history_report": None
            }

        # 4. 构建近期运行状态列表
        recent_rows: List[str] = []
        for model in all_models:
            att = attempts.get(model, 0)
            fail = failures.get(model, 0)
            if fail > att:
                att = fail
            succ = max(0, att - fail)
            if att > 0:
                rate = (succ / att) * 100
                recent_rows.append(f"- **{model}**: 请求 {att} 次 | 成功 {succ} 次 | 失败 {fail} 次 | 成功率 {rate:.1f}%")

        # 5. 构建历史累计统计列表
        history_rows: List[str] = []
        for model in all_models:
            succ = db_successes.get(model, 0)
            cost = db_costs.get(model, 0)
            avg_time = db_times.get(model, 0)
            if succ > 0:
                history_rows.append(f"- **{model}**: 成功 {succ} 次 | 平均耗时 {avg_time:.2f}s")

        return {
            "range_desc": range_desc,
            "recent_report": "\n".join(recent_rows) if recent_rows else None,
            "history_report": "\n".join(history_rows) if history_rows else None
        }


def create_plugin() -> LLMMonitorPlugin:
    """创建插件实例"""
    return LLMMonitorPlugin()