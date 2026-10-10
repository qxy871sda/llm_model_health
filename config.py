"""插件配置模型。"""

from typing import Literal

from maibot_sdk import Field, PluginConfigBase


class PluginSectionConfig(PluginConfigBase):
    """插件基础设置"""

    __ui_label__ = "基础设置"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(default=True, description="是否启用此统计插件", json_schema_extra={"label": "启用插件"})
    admin_qq: str = Field(
        default="",
        description="有权限调出数据的管理员 QQ 号，多个 QQ 用中/英文逗号分隔。留空时所有人均不可调用。",
        json_schema_extra={"label": "管理员 QQ"},
    )
    command_trigger: str = Field(
        default="/模型健康度",
        description="调出统计数据的指令，修改后请重启插件生效",
        json_schema_extra={"label": "触发指令"},
    )
    config_version: str = Field(
        default="1.2.0",
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
        json_schema_extra={"label": "统计范围类型"},
    )
    range_value: int = Field(
        default=30,
        ge=1,
        description=(
            "统计计算范围的数值。若类型为 minutes，代表统计最近 N 分钟；"
            "若类型为 count，代表窗口内最近 N 次模型请求尝试（成功与失败一起计数）。"
        ),
        json_schema_extra={"label": "统计范围数值", "min": 1},
    )
    db_scan_max_rows: int = Field(
        default=5000,
        ge=100,
        le=100000,
        description=(
            "窗口内最多读取多少条成功记录（宿主 llm_usage 表）。"
            "窗口较长或请求量很大时可调高；读取上限被填满时报告会明确提示。"
        ),
        json_schema_extra={"label": "成功记录读取上限", "min": 100, "max": 100000},
    )
    log_read_budget_mb: int = Field(
        default=32,
        ge=1,
        le=512,
        description="扫描失败日志的最大读取量（MB）。预算不足时报告会明确提示窗口未被完整覆盖。",
        json_schema_extra={"label": "失败日志读取预算(MB)", "min": 1, "max": 512},
    )
    top_model_rows: int = Field(
        default=15,
        ge=1,
        le=50,
        description="近期运行状态里最多展示多少个模型（按请求数排序）。",
        json_schema_extra={"label": "近期模型展示条数", "min": 1, "max": 50},
    )
    enable_snapshot_detail: bool = Field(
        default=False,
        description=(
            "是否从宿主 Prompt 快照读取「失败原因」与「模型自身耗时」（默认关闭）。"
            "快照只覆盖会写 Prompt 预览的模块，缺少快照时该部分自动省略。"
        ),
        json_schema_extra={"label": "失败原因与自身耗时"},
    )
    snapshot_reason_rows: int = Field(
        default=5,
        ge=1,
        le=20,
        description="失败原因最多展示多少条。",
        json_schema_extra={"label": "失败原因条数", "min": 1, "max": 20},
    )
    enable_sdk_statistics: bool = Field(
        default=False,
        description="是否获取并展示历史模型请求数、token 用量与平均耗时汇总（默认关闭）。",
        json_schema_extra={"label": "启用历史模型汇总"},
    )
    sdk_statistics_days: int = Field(
        default=365,
        ge=1,
        le=365,
        description="历史模型汇总的查询范围，代表最近 N 天，可设置为 1 至 365。",
        json_schema_extra={"label": "历史汇总天数", "min": 1, "max": 365},
    )
    show_model_cost: bool = Field(
        default=False,
        description="是否在历史汇总中展示每个模型的累计费用（默认关闭，避免误把内部账单当结论）。",
        json_schema_extra={"label": "显示模型费用"},
    )


class LLMMonitorPluginConfig(PluginConfigBase):
    """LLM 模型健康度统计插件配置"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    stats: StatsSectionConfig = Field(default_factory=StatsSectionConfig)
