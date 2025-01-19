from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
from anytree import NodeMixin
from ..dsl.models import Pipeline, Status, LogHandler

class JobStatus(Enum):
    """作业状态"""
    ENABLED = "enabled"    # 已启用
    DISABLED = "disabled"  # 已禁用
    DELETED = "deleted"    # 已删除

class BuildStatus(Enum):
    """构建状态"""
    PENDING = "pending"      # 等待执行
    RUNNING = "running"      # 正在执行
    SUCCESS = "success"      # 执行成功
    FAILED = "failed"        # 执行失败
    CANCELLED = "cancelled"  # 已取消
    TIMEOUT = "timeout"      # 执行超时
    
    @property
    def is_finished(self) -> bool:
        """是否已完成"""
        return self in [
            BuildStatus.SUCCESS,
            BuildStatus.FAILED,
            BuildStatus.CANCELLED,
            BuildStatus.TIMEOUT
        ]

class BuildLog(BaseModel):
    """构建日志"""
    timestamp: datetime
    level: str
    source: str
    message: str

class Build(BaseModel):
    """构建记录"""
    id: str  # 构建ID
    job_id: str  # 作业ID
    number: int  # 构建号
    status: BuildStatus = BuildStatus.PENDING
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration: Optional[float] = None  # 执行时长（秒）
    error: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)  # 构建参数
    pipeline_status: Dict[str, Status] = Field(default_factory=dict)  # 流水线各阶段状态
    logs: List[BuildLog] = Field(default_factory=list)  # 构建日志
    triggered_by: Optional[str] = None  # 触发者
    triggered_from: Optional[str] = None  # 触发来源（如果是被其他job调用）
    sub_builds: List[str] = Field(default_factory=list)  # 子构建ID列表
    
    @property
    def duration_str(self) -> str:
        """获取格式化的执行时长"""
        if not self.duration:
            return "N/A"
        minutes, seconds = divmod(self.duration, 60)
        hours, minutes = divmod(minutes, 60)
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
        
    def add_log(self, level: str, source: str, message: str) -> None:
        """添加日志"""
        self.logs.append(BuildLog(
            timestamp=datetime.now(),
            level=level,
            source=source,
            message=message
        ))

class JobType(Enum):
    """作业类型"""
    FOLDER = "folder"    # 文件夹
    PIPELINE = "pipeline"  # 流水线

class Job(NodeMixin, BaseModel):
    """作业定义"""
    id: str  # 作业ID
    name: str  # 作业名称
    path: str  # 作业路径
    type: JobType  # 作业类型
    description: Optional[str] = None  # 作业描述
    status: JobStatus = JobStatus.ENABLED
    pipeline: Optional[Pipeline] = None  # 流水线定义（仅pipeline类型有效）
    params: Dict[str, Any] = Field(default_factory=dict)  # 默认参数
    labels: Dict[str, str] = Field(default_factory=dict)  # 标签
    timeout: Optional[int] = None  # 超时时间（秒）
    max_builds: int = 50  # 最大保留构建数（仅pipeline类型有效）
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    last_build_id: Optional[str] = None  # 最后一次构建ID
    last_build_number: int = 0  # 最后一次构建号
    last_build_status: Optional[BuildStatus] = None  # 最后一次构建状态
    last_success_build_id: Optional[str] = None  # 最后一次成功构建ID
    last_failure_build_id: Optional[str] = None  # 最后一次失败构建ID
    
    def __init__(self, **data):
        super().__init__(**data)
        self.parent = None
        self.children = []
        
    def update(self, **kwargs) -> None:
        """更新作业信息"""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        self.updated_at = datetime.now()
        
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "id": self.id,
            "name": self.name,
            "path": self.path,
            "description": self.description,
            "status": self.status.value,
            "params": self.params,
            "labels": self.labels,
            "timeout": self.timeout,
            "max_builds": self.max_builds,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "last_build_number": self.last_build_number,
            "last_build_status": self.last_build_status.value if self.last_build_status else None
        }

class BuildLogHandler(LogHandler):
    """构建日志处理器"""
    def __init__(self, build: Build):
        self.build = build
        
    async def handle_log(self, source: str, level: str, message: str) -> None:
        """处理日志"""
        self.build.add_log(level, source, message) 