from typing import Dict, List, Any, Optional, Union, Callable
from enum import Enum
from pydantic import BaseModel, Field
from datetime import datetime
import asyncio
from contextlib import asynccontextmanager

class Status(Enum):
    """统一的状态定义"""
    PENDING = "pending"      # 等待执行
    RUNNING = "running"      # 正在执行
    SUCCESS = "success"      # 执行成功
    FAILED = "failed"        # 执行失败
    SKIPPED = "skipped"      # 已跳过
    CANCELLED = "cancelled"  # 已取消
    BLOCKED = "blocked"      # 被阻塞
    TIMEOUT = "timeout"      # 执行超时
    
    @property
    def is_finished(self) -> bool:
        """是否已完成"""
        return self in [
            Status.SUCCESS,
            Status.FAILED,
            Status.SKIPPED,
            Status.CANCELLED,
            Status.TIMEOUT
        ]
        
    @property
    def is_successful(self) -> bool:
        """是否成功完成"""
        return self == Status.SUCCESS
        
    @property
    def can_continue(self) -> bool:
        """是否可以继续执行"""
        return self not in [
            Status.FAILED,
            Status.CANCELLED,
            Status.TIMEOUT
        ]

class Context(BaseModel):
    """执行上下文"""
    variables: Dict[str, Any] = Field(default_factory=dict)
    env: Dict[str, str] = Field(default_factory=dict)
    workspace: str = ""
    artifacts: Dict[str, str] = Field(default_factory=dict)
    params: Dict[str, Any] = Field(default_factory=dict)
    secrets: Dict[str, str] = Field(default_factory=dict)
    
    def get_variable(self, name: str, default: Any = None) -> Any:
        """获取变量值"""
        return self.variables.get(name, default)
        
    def set_variable(self, name: str, value: Any) -> None:
        """设置变量值"""
        self.variables[name] = value
        
    def get_env(self, name: str, default: str = "") -> str:
        """获取环境变量"""
        return self.env.get(name, default)
        
    def set_env(self, name: str, value: str) -> None:
        """设置环境变量"""
        self.env[name] = value
        
    def get_param(self, name: str, default: Any = None) -> Any:
        """获取参数值"""
        return self.params.get(name, default)
        
    def get_secret(self, name: str, default: str = "") -> str:
        """获取密钥值"""
        return self.secrets.get(name, default)

class LogHandler:
    """日志处理器基类"""
    async def handle_log(self, source: str, level: str, message: str) -> None:
        """处理日志
        
        Args:
            source: 日志来源（step/stage/pipeline的名称）
            level: 日志级别 (info/warning/error)
            message: 日志消息
        """
        raise NotImplementedError()

class BaseComponent(BaseModel):
    """基础组件，提供通用的日志功能"""
    name: str
    log_handler: Optional[LogHandler] = None
    
    async def emit_log(self, level: str, message: str) -> None:
        """发送日志
        
        Args:
            level: 日志级别 (info/warning/error)
            message: 日志消息
        """
        if self.log_handler:
            await self.log_handler.handle_log(self.name, level, message)

class Step(BaseComponent):
    """步骤基类"""
    name: str
    status: Status = Status.PENDING
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    timeout: Optional[int] = None  # 超时时间（秒）
    retry: int = 0  # 重试次数
    retry_delay: int = 0  # 重试延迟（秒）
    description: Optional[str] = None  # 步骤描述
    labels: Dict[str, str] = Field(default_factory=dict)  # 标签
    outputs: Dict[str, Any] = Field(default_factory=dict)  # 输出结果
    logs: List[str] = Field(default_factory=list)  # 执行日志
    retry_count: int = 0  # 当前重试次数
    duration: Optional[float] = None  # 执行时长（秒）
    
    def log(self, message: str, level: str = "info") -> None:
        """添加日志"""
        timestamp = datetime.now().isoformat()
        log_message = f"[{timestamp}] {message}"
        self.logs.append(log_message)
        # 发送日志到外部处理器
        if self.log_handler:
            asyncio.create_task(self.emit_log(level, message))
        
    def set_output(self, name: str, value: Any) -> None:
        """设置输出结果"""
        self.outputs[name] = value
        
    def get_output(self, name: str, default: Any = None) -> Any:
        """获取输出结果"""
        return self.outputs.get(name, default)
        
    @property
    def has_timeout(self) -> bool:
        """是否已设置超时"""
        return self.timeout is not None
        
    @property
    def can_retry(self) -> bool:
        """是否可以重试"""
        return self.retry_count < self.retry
        
    @property
    def duration_str(self) -> str:
        """获取格式化的执行时长"""
        if not self.duration:
            return "N/A"
        minutes, seconds = divmod(self.duration, 60)
        hours, minutes = divmod(minutes, 60)
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
        
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "name": self.name,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "error": self.error,
            "duration": self.duration,
            "duration_str": self.duration_str,
            "retry_count": self.retry_count,
            "outputs": self.outputs,
            "logs": self.logs
        }
        
    async def run(self, context: Context) -> None:
        """运行步骤"""
        self.status = Status.RUNNING
        self.started_at = datetime.now()
        
        try:
            attempts = self.retry + 1
            last_error = None
            
            for attempt in range(attempts):
                try:
                    self.retry_count = attempt
                    if attempt > 0:  # 重试延迟
                        self.log(f"Retrying attempt {attempt + 1}/{attempts}")
                        await asyncio.sleep(self.retry_delay)
                        
                    # 创建超时任务
                    if self.timeout:
                        try:
                            await asyncio.wait_for(
                                self._execute(context),
                                timeout=self.timeout
                            )
                        except asyncio.TimeoutError:
                            self.status = Status.TIMEOUT
                            self.error = f"Step timed out after {self.timeout} seconds"
                            self.log(f"Timeout: {self.error}", level="warning")
                            raise
                    else:
                        await self._execute(context)
                        
                    self.status = Status.SUCCESS
                    self.log("Step completed successfully")
                    break
                    
                except Exception as e:
                    last_error = str(e)
                    self.log(f"Error: {last_error}", level="error")
                    if attempt < attempts - 1:  # 还有重试机会
                        continue
                    self.status = Status.FAILED
                    self.error = last_error
                    raise
                    
        except Exception as e:
            if self.status not in [Status.FAILED, Status.TIMEOUT]:
                self.status = Status.FAILED
                self.error = str(e)
            raise
        finally:
            self.finished_at = datetime.now()
            if self.started_at:
                self.duration = (self.finished_at - self.started_at).total_seconds()
            
    async def _execute(self, context: Context) -> None:
        """执行步骤（由子类实现）"""
        raise NotImplementedError()
        
    async def cancel(self) -> None:
        """取消步骤"""
        self.status = Status.CANCELLED
        self.log("Step cancelled")

class Stage(BaseComponent):
    """阶段基类"""
    name: str
    parallel: bool = False  # 是否并行执行
    status: Status = Status.PENDING
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    timeout: Optional[int] = None  # 超时时间（秒）
    description: Optional[str] = None  # 阶段描述
    labels: Dict[str, str] = Field(default_factory=dict)  # 标签
    steps: List[Step] = Field(default_factory=list)  # 步骤列表
    duration: Optional[float] = None  # 执行时长（秒）
    
    def add_step(self, step: Step) -> None:
        """添加步骤"""
        step.log_handler = self.log_handler  # 传递日志处理器
        self.steps.append(step)
        
    def get_step(self, name: str) -> Optional[Step]:
        """获取步骤"""
        for step in self.steps:
            if step.name == name:
                return step
        return None
        
    @property
    def duration_str(self) -> str:
        """获取格式化的执行时长"""
        if not self.duration:
            return "N/A"
        minutes, seconds = divmod(self.duration, 60)
        hours, minutes = divmod(minutes, 60)
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
        
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "name": self.name,
            "parallel": self.parallel,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "error": self.error,
            "duration": self.duration,
            "duration_str": self.duration_str,
            "steps": [step.to_dict() for step in self.steps]
        }
        
    async def run(self, context: Context) -> None:
        """运行阶段"""
        self.status = Status.RUNNING
        self.started_at = datetime.now()
        
        try:
            if self.parallel:
                # 并行执行所有步骤
                tasks = [step.run(context) for step in self.steps]
                if self.timeout:
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*tasks),
                            timeout=self.timeout
                        )
                    except asyncio.TimeoutError:
                        self.status = Status.TIMEOUT
                        self.error = f"Stage timed out after {self.timeout} seconds"
                        # 取消所有未完成的步骤
                        for step in self.steps:
                            if not step.status.is_finished:
                                await step.cancel()
                        raise
                else:
                    await asyncio.gather(*tasks)
            else:
                # 串行执行所有步骤
                for step in self.steps:
                    if self.timeout:
                        try:
                            await asyncio.wait_for(
                                step.run(context),
                                timeout=self.timeout
                            )
                        except asyncio.TimeoutError:
                            self.status = Status.TIMEOUT
                            self.error = f"Stage timed out after {self.timeout} seconds"
                            await step.cancel()
                            raise
                    else:
                        await step.run(context)
                        
            # 检查所有步骤的状态
            failed_steps = [step for step in self.steps if not step.status.is_successful]
            if failed_steps:
                self.status = Status.FAILED
                self.error = f"Steps failed: {', '.join(s.name for s in failed_steps)}"
            else:
                self.status = Status.SUCCESS
                
        except Exception as e:
            if self.status not in [Status.FAILED, Status.TIMEOUT]:
                self.status = Status.FAILED
                self.error = str(e)
            raise
        finally:
            self.finished_at = datetime.now()
            if self.started_at:
                self.duration = (self.finished_at - self.started_at).total_seconds()
                
    async def cancel(self) -> None:
        """取消阶段"""
        self.status = Status.CANCELLED
        # 取消所有未完成的步骤
        for step in self.steps:
            if not step.status.is_finished:
                await step.cancel()

class Pipeline(BaseComponent):
    """流水线基类"""
    name: str
    status: Status = Status.PENDING
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    timeout: Optional[int] = None  # 超时时间（秒）
    description: Optional[str] = None  # 流水线描述
    labels: Dict[str, str] = Field(default_factory=dict)  # 标签
    stages: List[Stage] = Field(default_factory=list)  # 阶段列表
    context: Context  # 执行上下文
    duration: Optional[float] = None  # 执行时长（秒）
    
    def add_stage(self, stage: Stage) -> None:
        """添加阶段"""
        stage.log_handler = self.log_handler  # 传递日志处理器
        self.stages.append(stage)
        
    def get_stage(self, name: str) -> Optional[Stage]:
        """获取阶段"""
        for stage in self.stages:
            if stage.name == name:
                return stage
        return None
        
    def get_step(self, stage_name: str, step_name: str) -> Optional[Step]:
        """获取步骤"""
        stage = self.get_stage(stage_name)
        if stage:
            return stage.get_step(step_name)
        return None
        
    @property
    def duration_str(self) -> str:
        """获取格式化的执行时长"""
        if not self.duration:
            return "N/A"
        minutes, seconds = divmod(self.duration, 60)
        hours, minutes = divmod(minutes, 60)
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
        
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "name": self.name,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "error": self.error,
            "duration": self.duration,
            "duration_str": self.duration_str,
            "stages": [stage.to_dict() for stage in self.stages],
            "context": {
                "variables": self.context.variables,
                "env": self.context.env,
                "workspace": self.context.workspace,
                "params": self.context.params
            }
        }
        
    async def run(self) -> None:
        """运行流水线"""
        self.status = Status.RUNNING
        self.started_at = datetime.now()
        
        try:
            # 串行执行所有阶段
            for stage in self.stages:
                if self.timeout:
                    try:
                        await asyncio.wait_for(
                            stage.run(self.context),
                            timeout=self.timeout
                        )
                    except asyncio.TimeoutError:
                        self.status = Status.TIMEOUT
                        self.error = f"Pipeline timed out after {self.timeout} seconds"
                        await stage.cancel()
                        raise
                else:
                    await stage.run(self.context)
                    
                # 检查阶段状态
                if not stage.status.is_successful:
                    self.status = Status.FAILED
                    self.error = f"Stage {stage.name} failed: {stage.error}"
                    return
                    
            self.status = Status.SUCCESS
            
        except Exception as e:
            if self.status not in [Status.FAILED, Status.TIMEOUT]:
                self.status = Status.FAILED
                self.error = str(e)
            raise
        finally:
            self.finished_at = datetime.now()
            if self.started_at:
                self.duration = (self.finished_at - self.started_at).total_seconds()
                
    async def cancel(self) -> None:
        """取消流水线"""
        self.status = Status.CANCELLED
        # 取消所有未完成的阶段
        for stage in self.stages:
            if not stage.status.is_finished:
                await stage.cancel() 