import asyncio
import uuid
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from typing import Dict, List, Any, Optional

from pydantic import BaseModel, Field


class Status(Enum):
    """统一的状态定义"""
    PENDING = "pending"  # 等待执行
    RUNNING = "running"  # 正在执行
    SUCCESS = "success"  # 执行成功
    FAILED = "failed"  # 执行失败
    SKIPPED = "skipped"  # 已跳过
    CANCELLED = "cancelled"  # 已取消
    BLOCKED = "blocked"  # 被阻塞
    TIMEOUT = "timeout"  # 执行超时

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
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    type: str  # build, stage, step 等
    status: Status = Status.PENDING
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    variables: Dict[str, Any] = Field(default_factory=dict)  # 变量
    env: Dict[str, str] = Field(default_factory=dict)  # 环境变量
    secrets: Dict[str, str] = Field(default_factory=dict)  # 密钥
    workspace: str = ""  # 工作目录
    artifacts: Dict[str, str] = Field(default_factory=dict)  # 制品
    outputs: Dict[str, Any] = Field(default_factory=dict)  # 输出
    parent: Optional['Context'] = None  # 父上下文

    def get_variable(self, name: str, default: Any = None) -> Any:
        """获取变量值（包括父上下文）"""
        if name in self.variables:
            return self.variables[name]
        if self.parent:
            return self.parent.get_variable(name, default)
        return default

    def set_variable(self, name: str, value: Any, scope: str = "local") -> None:
        """设置变量值
        
        Args:
            name: 变量名
            value: 变量值
            scope: 作用域（local/parent/global）
        """
        if scope == "local" or not self.parent:
            self.variables[name] = value
        elif scope == "parent" and self.parent:
            self.parent.set_variable(name, value, "local")
        elif scope == "global":
            context = self
            while context.parent:
                context = context.parent
            context.variables[name] = value

    def get_env(self, name: str, default: str = "") -> str:
        """获取环境变量（包括父上下文）"""
        if name in self.env:
            return self.env[name]
        if self.parent:
            return self.parent.get_env(name, default)
        return default

    def set_env(self, name: str, value: str, scope: str = "local") -> None:
        """设置环境变量
        
        Args:
            name: 变量名
            value: 变量值
            scope: 作用域（local/parent/global）
        """
        if scope == "local" or not self.parent:
            self.env[name] = value
        elif scope == "parent" and self.parent:
            self.parent.set_env(name, value, "local")
        elif scope == "global":
            context = self
            while context.parent:
                context = context.parent
            context.env[name] = value

    def get_secret(self, name: str, default: str = "") -> str:
        """获取密钥（包括父上下文）"""
        if name in self.secrets:
            return self.secrets[name]
        if self.parent:
            return self.parent.get_secret(name, default)
        return default

    def set_output(self, name: str, value: Any) -> None:
        """设置输出"""
        self.outputs[name] = value

    def get_output(self, name: str, default: Any = None) -> Any:
        """获取输出（包括父上下文）"""
        if name in self.outputs:
            return self.outputs[name]
        if self.parent:
            return self.parent.get_output(name, default)
        return default

    def get_artifact_path(self, name: str) -> Optional[str]:
        """获取制品路径"""
        return self.artifacts.get(name)

    def set_artifact(self, name: str, path: str) -> None:
        """设置制品"""
        self.artifacts[name] = path

    def get_workspace(self) -> str:
        """获取工作目录（如果未设置则使用父上下文的）"""
        if self.workspace:
            return self.workspace
        if self.parent:
            return self.parent.get_workspace()
        return ""

    def create_child(self, name: str, type: str) -> 'Context':
        """创建子上下文"""
        child = Context(
            name=name,
            type=type,
            workspace=self.get_workspace(),
            parent=self,  # 设置父上下文
            variables=self.variables.copy(),  # 继承变量
            env=self.env.copy(),  # 继承环境变量
            secrets=self.secrets.copy()  # 继承密钥
        )
        return child

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "variables": self.variables,
            "env": self.env,
            "workspace": self.workspace,
            "outputs": self.outputs,
            "artifacts": self.artifacts
        }


class ContextManager:
    """上下文管理器"""

    def __init__(self):
        self._contexts: Dict[str, Context] = {}
        self._current: Optional[Context] = None

    @property
    def current(self) -> Optional[Context]:
        """获取当前上下文"""
        return self._current

    def push(self, context: Context) -> None:
        """压入新的上下文"""
        context.parent = self._current
        self._contexts[context.id] = context
        self._current = context

    def pop(self) -> Optional[Context]:
        """弹出当前上下文"""
        if not self._current:
            return None

        context = self._current
        self._current = context.parent
        return context

    def get(self, context_id: str) -> Optional[Context]:
        """获取指定上下文"""
        return self._contexts.get(context_id)

    @contextmanager
    def scope(self, context: Context):
        """上下文作用域"""
        self.push(context)
        try:
            yield context
        finally:
            self.pop()


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
    logs: List[str] = Field(default_factory=list)  # 执行日志
    
    model_config = {
        "arbitrary_types_allowed": True  # 允许任意类型
    }

    def log(self, message: str, level: str = "info") -> None:
        """添加日志"""
        timestamp = datetime.now().isoformat()
        log_message = f"[{timestamp}] {message}"
        self.logs.append(log_message)
        # 发送日志到外部处理器
        if self.log_handler:
            asyncio.create_task(self.emit_log(level, message))

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
    timeout: Optional[float] = None  # 超时时间（秒）
    retry: int = 0  # 重试次数
    retry_delay: int = 0  # 重试延迟（秒）
    description: Optional[str] = None  # 步骤描述
    labels: Dict[str, str] = Field(default_factory=dict)  # 标签
    outputs: Dict[str, Any] = Field(default_factory=dict)  # 输出结果
    retry_count: int = 0  # 当前重试次数
    duration: Optional[float] = None  # 执行时长（秒）
    context_manager: Optional[ContextManager] = None  # 上下文管理器
    cancelled: bool = Field(default=False)  # 取消标志

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

        # 创建步骤上下文
        step_context = context.create_child(self.name, "step")

        try:
            if self.context_manager:
                with self.context_manager.scope(step_context):
                    await self._run_with_retries(step_context)
            else:
                await self._run_with_retries(step_context)

        finally:
            self.finished_at = datetime.now()
            if self.started_at:
                self.duration = (self.finished_at - self.started_at).total_seconds()

    async def _run_with_retries(self, context: Context) -> None:
        """带重试的执行逻辑"""
        attempts = self.retry + 1
        last_error = None

        for attempt in range(attempts):
            try:
                self.retry_count = attempt
                if attempt > 0:
                    self.log(f"Retrying attempt {attempt + 1}/{attempts}")
                    await asyncio.sleep(self.retry_delay)

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
                if attempt < attempts - 1:
                    continue
                self.status = Status.FAILED
                self.error = last_error
                raise

    async def cancel(self) -> None:
        """取消步骤执行"""
        self.cancelled = True
        self.status = Status.CANCELLED
        self.log("Step cancelled", level="warning")

    @property
    def is_cancelled(self) -> bool:
        """是否已取消"""
        return self.cancelled

    async def _execute(self, context: Context) -> None:
        """执行步骤（由子类实现）"""
        if self.is_cancelled:
            raise RuntimeError("Step was cancelled")
        raise NotImplementedError()


class Stage(BaseComponent):
    """阶段基类"""
    name: str
    parallel: bool = False  # 是否并行执行
    status: Status = Status.PENDING
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    timeout: Optional[float] = None  # 超时时间（秒）
    description: Optional[str] = None  # 阶段描述
    labels: Dict[str, str] = Field(default_factory=dict)  # 标签
    steps: List[Step] = Field(default_factory=list)  # 步骤列表
    duration: Optional[float] = None  # 执行时长（秒）
    context_manager: Optional[ContextManager] = None  # 上下文管理器
    cancelled: bool = Field(default=False)  # 取消标志

    def add_step(self, step: Step) -> None:
        """添加步骤"""
        step.log_handler = self.log_handler  # 传递日志处理器
        step.context_manager = self.context_manager  # 传递上下文管理器
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
            "steps": [step.to_dict() for step in self.steps],
            "logs": self.logs
        }

    async def run(self, context: Context) -> None:
        """运行阶段"""
        self.status = Status.RUNNING
        self.started_at = datetime.now()

        # 创建阶段上下文
        stage_context = context.create_child(self.name, "stage")

        try:
            if self.context_manager:
                with self.context_manager.scope(stage_context):
                    await self._run_steps(stage_context)
            else:
                await self._run_steps(stage_context)

        finally:
            self.finished_at = datetime.now()
            if self.started_at:
                self.duration = (self.finished_at - self.started_at).total_seconds()

    async def _run_steps(self, context: Context) -> None:
        """执行步骤"""
        try:
            if self.parallel:
                tasks = []
                for step in self.steps:
                    if self.is_cancelled:
                        break
                    tasks.append(step.run(context))
                    
                if tasks:
                    if self.timeout:
                        try:
                            await asyncio.wait_for(
                                asyncio.gather(*tasks),
                                timeout=self.timeout
                            )
                        except asyncio.TimeoutError:
                            self.status = Status.TIMEOUT
                            self.error = f"Stage timed out after {self.timeout} seconds"
                            await self.cancel()  # 超时时取消所有步骤
                            # 设置所有未完成步骤的状态为超时
                            for step in self.steps:
                                if not step.status.is_finished:
                                    step.status = Status.TIMEOUT
                            raise
                    else:
                        await asyncio.gather(*tasks)
            else:
                for step in self.steps:
                    if self.is_cancelled:  # 检查取消状态
                        self.status = Status.CANCELLED
                        break
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
                            step.status = Status.TIMEOUT  # 确保步骤状态也设置为超时
                            raise
                    else:
                        await step.run(context)

            if self.is_cancelled:
                self.status = Status.CANCELLED
                return

            failed_steps = [step for step in self.steps if not step.status.is_successful]
            if failed_steps:
                self.status = Status.FAILED
                self.error = f"Steps failed: {', '.join(s.name for s in failed_steps)}"
            else:
                self.status = Status.SUCCESS

        except asyncio.TimeoutError:
            # 超时异常需要向上传播
            raise
        except Exception as e:
            # 如果是取消操作引起的异常，保持取消状态
            if self.is_cancelled:
                self.status = Status.CANCELLED
                return
            # 如果是其他已知状态，保持当前状态
            elif self.status not in [Status.FAILED, Status.TIMEOUT]:
                self.status = Status.FAILED
                self.error = str(e)
            raise

    async def cancel(self) -> None:
        """取消阶段执行"""
        self.cancelled = True
        self.status = Status.CANCELLED
        # 取消所有未完成的步骤
        cancel_tasks = []
        for step in self.steps:
            if not step.status.is_finished:
                cancel_tasks.append(step.cancel())
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks)
        self.log("Stage cancelled", level="warning")

    @property
    def is_cancelled(self) -> bool:
        """是否已取消"""
        return self.cancelled


class Pipeline(BaseComponent):
    """流水线基类"""
    name: str
    status: Status = Status.PENDING
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    timeout: Optional[float] = None  # 超时时间（秒）
    description: Optional[str] = None  # 流水线描述
    labels: Dict[str, str] = Field(default_factory=dict)  # 标签
    stages: List[Stage] = Field(default_factory=list)  # 阶段列表
    context: Context  # 执行上下文
    duration: Optional[float] = None  # 执行时长（秒）
    context_manager: ContextManager = Field(default_factory=ContextManager)  # 上下文管理器
    cancelled: bool = Field(default=False)  # 取消标志
    cancel_event: Optional[asyncio.Event] = None  # 取消事件

    def __init__(self, **data):
        super().__init__(**data)
        self.cancel_event = asyncio.Event()
        
    @property
    def is_cancelled(self) -> bool:
        """是否已取消"""
        return self.cancelled

    def add_stage(self, stage: Stage) -> None:
        """添加阶段"""
        stage.log_handler = self.log_handler  # 传递日志处理器
        stage.context_manager = self.context_manager  # 传递上下文管理器
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
                "outputs": self.context.outputs
            }
        }

    async def run(self) -> None:
        """运行流水线"""
        self.status = Status.RUNNING
        self.started_at = datetime.now()

        # 设置流水线上下文
        pipeline_context = self.context.create_child(self.name, "pipeline")

        try:
            with self.context_manager.scope(pipeline_context):
                await self._run_stages(pipeline_context)

        finally:
            self.finished_at = datetime.now()
            if self.started_at:
                self.duration = (self.finished_at - self.started_at).total_seconds()

    async def _run_stages(self, context: Context) -> None:
        """执行阶段"""
        try:
            for stage in self.stages:
                # 检查取消状态
                if self.is_cancelled or await self._check_cancelled():
                    self.status = Status.CANCELLED
                    self.log("Pipeline execution cancelled", level="warning")
                    # 取消当前阶段
                    await stage.cancel()
                    return

                try:
                    if self.timeout:
                        try:
                            await asyncio.wait_for(
                                stage.run(context),
                                timeout=self.timeout
                            )
                        except asyncio.TimeoutError:
                            self.status = Status.TIMEOUT
                            self.error = f"Pipeline timed out after {self.timeout} seconds"
                            await stage.cancel()
                            stage.status = Status.TIMEOUT  # 确保阶段状态也设置为超时
                            raise
                    else:
                        await stage.run(context)

                    # 每个阶段执行完后检查取消状态
                    if self.is_cancelled or await self._check_cancelled():
                        self.status = Status.CANCELLED
                        self.log("Pipeline execution cancelled", level="warning")
                        return

                    if not stage.status.is_successful:
                        self.status = Status.FAILED
                        self.error = f"Stage {stage.name} failed: {stage.error}"
                        return

                except asyncio.TimeoutError:
                    # 超时异常需要向上传播
                    raise
                except Exception as e:
                    if self.is_cancelled:
                        self.status = Status.CANCELLED
                        return
                    raise

            # 只有在没有取消的情况下才设置成功状态
            if not self.is_cancelled:
                self.status = Status.SUCCESS

        except asyncio.TimeoutError:
            # 超时异常需要向上传播
            raise
        except Exception as e:
            # 如果已经取消，保持取消状态
            if self.is_cancelled:
                self.status = Status.CANCELLED
                return
            # 如果已经设置了其他状态（如超时），保持该状态
            elif self.status not in [Status.FAILED, Status.TIMEOUT]:
                self.status = Status.FAILED
                self.error = str(e)
            raise

    async def _check_cancelled(self) -> bool:
        """检查是否已取消
        
        Returns:
            bool: 是否已取消
        """
        try:
            await asyncio.wait_for(
                self.cancel_event.wait(),
                timeout=0.1  # 100ms 超时
            )
            return True
        except asyncio.TimeoutError:
            return False

    async def cancel(self) -> None:
        """取消流水线执行"""
        if self.cancelled:
            return

        self.log("Cancelling pipeline execution", level="warning")
        self.cancelled = True
        self.status = Status.CANCELLED
        self.cancel_event.set()  # 设置取消事件

        try:
            # 取消所有未完成的阶段
            cancel_tasks = []
            for stage in self.stages:
                if not stage.status.is_finished:
                    cancel_tasks.append(stage.cancel())
            if cancel_tasks:
                await asyncio.gather(*cancel_tasks, return_exceptions=True)
        except Exception as e:
            self.error = f"Error during cancellation: {str(e)}"
            self.log(f"Cancellation error: {str(e)}", level="error")
            # 即使取消过程出错，也保持取消状态
            self.status = Status.CANCELLED

        self.log("Pipeline cancelled", level="warning")
