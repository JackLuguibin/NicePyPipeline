from typing import Dict, Any, Optional, List
from core.base_manager import BaseManager
import asyncio
from datetime import datetime
import uuid
import logging
from enum import Enum
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class TaskBase(BaseModel):
    name: str
    
class TaskInfo(TaskBase):
    id: str
    status: TaskStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Any] = None
    error: Optional[str] = None

class Task:
    def __init__(self, name: str, func: callable, args: tuple = (), kwargs: dict = None):
        self.id = str(uuid.uuid4())
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs or {}
        self.status = TaskStatus.PENDING
        self.created_at = datetime.now()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.result: Any = None
        self.error: Optional[Exception] = None
        
    def to_info(self) -> TaskInfo:
        return TaskInfo(
            id=self.id,
            name=self.name,
            status=self.status,
            created_at=self.created_at,
            started_at=self.started_at,
            completed_at=self.completed_at,
            result=self.result,
            error=str(self.error) if self.error else None
        )

class TaskManager(BaseManager):
    def __init__(self, app: FastAPI):
        super().__init__(app, "task")
        self._tasks: Dict[str, Task] = {}
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self.logger = logging.getLogger(__name__)
        
    def setup_routes(self) -> None:
        """设置路由"""
        self._router.get("/list")(self.list_tasks_route)
        self._router.get("/list/{status}")(self.list_tasks_by_status_route)
        self._router.get("/{task_id}")(self.get_task_route)
        self._router.post("/{task_id}/cancel")(self.cancel_task_route)
        
    async def _initialize(self) -> None:
        """初始化任务管理器"""
        self.logger.info("Initializing task manager")
        
    async def _start(self) -> None:
        """启动任务管理器"""
        self.logger.info("Starting task manager")
        
    async def _stop(self) -> None:
        """停止任务管理器"""
        self.logger.info("Stopping task manager")
        # 取消所有运行中的任务
        for task in self._running_tasks.values():
            task.cancel()
        await asyncio.gather(*self._running_tasks.values(), return_exceptions=True)
        
    async def _cleanup(self) -> None:
        """清理任务管理器"""
        self.logger.info("Cleaning up task manager")
        await self._stop()
        self._tasks.clear()
        self._running_tasks.clear()
        
    async def submit_task(self, name: str, func: callable, *args, **kwargs) -> str:
        """提交新任务"""
        task = Task(name, func, args, kwargs)
        self._tasks[task.id] = task
        
        async def _run_task():
            try:
                task.started_at = datetime.now()
                task.status = TaskStatus.RUNNING
                task.result = await func(*args, **kwargs)
                task.status = TaskStatus.COMPLETED
            except Exception as e:
                task.error = e
                task.status = TaskStatus.FAILED
                self.logger.error(f"Task {task.id} failed: {str(e)}")
            finally:
                task.completed_at = datetime.now()
                self._running_tasks.pop(task.id, None)
                
        self._running_tasks[task.id] = asyncio.create_task(_run_task())
        return task.id
        
    def get_task(self, task_id: str) -> Optional[Task]:
        """获取任务信息"""
        return self._tasks.get(task_id)
        
    def list_tasks(self, status: Optional[TaskStatus] = None) -> List[Task]:
        """列出所有任务"""
        if status is None:
            return list(self._tasks.values())
        return [task for task in self._tasks.values() if task.status == status]
        
    async def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        task = self._tasks.get(task_id)
        if task is None:
            return False
            
        running_task = self._running_tasks.get(task_id)
        if running_task and not running_task.done():
            running_task.cancel()
            task.status = TaskStatus.CANCELLED
            task.completed_at = datetime.now()
            return True
        return False
        
    # API路由处理器
    async def list_tasks_route(self) -> List[TaskInfo]:
        """列出所有任务的路由处理器"""
        tasks = self.list_tasks()
        return [task.to_info() for task in tasks]
        
    async def list_tasks_by_status_route(self, status: TaskStatus) -> List[TaskInfo]:
        """列出指定状态任务的路由处理器"""
        tasks = self.list_tasks(status)
        return [task.to_info() for task in tasks]
        
    async def get_task_route(self, task_id: str) -> TaskInfo:
        """获取任务信息的路由处理器"""
        task = self.get_task(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        return task.to_info()
        
    async def cancel_task_route(self, task_id: str) -> dict:
        """取消任务的路由处理器"""
        success = await self.cancel_task(task_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found or already completed")
        return {"status": "success", "message": f"Task {task_id} cancelled"} 