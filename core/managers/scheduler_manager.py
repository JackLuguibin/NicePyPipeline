from typing import Dict, List, Any, Optional, Callable
from core.base_manager import BaseManager
import asyncio
from datetime import datetime, timedelta
import uuid
import logging
from enum import Enum
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import croniter
import pytz

class ScheduleType(Enum):
    CRON = "cron"
    INTERVAL = "interval"
    DATETIME = "datetime"

class ScheduleStatus(Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    ERROR = "error"

class ScheduleInfo(BaseModel):
    id: str
    name: str
    type: ScheduleType
    status: ScheduleStatus
    schedule: dict
    pipeline_id: str
    created_at: datetime
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    error: Optional[str] = None

class ScheduleCreateRequest(BaseModel):
    name: str
    type: ScheduleType
    schedule: dict  # For CRON: {"expression": "* * * * *"}, For INTERVAL: {"minutes": 5}, For DATETIME: {"datetime": "2023-12-31T23:59:59"}
    pipeline_id: str

class Schedule:
    def __init__(self, name: str, schedule_type: ScheduleType, schedule: dict, pipeline_id: str):
        self.id = str(uuid.uuid4())
        self.name = name
        self.type = schedule_type
        self.status = ScheduleStatus.ACTIVE
        self.schedule = schedule
        self.pipeline_id = pipeline_id
        self.created_at = datetime.now()
        self.next_run: Optional[datetime] = None
        self.last_run: Optional[datetime] = None
        self.error: Optional[str] = None
        self._task: Optional[asyncio.Task] = None
        
        # 计算下次运行时间
        self._calculate_next_run()
        
    def _calculate_next_run(self) -> None:
        """计算下次运行时间"""
        now = datetime.now(pytz.UTC)
        
        if self.type == ScheduleType.CRON:
            cron = croniter.croniter(self.schedule["expression"], now)
            self.next_run = cron.get_next(datetime)
        elif self.type == ScheduleType.INTERVAL:
            minutes = self.schedule.get("minutes", 0)
            hours = self.schedule.get("hours", 0)
            days = self.schedule.get("days", 0)
            
            if not any([minutes, hours, days]):
                raise ValueError("Interval schedule must specify minutes, hours, or days")
                
            delta = timedelta(
                minutes=minutes,
                hours=hours,
                days=days
            )
            
            self.next_run = (self.last_run or now) + delta
        elif self.type == ScheduleType.DATETIME:
            self.next_run = datetime.fromisoformat(self.schedule["datetime"])
            if self.next_run.tzinfo is None:
                self.next_run = pytz.UTC.localize(self.next_run)
                
    def to_info(self) -> ScheduleInfo:
        return ScheduleInfo(
            id=self.id,
            name=self.name,
            type=self.type,
            status=self.status,
            schedule=self.schedule,
            pipeline_id=self.pipeline_id,
            created_at=self.created_at,
            next_run=self.next_run,
            last_run=self.last_run,
            error=self.error
        )

class SchedulerManager(BaseManager):
    def __init__(self, app: FastAPI):
        super().__init__(app, "scheduler")
        self._schedules: Dict[str, Schedule] = {}
        self.logger = logging.getLogger(__name__)
        
    def setup_routes(self) -> None:
        """设置路由"""
        self._router.get("/list")(self.list_schedules)
        self._router.post("/create")(self.create_schedule)
        self._router.get("/{schedule_id}")(self.get_schedule)
        self._router.post("/{schedule_id}/pause")(self.pause_schedule)
        self._router.post("/{schedule_id}/resume")(self.resume_schedule)
        self._router.delete("/{schedule_id}")(self.delete_schedule)
        
    async def _initialize(self) -> None:
        """初始化调度管理器"""
        self.logger.info("Initializing scheduler manager")
        
    async def _start(self) -> None:
        """启动调度管理器"""
        self.logger.info("Starting scheduler manager")
        # 启动调度循环
        asyncio.create_task(self._schedule_loop())
        
    async def _stop(self) -> None:
        """停止调度管理器"""
        self.logger.info("Stopping scheduler manager")
        # 停止所有调度任务
        for schedule in self._schedules.values():
            if schedule._task and not schedule._task.done():
                schedule._task.cancel()
                
    async def _cleanup(self) -> None:
        """清理调度管理器"""
        self.logger.info("Cleaning up scheduler manager")
        await self._stop()
        self._schedules.clear()
        
    async def _schedule_loop(self) -> None:
        """调度循环"""
        while True:
            try:
                now = datetime.now(pytz.UTC)
                
                # 检查所有活动的调度
                for schedule in self._schedules.values():
                    if (schedule.status == ScheduleStatus.ACTIVE and
                        schedule.next_run and
                        schedule.next_run <= now):
                        # 创建执行任务
                        schedule._task = asyncio.create_task(
                            self._execute_schedule(schedule)
                        )
                        
                # 每分钟检查一次
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in schedule loop: {str(e)}")
                await asyncio.sleep(60)
                
    async def _execute_schedule(self, schedule: Schedule) -> None:
        """执行调度任务"""
        try:
            schedule.last_run = datetime.now(pytz.UTC)
            # 这里应该调用流水线管理器启动流水线
            # await pipeline_manager.start_pipeline(schedule.pipeline_id)
            schedule._calculate_next_run()
            
            # 对于一次性的datetime调度，执行后标记为完成
            if schedule.type == ScheduleType.DATETIME:
                schedule.status = ScheduleStatus.COMPLETED
                
        except Exception as e:
            schedule.error = str(e)
            schedule.status = ScheduleStatus.ERROR
            self.logger.error(f"Error executing schedule {schedule.id}: {str(e)}")
            
    # API路由处理器
    async def list_schedules(self) -> List[ScheduleInfo]:
        """列出所有调度"""
        return [schedule.to_info() for schedule in self._schedules.values()]
        
    async def create_schedule(self, request: ScheduleCreateRequest) -> ScheduleInfo:
        """创建调度"""
        try:
            schedule = Schedule(
                name=request.name,
                schedule_type=request.type,
                schedule=request.schedule,
                pipeline_id=request.pipeline_id
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
            
        self._schedules[schedule.id] = schedule
        return schedule.to_info()
        
    async def get_schedule(self, schedule_id: str) -> ScheduleInfo:
        """获取调度信息"""
        schedule = self._schedules.get(schedule_id)
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"Schedule {schedule_id} not found")
        return schedule.to_info()
        
    async def pause_schedule(self, schedule_id: str) -> ScheduleInfo:
        """暂停调度"""
        schedule = self._schedules.get(schedule_id)
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"Schedule {schedule_id} not found")
            
        if schedule.status == ScheduleStatus.ACTIVE:
            schedule.status = ScheduleStatus.PAUSED
            if schedule._task and not schedule._task.done():
                schedule._task.cancel()
                
        return schedule.to_info()
        
    async def resume_schedule(self, schedule_id: str) -> ScheduleInfo:
        """恢复调度"""
        schedule = self._schedules.get(schedule_id)
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"Schedule {schedule_id} not found")
            
        if schedule.status == ScheduleStatus.PAUSED:
            schedule.status = ScheduleStatus.ACTIVE
            schedule._calculate_next_run()
            
        return schedule.to_info()
        
    async def delete_schedule(self, schedule_id: str) -> dict:
        """删除调度"""
        schedule = self._schedules.get(schedule_id)
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"Schedule {schedule_id} not found")
            
        if schedule._task and not schedule._task.done():
            schedule._task.cancel()
            
        del self._schedules[schedule_id]
        return {"status": "success", "message": f"Schedule {schedule_id} deleted"} 