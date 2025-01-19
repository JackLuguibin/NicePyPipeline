from typing import Dict, List, Any, Optional, Callable
from core.base_manager import BaseManager
import asyncio
from datetime import datetime
import uuid
import logging
from enum import Enum
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import re

class TriggerType(Enum):
    CRON = "cron"
    WEBHOOK = "webhook"
    EVENT = "event"
    GIT = "git"
    FILE = "file"

class TriggerStatus(Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"

class TriggerInfo(BaseModel):
    id: str
    name: str
    type: TriggerType
    status: TriggerStatus
    condition: dict
    created_at: datetime
    last_triggered: Optional[datetime] = None
    pipeline_id: str

class TriggerCreateRequest(BaseModel):
    name: str
    type: TriggerType
    condition: dict
    pipeline_id: str

class Trigger:
    def __init__(self, name: str, trigger_type: TriggerType, condition: dict, pipeline_id: str):
        self.id = str(uuid.uuid4())
        self.name = name
        self.type = trigger_type
        self.status = TriggerStatus.ENABLED
        self.condition = condition
        self.created_at = datetime.now()
        self.last_triggered: Optional[datetime] = None
        self.pipeline_id = pipeline_id
        self._task: Optional[asyncio.Task] = None
        
    def to_info(self) -> TriggerInfo:
        return TriggerInfo(
            id=self.id,
            name=self.name,
            type=self.type,
            status=self.status,
            condition=self.condition,
            created_at=self.created_at,
            last_triggered=self.last_triggered,
            pipeline_id=self.pipeline_id
        )

class TriggerManager(BaseManager):
    def __init__(self, app: FastAPI):
        super().__init__(app, "trigger")
        self._triggers: Dict[str, Trigger] = {}
        self._event_handlers: Dict[str, List[Callable]] = {}
        self.logger = logging.getLogger(__name__)
        
    def setup_routes(self) -> None:
        """设置路由"""
        self._router.get("/list")(self.list_triggers)
        self._router.post("/create")(self.create_trigger)
        self._router.get("/{trigger_id}")(self.get_trigger)
        self._router.post("/{trigger_id}/enable")(self.enable_trigger)
        self._router.post("/{trigger_id}/disable")(self.disable_trigger)
        self._router.delete("/{trigger_id}")(self.delete_trigger)
        self._router.post("/webhook/{trigger_id}")(self.handle_webhook)
        
    async def _initialize(self) -> None:
        """初始化触发器管理器"""
        self.logger.info("Initializing trigger manager")
        
    async def _start(self) -> None:
        """启动触发器管理器"""
        self.logger.info("Starting trigger manager")
        # 启动所有已启用的触发器
        for trigger in self._triggers.values():
            if trigger.status == TriggerStatus.ENABLED:
                await self._start_trigger(trigger)
        
    async def _stop(self) -> None:
        """停止触发器管理器"""
        self.logger.info("Stopping trigger manager")
        # 停止所有触发器
        for trigger in self._triggers.values():
            await self._stop_trigger(trigger)
        
    async def _cleanup(self) -> None:
        """清理触发器管理器"""
        self.logger.info("Cleaning up trigger manager")
        await self._stop()
        self._triggers.clear()
        self._event_handlers.clear()
        
    async def _start_trigger(self, trigger: Trigger) -> None:
        """启动触发器"""
        if trigger.type == TriggerType.CRON:
            trigger._task = asyncio.create_task(self._run_cron_trigger(trigger))
        elif trigger.type == TriggerType.EVENT:
            event_name = trigger.condition.get("event")
            if event_name:
                self._register_event_handler(event_name, lambda: self._handle_trigger(trigger))
                
    async def _stop_trigger(self, trigger: Trigger) -> None:
        """停止触发器"""
        if trigger._task and not trigger._task.done():
            trigger._task.cancel()
            try:
                await trigger._task
            except asyncio.CancelledError:
                pass
                
    async def _run_cron_trigger(self, trigger: Trigger) -> None:
        """运行CRON触发器"""
        cron_expr = trigger.condition.get("cron")
        if not cron_expr:
            return
            
        while True:
            try:
                # 这里应该实现CRON表达式的解析和调度
                # 为简单起见，这里只是示例
                await asyncio.sleep(60)
                await self._handle_trigger(trigger)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in cron trigger {trigger.id}: {str(e)}")
                
    def _register_event_handler(self, event_name: str, handler: Callable) -> None:
        """注册事件处理器"""
        if event_name not in self._event_handlers:
            self._event_handlers[event_name] = []
        self._event_handlers[event_name].append(handler)
        
    async def handle_event(self, event_name: str, event_data: Any = None) -> None:
        """处理事件"""
        handlers = self._event_handlers.get(event_name, [])
        for handler in handlers:
            try:
                await handler(event_data)
            except Exception as e:
                self.logger.error(f"Error handling event {event_name}: {str(e)}")
                
    async def _handle_trigger(self, trigger: Trigger) -> None:
        """处理触发器"""
        try:
            # 这里应该调用流水线管理器启动流水线
            # pipeline_manager.start_pipeline(trigger.pipeline_id)
            trigger.last_triggered = datetime.now()
        except Exception as e:
            self.logger.error(f"Error handling trigger {trigger.id}: {str(e)}")
            
    # API路由处理器
    async def list_triggers(self) -> List[TriggerInfo]:
        """列出所有触发器"""
        return [trigger.to_info() for trigger in self._triggers.values()]
        
    async def create_trigger(self, request: TriggerCreateRequest) -> TriggerInfo:
        """创建触发器"""
        trigger = Trigger(
            name=request.name,
            trigger_type=request.type,
            condition=request.condition,
            pipeline_id=request.pipeline_id
        )
        self._triggers[trigger.id] = trigger
        
        if trigger.status == TriggerStatus.ENABLED:
            await self._start_trigger(trigger)
            
        return trigger.to_info()
        
    async def get_trigger(self, trigger_id: str) -> TriggerInfo:
        """获取触发器信息"""
        trigger = self._triggers.get(trigger_id)
        if trigger is None:
            raise HTTPException(status_code=404, detail=f"Trigger {trigger_id} not found")
        return trigger.to_info()
        
    async def enable_trigger(self, trigger_id: str) -> TriggerInfo:
        """启用触发器"""
        trigger = self._triggers.get(trigger_id)
        if trigger is None:
            raise HTTPException(status_code=404, detail=f"Trigger {trigger_id} not found")
            
        if trigger.status == TriggerStatus.DISABLED:
            trigger.status = TriggerStatus.ENABLED
            await self._start_trigger(trigger)
            
        return trigger.to_info()
        
    async def disable_trigger(self, trigger_id: str) -> TriggerInfo:
        """禁用触发器"""
        trigger = self._triggers.get(trigger_id)
        if trigger is None:
            raise HTTPException(status_code=404, detail=f"Trigger {trigger_id} not found")
            
        if trigger.status == TriggerStatus.ENABLED:
            trigger.status = TriggerStatus.DISABLED
            await self._stop_trigger(trigger)
            
        return trigger.to_info()
        
    async def delete_trigger(self, trigger_id: str) -> dict:
        """删除触发器"""
        trigger = self._triggers.get(trigger_id)
        if trigger is None:
            raise HTTPException(status_code=404, detail=f"Trigger {trigger_id} not found")
            
        await self._stop_trigger(trigger)
        del self._triggers[trigger_id]
        return {"status": "success", "message": f"Trigger {trigger_id} deleted"}
        
    async def handle_webhook(self, trigger_id: str, data: dict) -> dict:
        """处理Webhook触发"""
        trigger = self._triggers.get(trigger_id)
        if trigger is None:
            raise HTTPException(status_code=404, detail=f"Trigger {trigger_id} not found")
            
        if trigger.type != TriggerType.WEBHOOK:
            raise HTTPException(status_code=400, detail="Not a webhook trigger")
            
        if trigger.status != TriggerStatus.ENABLED:
            raise HTTPException(status_code=400, detail="Trigger is disabled")
            
        # 验证webhook数据是否匹配条件
        if not self._validate_webhook_condition(trigger.condition, data):
            raise HTTPException(status_code=400, detail="Webhook data does not match conditions")
            
        await self._handle_trigger(trigger)
        return {"status": "success", "message": "Webhook processed"}
        
    def _validate_webhook_condition(self, condition: dict, data: dict) -> bool:
        """验证webhook数据是否匹配条件"""
        for key, pattern in condition.items():
            value = data.get(key)
            if value is None:
                return False
            if not re.match(pattern, str(value)):
                return False
        return True 