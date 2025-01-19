from typing import Dict, Any, Optional, List
from core.base_manager import BaseManager
import asyncio
from asyncio import Queue
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

class QueueInfo(BaseModel):
    name: str
    size: int
    maxsize: int

class QueueCreateRequest(BaseModel):
    name: str
    maxsize: int = 0

class QueueManager(BaseManager):
    def __init__(self, app: FastAPI):
        super().__init__(app, "queue")
        self._queues: Dict[str, Queue] = {}
        self.logger = logging.getLogger(__name__)
        
    def setup_routes(self) -> None:
        """设置路由"""
        self._router.get("/list")(self.list_queues)
        self._router.post("/create")(self.create_queue_route)
        self._router.get("/{queue_name}/size")(self.get_queue_size)
        self._router.post("/{queue_name}/put")(self.put_item)
        self._router.get("/{queue_name}/get")(self.get_item)
        
    async def _initialize(self) -> None:
        """初始化队列管理器"""
        self.logger.info("Initializing queue manager")
        # 创建默认队列
        await self.create_queue("default")
        await self.create_queue("high_priority")
        await self.create_queue("low_priority")
        
    async def _start(self) -> None:
        """启动队列管理器"""
        self.logger.info("Starting queue manager")
        
    async def _stop(self) -> None:
        """停止队列管理器"""
        self.logger.info("Stopping queue manager")
        
    async def _cleanup(self) -> None:
        """清理队列管理器"""
        self.logger.info("Cleaning up queue manager")
        self._queues.clear()
        
    async def create_queue(self, name: str, maxsize: int = 0) -> Queue:
        """创建新队列"""
        if name in self._queues:
            raise ValueError(f"Queue {name} already exists")
        queue = Queue(maxsize=maxsize)
        self._queues[name] = queue
        return queue
        
    async def get_queue(self, name: str) -> Optional[Queue]:
        """获取指定队列"""
        return self._queues.get(name)
        
    async def put(self, queue_name: str, item: Any) -> None:
        """向指定队列添加项目"""
        queue = await self.get_queue(queue_name)
        if queue is None:
            raise ValueError(f"Queue {queue_name} does not exist")
        await queue.put(item)
        
    async def get(self, queue_name: str) -> Any:
        """从指定队列获取项目"""
        queue = await self.get_queue(queue_name)
        if queue is None:
            raise ValueError(f"Queue {queue_name} does not exist")
        return await queue.get()
        
    def queue_size(self, queue_name: str) -> int:
        """获取指定队列的大小"""
        queue = self._queues.get(queue_name)
        if queue is None:
            raise ValueError(f"Queue {queue_name} does not exist")
        return queue.qsize()
        
    # API路由处理器
    async def list_queues(self) -> List[QueueInfo]:
        """列出所有队列"""
        return [
            QueueInfo(
                name=name,
                size=queue.qsize(),
                maxsize=queue._maxsize
            )
            for name, queue in self._queues.items()
        ]
        
    async def create_queue_route(self, request: QueueCreateRequest) -> QueueInfo:
        """创建队列的路由处理器"""
        try:
            queue = await self.create_queue(request.name, request.maxsize)
            return QueueInfo(
                name=request.name,
                size=queue.qsize(),
                maxsize=queue._maxsize
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
            
    async def get_queue_size(self, queue_name: str) -> int:
        """获取队列大小的路由处理器"""
        try:
            return self.queue_size(queue_name)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
            
    async def put_item(self, queue_name: str, item: Any) -> dict:
        """添加项目的路由处理器"""
        try:
            await self.put(queue_name, item)
            return {"status": "success", "message": "Item added to queue"}
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
            
    async def get_item(self, queue_name: str) -> Any:
        """获取项目的路由处理器"""
        try:
            item = await self.get(queue_name)
            return {"item": item}
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e)) 