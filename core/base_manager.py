from abc import ABC, abstractmethod
from typing import Optional
import asyncio
from enum import Enum
from fastapi import FastAPI, APIRouter

class ManagerState(Enum):
    INITIALIZED = "initialized"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    DISABLED = "disabled"

class BaseManager(ABC):
    def __init__(self, app: FastAPI, name: str):
        self.app = app
        self.name = name
        self._state = ManagerState.INITIALIZED
        self._lock = asyncio.Lock()
        self._router = APIRouter(prefix=f"/{name}", tags=[name])
        
    @property
    def state(self) -> ManagerState:
        return self._state
        
    def register_routes(self) -> None:
        """注册路由到FastAPI实例"""
        self.setup_routes()
        self.app.include_router(self._router)
        
    @abstractmethod
    def setup_routes(self) -> None:
        """设置路由
        子类应该在此方法中使用self._router定义路由
        例如：self._router.get("/")(self.some_route_handler)
        """
        pass
        
    async def initialize(self) -> None:
        """初始化管理器"""
        async with self._lock:
            await self._initialize()
            self.register_routes()
            
    @abstractmethod
    async def _initialize(self) -> None:
        """具体初始化实现"""
        pass
        
    async def start(self) -> None:
        """启动管理器"""
        async with self._lock:
            if self._state != ManagerState.INITIALIZED:
                raise RuntimeError(f"Manager {self.name} is not in initialized state")
            self._state = ManagerState.STARTING
            await self._start()
            self._state = ManagerState.RUNNING
            
    @abstractmethod
    async def _start(self) -> None:
        """具体启动实现"""
        pass
        
    async def stop(self) -> None:
        """停止管理器"""
        async with self._lock:
            if self._state != ManagerState.RUNNING:
                return
            self._state = ManagerState.STOPPING
            await self._stop()
            self._state = ManagerState.STOPPED
            
    @abstractmethod
    async def _stop(self) -> None:
        """具体停止实现"""
        pass
        
    async def enable(self) -> None:
        """启用管理器"""
        async with self._lock:
            if self._state == ManagerState.DISABLED:
                self._state = ManagerState.INITIALIZED
                await self.start()
                
    async def disable(self) -> None:
        """禁用管理器"""
        async with self._lock:
            await self.stop()
            self._state = ManagerState.DISABLED
            
    async def cleanup(self) -> None:
        """清理资源"""
        async with self._lock:
            await self.stop()
            await self._cleanup()
            
    @abstractmethod
    async def _cleanup(self) -> None:
        """具体清理实现"""
        pass 