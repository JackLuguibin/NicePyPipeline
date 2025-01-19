from typing import Dict, Type, Optional
from core.base_manager import BaseManager
import asyncio
import logging
from fastapi import FastAPI

class App:
    def __init__(self, app: FastAPI):
        self.app = app
        self._managers: Dict[str, BaseManager] = {}
        self._initialized = False
        self._running = False
        self.logger = logging.getLogger(__name__)
        
    def register_manager(self, manager: BaseManager) -> None:
        """注册管理器"""
        if manager.name in self._managers:
            raise ValueError(f"Manager {manager.name} already registered")
        self._managers[manager.name] = manager
        
    async def initialize(self) -> None:
        """初始化所有管理器"""
        if self._initialized:
            return
            
        self.logger.info("Initializing application...")
        for name, manager in self._managers.items():
            self.logger.info(f"Initializing manager: {name}")
            await manager.initialize()
            
        self._initialized = True
        self.logger.info("Application initialized successfully")
        
    async def start(self) -> None:
        """启动所有管理器"""
        if self._running:
            return
            
        if not self._initialized:
            await self.initialize()
            
        self.logger.info("Starting application...")
        for name, manager in self._managers.items():
            self.logger.info(f"Starting manager: {name}")
            await manager.start()
            
        self._running = True
        self.logger.info("Application started successfully")
        
    async def stop(self) -> None:
        """停止所有管理器"""
        if not self._running:
            return
            
        self.logger.info("Stopping application...")
        for name, manager in reversed(list(self._managers.items())):
            self.logger.info(f"Stopping manager: {name}")
            await manager.stop()
            
        self._running = False
        self.logger.info("Application stopped successfully")
        
    async def cleanup(self) -> None:
        """清理所有管理器"""
        self.logger.info("Cleaning up application...")
        await self.stop()
        
        for name, manager in reversed(list(self._managers.items())):
            self.logger.info(f"Cleaning up manager: {name}")
            await manager.cleanup()
            
        self._managers.clear()
        self._initialized = False
        self.logger.info("Application cleanup completed") 