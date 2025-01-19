from typing import Optional
import logging
from fastapi import FastAPI

class BaseManager:
    """基础管理器类"""
    def __init__(self, app: Optional[FastAPI] = None):
        self._app = app
        self._logger = logging.getLogger(self.__class__.__name__)
        
    async def initialize(self) -> None:
        """初始化管理器"""
        await self._initialize()
        
    async def _initialize(self) -> None:
        """初始化（由子类实现）"""
        pass 