from typing import Dict, Any, Optional, Type
from core.base_manager import BaseManager
import asyncio
from datetime import datetime
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import AsyncAdaptedQueuePool
from contextlib import asynccontextmanager
import alembic.config
import os
from pathlib import Path

# 创建Base类
Base = declarative_base()

class DatabaseInfo(BaseModel):
    name: str
    url: str
    pool_size: int
    max_overflow: int
    status: str
    connected_at: Optional[datetime] = None

class DatabaseManager(BaseManager):
    def __init__(self, app: FastAPI, config_manager):
        super().__init__(app, "database")
        self.config_manager = config_manager
        self._engines: Dict[str, AsyncEngine] = {}
        self._session_factories: Dict[str, sessionmaker] = {}
        self.logger = logging.getLogger(__name__)
        
    def setup_routes(self) -> None:
        """设置路由"""
        self._router.get("/list")(self.list_databases)
        self._router.get("/status/{name}")(self.get_database_status)
        self._router.post("/migrate")(self.run_migrations)
        
    async def _initialize(self) -> None:
        """初始化数据库管理器"""
        self.logger.info("Initializing database manager")
        
        # 创建数据库目录
        db_dir = Path("data")
        db_dir.mkdir(exist_ok=True)
        
        # 创建迁移目录
        migrations_dir = Path("migrations")
        migrations_dir.mkdir(exist_ok=True)
        
        # 初始化默认数据库
        await self.setup_database(
            "default",
            self.config_manager.get_value("database.url"),
            self.config_manager.get_value("database.pool_size", 5),
            self.config_manager.get_value("database.max_overflow", 10)
        )
        
    async def _start(self) -> None:
        """启动数据库管理器"""
        self.logger.info("Starting database manager")
        
    async def _stop(self) -> None:
        """停止数据库管理器"""
        self.logger.info("Stopping database manager")
        # 关闭所有数据库连接
        for engine in self._engines.values():
            await engine.dispose()
            
    async def _cleanup(self) -> None:
        """清理数据库管理器"""
        self.logger.info("Cleaning up database manager")
        await self._stop()
        self._engines.clear()
        self._session_factories.clear()
        
    async def setup_database(
        self,
        name: str,
        url: str,
        pool_size: int = 5,
        max_overflow: int = 10
    ) -> None:
        """设置数据库连接"""
        try:
            # 创建异步引擎
            engine = create_async_engine(
                url,
                poolclass=AsyncAdaptedQueuePool,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_pre_ping=True  # 自动检测断开的连接
            )
            
            # 创建session工厂
            session_factory = sessionmaker(
                engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            self._engines[name] = engine
            self._session_factories[name] = session_factory
            
            # 测试连接
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                
        except Exception as e:
            self.logger.error(f"Failed to setup database {name}: {str(e)}")
            raise
            
    @asynccontextmanager
    async def get_session(self, database: str = "default") -> AsyncSession:
        """获取数据库会话"""
        if database not in self._session_factories:
            raise ValueError(f"Database {database} not configured")
            
        session_factory = self._session_factories[database]
        session = session_factory()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
            
    async def execute_query(
        self,
        query: Any,
        database: str = "default",
        **kwargs
    ) -> Any:
        """执行查询"""
        async with self.get_session(database) as session:
            result = await session.execute(query, **kwargs)
            return result
            
    async def run_migrations(self, database: str = "default") -> dict:
        """运行数据库迁移"""
        try:
            # 设置Alembic配置
            alembic_cfg = alembic.config.Config()
            alembic_cfg.set_main_option("script_location", "migrations")
            alembic_cfg.set_main_option(
                "sqlalchemy.url",
                str(self._engines[database].url)
            )
            
            # 运行迁移
            with alembic.config.Config().with_config(alembic_cfg) as config:
                alembic.command.upgrade(config, "head")
                
            return {
                "status": "success",
                "message": f"Successfully migrated database {database}"
            }
        except Exception as e:
            self.logger.error(f"Migration failed: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Migration failed: {str(e)}"
            )
            
    # API路由处理器
    async def list_databases(self) -> list[DatabaseInfo]:
        """列出所有数据库"""
        result = []
        for name, engine in self._engines.items():
            try:
                # 测试连接
                async with engine.begin() as conn:
                    await conn.execute("SELECT 1")
                    status = "connected"
            except Exception:
                status = "error"
                
            result.append(
                DatabaseInfo(
                    name=name,
                    url=str(engine.url),
                    pool_size=engine.pool.size(),
                    max_overflow=engine.pool._max_overflow,
                    status=status,
                    connected_at=datetime.now() if status == "connected" else None
                )
            )
        return result
        
    async def get_database_status(self, name: str) -> DatabaseInfo:
        """获取数据库状态"""
        if name not in self._engines:
            raise HTTPException(
                status_code=404,
                detail=f"Database {name} not found"
            )
            
        engine = self._engines[name]
        try:
            # 测试连接
            async with engine.begin() as conn:
                await conn.execute("SELECT 1")
                status = "connected"
        except Exception:
            status = "error"
            
        return DatabaseInfo(
            name=name,
            url=str(engine.url),
            pool_size=engine.pool.size(),
            max_overflow=engine.pool._max_overflow,
            status=status,
            connected_at=datetime.now() if status == "connected" else None
        )
        
    # 实用方法
    def get_model_class(self, model_name: str) -> Type[Base]:
        """获取模型类"""
        for cls in Base.__subclasses__():
            if cls.__name__ == model_name:
                return cls
        raise ValueError(f"Model {model_name} not found")
        
    async def create_tables(self, database: str = "default") -> None:
        """创建所有表"""
        engine = self._engines[database]
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
    async def drop_tables(self, database: str = "default") -> None:
        """删除所有表"""
        engine = self._engines[database]
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all) 