from typing import Dict, Any, Optional
from core.base_manager import BaseManager
import asyncio
from datetime import datetime
import logging
from enum import Enum
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import yaml
import os
from pathlib import Path
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class ConfigScope(Enum):
    SYSTEM = "system"
    PLUGIN = "plugin"
    USER = "user"

class ConfigValue(BaseModel):
    value: Any
    scope: ConfigScope
    last_modified: datetime
    description: Optional[str] = None

class ConfigInfo(BaseModel):
    key: str
    value: Any
    scope: ConfigScope
    last_modified: datetime
    description: Optional[str] = None

class ConfigUpdateRequest(BaseModel):
    value: Any
    description: Optional[str] = None

class ConfigFileHandler(FileSystemEventHandler):
    def __init__(self, config_manager):
        self.config_manager = config_manager
        
    def on_modified(self, event):
        if event.src_path.endswith('.yaml'):
            asyncio.create_task(self.config_manager.reload_config())

class ConfigManager(BaseManager):
    def __init__(self, app: FastAPI):
        super().__init__(app, "config")
        self._config: Dict[str, ConfigValue] = {}
        self._config_file = Path("config/config.yaml")
        self._observer: Optional[Observer] = None
        self.logger = logging.getLogger(__name__)
        
    def setup_routes(self) -> None:
        """设置路由"""
        self._router.get("/list")(self.list_configs)
        self._router.get("/get/{key}")(self.get_config)
        self._router.post("/set/{key}")(self.set_config)
        self._router.delete("/{key}")(self.delete_config)
        self._router.post("/reload")(self.reload_config)
        
    async def _initialize(self) -> None:
        """初始化配置管理器"""
        self.logger.info("Initializing config manager")
        # 创建配置目录
        self._config_file.parent.mkdir(exist_ok=True)
        
        # 如果配置文件不存在，创建默认配置
        if not self._config_file.exists():
            await self._create_default_config()
            
        # 加载配置
        await self.reload_config()
        
        # 设置文件监视器
        self._setup_file_watcher()
        
    async def _start(self) -> None:
        """启动配置管理器"""
        self.logger.info("Starting config manager")
        if self._observer:
            self._observer.start()
        
    async def _stop(self) -> None:
        """停止配置管理器"""
        self.logger.info("Stopping config manager")
        if self._observer:
            self._observer.stop()
            self._observer.join()
        
    async def _cleanup(self) -> None:
        """清理配置管理器"""
        self.logger.info("Cleaning up config manager")
        await self._stop()
        self._config.clear()
        
    def _setup_file_watcher(self) -> None:
        """设置配置文件监视器"""
        self._observer = Observer()
        handler = ConfigFileHandler(self)
        self._observer.schedule(
            handler,
            str(self._config_file.parent),
            recursive=False
        )
        
    async def _create_default_config(self) -> None:
        """创建默认配置文件"""
        default_config = {
            "system": {
                "host": "0.0.0.0",
                "port": 8000,
                "debug": False,
                "log_level": "INFO",
                "secret_key": os.urandom(24).hex()
            },
            "database": {
                "url": "sqlite+aiosqlite:///data/app.db",
                "pool_size": 5,
                "max_overflow": 10
            },
            "storage": {
                "path": "data/storage",
                "max_size": "1GB",
                "backup_enabled": True,
                "backup_interval": "1d"
            },
            "security": {
                "jwt_secret": os.urandom(32).hex(),
                "jwt_algorithm": "HS256",
                "jwt_expires_minutes": 30,
                "password_min_length": 8
            }
        }
        
        with open(self._config_file, "w") as f:
            yaml.dump(default_config, f)
            
    def _get_env_override(self, key: str) -> Optional[str]:
        """获取环境变量覆盖值"""
        env_key = f"APP_{key.upper()}"
        return os.environ.get(env_key)
        
    async def reload_config(self) -> dict:
        """重新加载配置"""
        try:
            with open(self._config_file, "r") as f:
                config_data = yaml.safe_load(f)
                
            # 清除现有配置
            self._config.clear()
            
            # 加载新配置
            self._load_config_recursive(config_data, ConfigScope.SYSTEM)
            
            # 应用环境变量覆盖
            for key in self._config.keys():
                env_value = self._get_env_override(key)
                if env_value is not None:
                    try:
                        # 尝试解析JSON值
                        value = json.loads(env_value)
                    except json.JSONDecodeError:
                        # 如果不是JSON，则使用原始字符串
                        value = env_value
                        
                    self._config[key] = ConfigValue(
                        value=value,
                        scope=ConfigScope.SYSTEM,
                        last_modified=datetime.now(),
                        description="Override from environment variable"
                    )
                    
            return {"status": "success", "message": "Configuration reloaded"}
        except Exception as e:
            self.logger.error(f"Failed to reload config: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to reload config: {str(e)}"
            )
            
    def _load_config_recursive(self, config_data: dict, scope: ConfigScope, prefix: str = "") -> None:
        """递归加载配置"""
        for key, value in config_data.items():
            full_key = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                self._load_config_recursive(value, scope, full_key)
            else:
                self._config[full_key] = ConfigValue(
                    value=value,
                    scope=scope,
                    last_modified=datetime.now()
                )
                
    def get_value(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        config = self._config.get(key)
        return config.value if config else default
        
    # API路由处理器
    async def list_configs(self) -> List[ConfigInfo]:
        """列出所有配置"""
        return [
            ConfigInfo(
                key=key,
                value=config.value,
                scope=config.scope,
                last_modified=config.last_modified,
                description=config.description
            )
            for key, config in self._config.items()
        ]
        
    async def get_config(self, key: str) -> ConfigInfo:
        """获取配置信息"""
        config = self._config.get(key)
        if config is None:
            raise HTTPException(status_code=404, detail=f"Config {key} not found")
            
        return ConfigInfo(
            key=key,
            value=config.value,
            scope=config.scope,
            last_modified=config.last_modified,
            description=config.description
        )
        
    async def set_config(self, key: str, request: ConfigUpdateRequest) -> ConfigInfo:
        """设置配置值"""
        if key not in self._config:
            raise HTTPException(status_code=404, detail=f"Config {key} not found")
            
        config = self._config[key]
        if config.scope != ConfigScope.USER:
            raise HTTPException(
                status_code=403,
                detail="Can only modify user-scope configurations"
            )
            
        config.value = request.value
        config.last_modified = datetime.now()
        if request.description:
            config.description = request.description
            
        # 保存到配置文件
        await self._save_config()
        
        return ConfigInfo(
            key=key,
            value=config.value,
            scope=config.scope,
            last_modified=config.last_modified,
            description=config.description
        )
        
    async def delete_config(self, key: str) -> dict:
        """删除配置"""
        if key not in self._config:
            raise HTTPException(status_code=404, detail=f"Config {key} not found")
            
        config = self._config[key]
        if config.scope != ConfigScope.USER:
            raise HTTPException(
                status_code=403,
                detail="Can only delete user-scope configurations"
            )
            
        del self._config[key]
        
        # 保存到配置文件
        await self._save_config()
        
        return {"status": "success", "message": f"Config {key} deleted"}
        
    async def _save_config(self) -> None:
        """保存配置到文件"""
        config_data = {}
        
        # 构建配置树
        for key, config in self._config.items():
            parts = key.split(".")
            current = config_data
            
            # 构建嵌套字典
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
                
            # 设置最终值
            current[parts[-1]] = config.value
            
        # 保存到文件
        with open(self._config_file, "w") as f:
            yaml.dump(config_data, f) 