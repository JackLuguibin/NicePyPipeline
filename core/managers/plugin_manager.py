from typing import Dict, List, Any, Optional, Type
from core.base_manager import BaseManager
import asyncio
from datetime import datetime
import uuid
import logging
from enum import Enum
from fastapi import FastAPI, HTTPException, UploadFile, File
from pydantic import BaseModel
import importlib.util
import sys
from pathlib import Path
import yaml
import pkg_resources

class PluginStatus(Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    ERROR = "error"

class PluginInfo(BaseModel):
    id: str
    name: str
    version: str
    description: str
    author: str
    status: PluginStatus
    config: dict
    created_at: datetime
    error: Optional[str] = None

class PluginCreateRequest(BaseModel):
    name: str
    config: dict = {}

class Plugin:
    def __init__(self, name: str, config: dict = None):
        self.id = str(uuid.uuid4())
        self.name = name
        self.status = PluginStatus.DISABLED
        self.config = config or {}
        self.created_at = datetime.now()
        self.error: Optional[str] = None
        self._instance: Any = None
        self._module: Any = None
        
        # 从插件元数据中获取信息
        metadata = self._load_metadata()
        self.version = metadata.get("version", "0.0.1")
        self.description = metadata.get("description", "")
        self.author = metadata.get("author", "")
        
    def _load_metadata(self) -> dict:
        """加载插件元数据"""
        try:
            metadata_file = Path(f"plugins/{self.name}/plugin.yaml")
            if metadata_file.exists():
                with open(metadata_file, "r") as f:
                    return yaml.safe_load(f)
        except Exception:
            pass
        return {}
        
    def to_info(self) -> PluginInfo:
        return PluginInfo(
            id=self.id,
            name=self.name,
            version=self.version,
            description=self.description,
            author=self.author,
            status=self.status,
            config=self.config,
            created_at=self.created_at,
            error=self.error
        )

class PluginManager(BaseManager):
    def __init__(self, app: FastAPI):
        super().__init__(app, "plugin")
        self._plugins: Dict[str, Plugin] = {}
        self.logger = logging.getLogger(__name__)
        
    def setup_routes(self) -> None:
        """设置路由"""
        self._router.get("/list")(self.list_plugins)
        self._router.post("/install")(self.install_plugin)
        self._router.get("/{plugin_id}")(self.get_plugin)
        self._router.post("/{plugin_id}/enable")(self.enable_plugin)
        self._router.post("/{plugin_id}/disable")(self.disable_plugin)
        self._router.delete("/{plugin_id}")(self.uninstall_plugin)
        self._router.post("/{plugin_id}/config")(self.update_plugin_config)
        
    async def _initialize(self) -> None:
        """初始化插件管理器"""
        self.logger.info("Initializing plugin manager")
        # 创建插件目录
        plugin_dir = Path("plugins")
        plugin_dir.mkdir(exist_ok=True)
        
        # 加载已安装的插件
        await self._load_installed_plugins()
        
    async def _start(self) -> None:
        """启动插件管理器"""
        self.logger.info("Starting plugin manager")
        # 启动所有已启用的插件
        for plugin in self._plugins.values():
            if plugin.status == PluginStatus.ENABLED:
                await self._start_plugin(plugin)
        
    async def _stop(self) -> None:
        """停止插件管理器"""
        self.logger.info("Stopping plugin manager")
        # 停止所有插件
        for plugin in self._plugins.values():
            await self._stop_plugin(plugin)
        
    async def _cleanup(self) -> None:
        """清理插件管理器"""
        self.logger.info("Cleaning up plugin manager")
        await self._stop()
        self._plugins.clear()
        
    async def _load_installed_plugins(self) -> None:
        """加载已安装的插件"""
        plugin_dir = Path("plugins")
        for plugin_path in plugin_dir.iterdir():
            if plugin_path.is_dir() and (plugin_path / "__init__.py").exists():
                try:
                    plugin = Plugin(plugin_path.name)
                    self._plugins[plugin.id] = plugin
                    if (plugin_path / "config.yaml").exists():
                        with open(plugin_path / "config.yaml", "r") as f:
                            plugin.config = yaml.safe_load(f)
                except Exception as e:
                    self.logger.error(f"Error loading plugin {plugin_path.name}: {str(e)}")
                    
    async def _load_plugin_module(self, plugin: Plugin) -> None:
        """加载插件模块"""
        try:
            plugin_dir = Path(f"plugins/{plugin.name}")
            spec = importlib.util.spec_from_file_location(
                plugin.name,
                plugin_dir / "__init__.py"
            )
            if spec is None or spec.loader is None:
                raise ImportError("Failed to load plugin module")
                
            module = importlib.util.module_from_spec(spec)
            sys.modules[plugin.name] = module
            spec.loader.exec_module(module)
            plugin._module = module
            
            # 实例化插件类
            if hasattr(module, "Plugin"):
                plugin._instance = module.Plugin(plugin.config)
            else:
                raise ImportError("Plugin class not found")
                
        except Exception as e:
            plugin.error = str(e)
            plugin.status = PluginStatus.ERROR
            raise
            
    async def _start_plugin(self, plugin: Plugin) -> None:
        """启动插件"""
        try:
            if plugin._instance is None:
                await self._load_plugin_module(plugin)
                
            if hasattr(plugin._instance, "start"):
                await plugin._instance.start()
                
            plugin.status = PluginStatus.ENABLED
        except Exception as e:
            plugin.error = str(e)
            plugin.status = PluginStatus.ERROR
            raise
            
    async def _stop_plugin(self, plugin: Plugin) -> None:
        """停止插件"""
        if plugin._instance and hasattr(plugin._instance, "stop"):
            try:
                await plugin._instance.stop()
            except Exception as e:
                self.logger.error(f"Error stopping plugin {plugin.name}: {str(e)}")
                
        plugin.status = PluginStatus.DISABLED
        plugin._instance = None
        
        # 从sys.modules中移除插件模块
        if plugin.name in sys.modules:
            del sys.modules[plugin.name]
            
    async def _install_plugin_package(self, plugin: Plugin) -> None:
        """安装插件包依赖"""
        requirements_file = Path(f"plugins/{plugin.name}/requirements.txt")
        if requirements_file.exists():
            try:
                # 使用pip安装依赖
                import pip
                pip.main(["install", "-r", str(requirements_file)])
            except Exception as e:
                raise RuntimeError(f"Failed to install plugin dependencies: {str(e)}")
                
    # API路由处理器
    async def list_plugins(self) -> List[PluginInfo]:
        """列出所有插件"""
        return [plugin.to_info() for plugin in self._plugins.values()]
        
    async def install_plugin(self, file: UploadFile = File(...)) -> PluginInfo:
        """安装插件"""
        if not file.filename.endswith('.zip'):
            raise HTTPException(status_code=400, detail="Only ZIP files are allowed")
            
        try:
            import zipfile
            import io
            
            # 读取上传的ZIP文件
            content = await file.read()
            zip_file = zipfile.ZipFile(io.BytesIO(content))
            
            # 获取插件名称
            plugin_name = Path(zip_file.namelist()[0]).parts[0]
            plugin_dir = Path("plugins") / plugin_name
            
            # 如果插件已存在，先卸载
            if plugin_dir.exists():
                raise HTTPException(
                    status_code=400,
                    detail=f"Plugin {plugin_name} already exists"
                )
                
            # 解压插件文件
            zip_file.extractall("plugins")
            
            # 创建插件实例
            plugin = Plugin(plugin_name)
            
            # 安装依赖
            await self._install_plugin_package(plugin)
            
            self._plugins[plugin.id] = plugin
            return plugin.to_info()
            
        except Exception as e:
            if plugin_dir.exists():
                import shutil
                shutil.rmtree(plugin_dir)
            raise HTTPException(
                status_code=400,
                detail=f"Failed to install plugin: {str(e)}"
            )
            
    async def get_plugin(self, plugin_id: str) -> PluginInfo:
        """获取插件信息"""
        plugin = self._plugins.get(plugin_id)
        if plugin is None:
            raise HTTPException(status_code=404, detail=f"Plugin {plugin_id} not found")
        return plugin.to_info()
        
    async def enable_plugin(self, plugin_id: str) -> PluginInfo:
        """启用插件"""
        plugin = self._plugins.get(plugin_id)
        if plugin is None:
            raise HTTPException(status_code=404, detail=f"Plugin {plugin_id} not found")
            
        if plugin.status != PluginStatus.ENABLED:
            try:
                await self._start_plugin(plugin)
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
                
        return plugin.to_info()
        
    async def disable_plugin(self, plugin_id: str) -> PluginInfo:
        """禁用插件"""
        plugin = self._plugins.get(plugin_id)
        if plugin is None:
            raise HTTPException(status_code=404, detail=f"Plugin {plugin_id} not found")
            
        if plugin.status == PluginStatus.ENABLED:
            await self._stop_plugin(plugin)
            
        return plugin.to_info()
        
    async def uninstall_plugin(self, plugin_id: str) -> dict:
        """卸载插件"""
        plugin = self._plugins.get(plugin_id)
        if plugin is None:
            raise HTTPException(status_code=404, detail=f"Plugin {plugin_id} not found")
            
        # 停止插件
        if plugin.status == PluginStatus.ENABLED:
            await self._stop_plugin(plugin)
            
        # 删除插件文件
        plugin_dir = Path(f"plugins/{plugin.name}")
        if plugin_dir.exists():
            import shutil
            shutil.rmtree(plugin_dir)
            
        del self._plugins[plugin_id]
        return {"status": "success", "message": f"Plugin {plugin.name} uninstalled"}
        
    async def update_plugin_config(self, plugin_id: str, config: dict) -> PluginInfo:
        """更新插件配置"""
        plugin = self._plugins.get(plugin_id)
        if plugin is None:
            raise HTTPException(status_code=404, detail=f"Plugin {plugin_id} not found")
            
        # 更新配置
        plugin.config.update(config)
        
        # 保存配置到文件
        config_file = Path(f"plugins/{plugin.name}/config.yaml")
        with open(config_file, "w") as f:
            yaml.dump(plugin.config, f)
            
        # 如果插件正在运行，重新加载配置
        if (plugin.status == PluginStatus.ENABLED and
            plugin._instance and
            hasattr(plugin._instance, "reload_config")):
            await plugin._instance.reload_config(plugin.config)
            
        return plugin.to_info() 