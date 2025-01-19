from typing import Dict, List, Any, Optional
from core.base_manager import BaseManager
import asyncio
from datetime import datetime
import uuid
import logging
from enum import Enum
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from pathlib import Path
import aiofiles
import shutil
import os
import mimetypes
import hashlib
import aiofiles.os

class FileType(Enum):
    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"

class FileInfo(BaseModel):
    name: str
    path: str
    type: FileType
    size: int
    created_at: datetime
    modified_at: datetime
    mime_type: Optional[str] = None
    hash: Optional[str] = None

class DirectoryInfo(BaseModel):
    path: str
    items: List[FileInfo]
    total_size: int
    total_files: int
    total_directories: int

class FileManager(BaseManager):
    def __init__(self, app: FastAPI):
        super().__init__(app, "file")
        self._base_path = Path("workspace")
        self._temp_path = Path("temp")
        self.logger = logging.getLogger(__name__)
        
    def setup_routes(self) -> None:
        """设置路由"""
        self._router.get("/list/{path:path}")(self.list_directory)
        self._router.post("/upload/{path:path}")(self.upload_file)
        self._router.get("/download/{path:path}")(self.download_file)
        self._router.delete("/{path:path}")(self.delete_file)
        self._router.post("/mkdir/{path:path}")(self.create_directory)
        self._router.post("/move")(self.move_file)
        self._router.post("/copy")(self.copy_file)
        
    async def _initialize(self) -> None:
        """初始化文件管理器"""
        self.logger.info("Initializing file manager")
        # 创建基础目录
        self._base_path.mkdir(exist_ok=True)
        self._temp_path.mkdir(exist_ok=True)
        
    async def _start(self) -> None:
        """启动文件管理器"""
        self.logger.info("Starting file manager")
        
    async def _stop(self) -> None:
        """停止文件管理器"""
        self.logger.info("Stopping file manager")
        
    async def _cleanup(self) -> None:
        """清理文件管理器"""
        self.logger.info("Cleaning up file manager")
        # 清理临时目录
        if self._temp_path.exists():
            shutil.rmtree(self._temp_path)
            self._temp_path.mkdir()
            
    def _normalize_path(self, path: str) -> Path:
        """规范化路径"""
        normalized = Path(path).resolve()
        if not str(normalized).startswith(str(self._base_path.resolve())):
            raise HTTPException(
                status_code=403,
                detail="Access to this path is forbidden"
            )
        return normalized
        
    async def _get_file_info(self, path: Path) -> FileInfo:
        """获取文件信息"""
        stat = await aiofiles.os.stat(path)
        file_type = FileType.DIRECTORY if path.is_dir() else FileType.FILE
        if path.is_symlink():
            file_type = FileType.SYMLINK
            
        mime_type = None
        file_hash = None
        
        if file_type == FileType.FILE:
            mime_type, _ = mimetypes.guess_type(path)
            # 只对小文件计算哈希值
            if stat.st_size < 10 * 1024 * 1024:  # 10MB
                async with aiofiles.open(path, "rb") as f:
                    content = await f.read()
                    file_hash = hashlib.sha256(content).hexdigest()
                    
        return FileInfo(
            name=path.name,
            path=str(path.relative_to(self._base_path)),
            type=file_type,
            size=stat.st_size,
            created_at=datetime.fromtimestamp(stat.st_ctime),
            modified_at=datetime.fromtimestamp(stat.st_mtime),
            mime_type=mime_type,
            hash=file_hash
        )
        
    # API路由处理器
    async def list_directory(self, path: str) -> DirectoryInfo:
        """列出目录内容"""
        dir_path = self._normalize_path(path)
        if not dir_path.exists():
            raise HTTPException(status_code=404, detail="Directory not found")
        if not dir_path.is_dir():
            raise HTTPException(status_code=400, detail="Path is not a directory")
            
        items = []
        total_size = 0
        total_files = 0
        total_directories = 0
        
        async for entry in aiofiles.os.scandir(dir_path):
            info = await self._get_file_info(Path(entry.path))
            items.append(info)
            
            if info.type == FileType.FILE:
                total_size += info.size
                total_files += 1
            elif info.type == FileType.DIRECTORY:
                total_directories += 1
                
        return DirectoryInfo(
            path=str(dir_path.relative_to(self._base_path)),
            items=items,
            total_size=total_size,
            total_files=total_files,
            total_directories=total_directories
        )
        
    async def upload_file(self, path: str, file: UploadFile = File(...)) -> FileInfo:
        """上传文件"""
        file_path = self._normalize_path(path) / file.filename
        if file_path.exists():
            raise HTTPException(
                status_code=400,
                detail="File already exists"
            )
            
        # 确保父目录存在
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            async with aiofiles.open(file_path, "wb") as f:
                while content := await file.read(8192):
                    await f.write(content)
                    
            return await self._get_file_info(file_path)
        except Exception as e:
            if file_path.exists():
                file_path.unlink()
            raise HTTPException(
                status_code=500,
                detail=f"Failed to upload file: {str(e)}"
            )
            
    async def download_file(self, path: str) -> StreamingResponse:
        """下载文件"""
        file_path = self._normalize_path(path)
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="File not found")
        if not file_path.is_file():
            raise HTTPException(status_code=400, detail="Path is not a file")
            
        return FileResponse(
            file_path,
            filename=file_path.name,
            media_type=mimetypes.guess_type(file_path)[0]
        )
        
    async def delete_file(self, path: str) -> dict:
        """删除文件或目录"""
        file_path = self._normalize_path(path)
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="Path not found")
            
        try:
            if file_path.is_dir():
                shutil.rmtree(file_path)
            else:
                file_path.unlink()
            return {"status": "success", "message": f"Deleted {path}"}
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete: {str(e)}"
            )
            
    async def create_directory(self, path: str) -> FileInfo:
        """创建目录"""
        dir_path = self._normalize_path(path)
        if dir_path.exists():
            raise HTTPException(
                status_code=400,
                detail="Directory already exists"
            )
            
        try:
            dir_path.mkdir(parents=True)
            return await self._get_file_info(dir_path)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create directory: {str(e)}"
            )
            
    async def move_file(self, source: str, destination: str) -> FileInfo:
        """移动文件或目录"""
        src_path = self._normalize_path(source)
        dst_path = self._normalize_path(destination)
        
        if not src_path.exists():
            raise HTTPException(status_code=404, detail="Source path not found")
        if dst_path.exists():
            raise HTTPException(
                status_code=400,
                detail="Destination already exists"
            )
            
        try:
            # 确保目标父目录存在
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(src_path, dst_path)
            return await self._get_file_info(dst_path)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to move: {str(e)}"
            )
            
    async def copy_file(self, source: str, destination: str) -> FileInfo:
        """复制文件或目录"""
        src_path = self._normalize_path(source)
        dst_path = self._normalize_path(destination)
        
        if not src_path.exists():
            raise HTTPException(status_code=404, detail="Source path not found")
        if dst_path.exists():
            raise HTTPException(
                status_code=400,
                detail="Destination already exists"
            )
            
        try:
            # 确保目标父目录存在
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            
            if src_path.is_dir():
                shutil.copytree(src_path, dst_path)
            else:
                shutil.copy2(src_path, dst_path)
                
            return await self._get_file_info(dst_path)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to copy: {str(e)}"
            ) 