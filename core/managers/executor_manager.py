from typing import Dict, List, Any, Optional
from core.base_manager import BaseManager
import asyncio
from datetime import datetime
import uuid
import logging
from enum import Enum
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import docker
import aiohttp
import os
import tempfile
import shutil

class ExecutorType(Enum):
    DOCKER = "docker"
    SHELL = "shell"
    PYTHON = "python"
    REMOTE = "remote"

class ExecutorStatus(Enum):
    IDLE = "idle"
    BUSY = "busy"
    ERROR = "error"
    OFFLINE = "offline"

class ExecutorInfo(BaseModel):
    id: str
    name: str
    type: ExecutorType
    status: ExecutorStatus
    config: dict
    created_at: datetime
    last_used: Optional[datetime] = None
    current_task: Optional[str] = None

class ExecutorCreateRequest(BaseModel):
    name: str
    type: ExecutorType
    config: dict

class ExecutionResult(BaseModel):
    success: bool
    output: str
    error: Optional[str] = None
    start_time: datetime
    end_time: datetime
    exit_code: int

class Executor:
    def __init__(self, name: str, executor_type: ExecutorType, config: dict):
        self.id = str(uuid.uuid4())
        self.name = name
        self.type = executor_type
        self.status = ExecutorStatus.IDLE
        self.config = config
        self.created_at = datetime.now()
        self.last_used: Optional[datetime] = None
        self.current_task: Optional[str] = None
        self._docker_client: Optional[docker.DockerClient] = None
        self._work_dir: Optional[str] = None
        
    def to_info(self) -> ExecutorInfo:
        return ExecutorInfo(
            id=self.id,
            name=self.name,
            type=self.type,
            status=self.status,
            config=self.config,
            created_at=self.created_at,
            last_used=self.last_used,
            current_task=self.current_task
        )

class ExecutorManager(BaseManager):
    def __init__(self, app: FastAPI):
        super().__init__(app, "executor")
        self._executors: Dict[str, Executor] = {}
        self.logger = logging.getLogger(__name__)
        
    def setup_routes(self) -> None:
        """设置路由"""
        self._router.get("/list")(self.list_executors)
        self._router.post("/create")(self.create_executor)
        self._router.get("/{executor_id}")(self.get_executor)
        self._router.delete("/{executor_id}")(self.delete_executor)
        self._router.post("/{executor_id}/execute")(self.execute_task)
        
    async def _initialize(self) -> None:
        """初始化执行器管理器"""
        self.logger.info("Initializing executor manager")
        # 创建默认执行器
        await self.create_executor(ExecutorCreateRequest(
            name="default_shell",
            type=ExecutorType.SHELL,
            config={}
        ))
        
    async def _start(self) -> None:
        """启动执行器管理器"""
        self.logger.info("Starting executor manager")
        
    async def _stop(self) -> None:
        """停止执行器管理器"""
        self.logger.info("Stopping executor manager")
        # 清理所有执行器
        for executor in self._executors.values():
            await self._cleanup_executor(executor)
        
    async def _cleanup(self) -> None:
        """清理执行器管理器"""
        self.logger.info("Cleaning up executor manager")
        await self._stop()
        self._executors.clear()
        
    async def _cleanup_executor(self, executor: Executor) -> None:
        """清理执行器资源"""
        if executor.type == ExecutorType.DOCKER and executor._docker_client:
            executor._docker_client.close()
            executor._docker_client = None
            
        if executor._work_dir and os.path.exists(executor._work_dir):
            shutil.rmtree(executor._work_dir)
            executor._work_dir = None
            
    async def _prepare_executor(self, executor: Executor) -> None:
        """准备执行器"""
        if executor.type == ExecutorType.DOCKER:
            if not executor._docker_client:
                executor._docker_client = docker.from_env()
                
        if not executor._work_dir:
            executor._work_dir = tempfile.mkdtemp(prefix=f"executor_{executor.id}_")
            
    async def execute_shell_command(self, command: str, work_dir: str = None) -> ExecutionResult:
        """执行Shell命令"""
        start_time = datetime.now()
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=work_dir
        )
        
        stdout, stderr = await process.communicate()
        end_time = datetime.now()
        
        return ExecutionResult(
            success=process.returncode == 0,
            output=stdout.decode(),
            error=stderr.decode() if stderr else None,
            start_time=start_time,
            end_time=end_time,
            exit_code=process.returncode
        )
        
    async def execute_docker_command(self, executor: Executor, command: str) -> ExecutionResult:
        """在Docker容器中执行命令"""
        start_time = datetime.now()
        try:
            container = executor._docker_client.containers.run(
                executor.config["image"],
                command,
                detach=True,
                remove=True,
                volumes={executor._work_dir: {"bind": "/workspace", "mode": "rw"}}
            )
            
            output = container.logs(stdout=True, stderr=True)
            exit_code = container.wait()["StatusCode"]
            
            return ExecutionResult(
                success=exit_code == 0,
                output=output.decode(),
                error=None,
                start_time=start_time,
                end_time=datetime.now(),
                exit_code=exit_code
            )
        except Exception as e:
            return ExecutionResult(
                success=False,
                output="",
                error=str(e),
                start_time=start_time,
                end_time=datetime.now(),
                exit_code=-1
            )
            
    async def execute_remote_command(self, executor: Executor, command: str) -> ExecutionResult:
        """在远程主机上执行命令"""
        start_time = datetime.now()
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"{executor.config['url']}/execute",
                    json={"command": command}
                ) as response:
                    result = await response.json()
                    return ExecutionResult(
                        success=result["success"],
                        output=result["output"],
                        error=result.get("error"),
                        start_time=start_time,
                        end_time=datetime.now(),
                        exit_code=result.get("exit_code", -1)
                    )
            except Exception as e:
                return ExecutionResult(
                    success=False,
                    output="",
                    error=str(e),
                    start_time=start_time,
                    end_time=datetime.now(),
                    exit_code=-1
                )
                
    # API路由处理器
    async def list_executors(self) -> List[ExecutorInfo]:
        """列出所有执行器"""
        return [executor.to_info() for executor in self._executors.values()]
        
    async def create_executor(self, request: ExecutorCreateRequest) -> ExecutorInfo:
        """创建执行器"""
        executor = Executor(
            name=request.name,
            executor_type=request.type,
            config=request.config
        )
        
        try:
            await self._prepare_executor(executor)
        except Exception as e:
            await self._cleanup_executor(executor)
            raise HTTPException(
                status_code=400,
                detail=f"Failed to prepare executor: {str(e)}"
            )
            
        self._executors[executor.id] = executor
        return executor.to_info()
        
    async def get_executor(self, executor_id: str) -> ExecutorInfo:
        """获取执行器信息"""
        executor = self._executors.get(executor_id)
        if executor is None:
            raise HTTPException(status_code=404, detail=f"Executor {executor_id} not found")
        return executor.to_info()
        
    async def delete_executor(self, executor_id: str) -> dict:
        """删除执行器"""
        executor = self._executors.get(executor_id)
        if executor is None:
            raise HTTPException(status_code=404, detail=f"Executor {executor_id} not found")
            
        if executor.status == ExecutorStatus.BUSY:
            raise HTTPException(status_code=400, detail="Cannot delete busy executor")
            
        await self._cleanup_executor(executor)
        del self._executors[executor_id]
        return {"status": "success", "message": f"Executor {executor_id} deleted"}
        
    async def execute_task(self, executor_id: str, command: str) -> ExecutionResult:
        """执行任务"""
        executor = self._executors.get(executor_id)
        if executor is None:
            raise HTTPException(status_code=404, detail=f"Executor {executor_id} not found")
            
        if executor.status == ExecutorStatus.BUSY:
            raise HTTPException(status_code=400, detail="Executor is busy")
            
        try:
            executor.status = ExecutorStatus.BUSY
            executor.last_used = datetime.now()
            
            if executor.type == ExecutorType.SHELL:
                result = await self.execute_shell_command(command, executor._work_dir)
            elif executor.type == ExecutorType.DOCKER:
                result = await self.execute_docker_command(executor, command)
            elif executor.type == ExecutorType.REMOTE:
                result = await self.execute_remote_command(executor, command)
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported executor type: {executor.type}")
                
            return result
        except Exception as e:
            executor.status = ExecutorStatus.ERROR
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            if executor.status != ExecutorStatus.ERROR:
                executor.status = ExecutorStatus.IDLE 