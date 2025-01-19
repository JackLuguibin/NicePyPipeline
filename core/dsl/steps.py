from typing import Dict, Any, Optional, List
from .models import Step, Context
import asyncio
import os
import shutil
from pathlib import Path
import subprocess
import docker
import aiohttp

class ShellStep(Step):
    """Shell命令步骤"""
    command: str
    shell: str = "/bin/bash"
    cwd: Optional[str] = None
    env: Dict[str, str] = {}
    
    async def run(self, context: Context) -> None:
        self.status = StepStatus.RUNNING
        self.started_at = datetime.now()
        
        try:
            # 合并环境变量
            env = os.environ.copy()
            env.update(context.env)
            env.update(self.env)
            
            # 设置工作目录
            cwd = self.cwd or context.workspace
            
            # 执行命令
            process = await asyncio.create_subprocess_shell(
                self.command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                cwd=cwd,
                shell=True
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                self.status = StepStatus.SUCCESS
            else:
                self.status = StepStatus.FAILED
                self.error = stderr.decode()
                
        except Exception as e:
            self.status = StepStatus.FAILED
            self.error = str(e)
            raise
        finally:
            self.finished_at = datetime.now()

class DockerStep(Step):
    """Docker容器步骤"""
    image: str
    command: Optional[str] = None
    env: Dict[str, str] = {}
    volumes: Dict[str, str] = {}
    working_dir: Optional[str] = None
    
    async def run(self, context: Context) -> None:
        self.status = StepStatus.RUNNING
        self.started_at = datetime.now()
        
        try:
            # 创建Docker客户端
            client = docker.from_env()
            
            # 准备环境变量
            env = {}
            env.update(context.env)
            env.update(self.env)
            
            # 准备卷映射
            volumes = {}
            if context.workspace:
                volumes[context.workspace] = {
                    "bind": "/workspace",
                    "mode": "rw"
                }
            volumes.update(self.volumes)
            
            # 运行容器
            container = client.containers.run(
                self.image,
                self.command,
                environment=env,
                volumes=volumes,
                working_dir=self.working_dir or "/workspace",
                detach=True
            )
            
            # 等待容器完成
            result = container.wait()
            
            if result["StatusCode"] == 0:
                self.status = StepStatus.SUCCESS
            else:
                self.status = StepStatus.FAILED
                self.error = container.logs().decode()
                
            # 清理容器
            container.remove()
            
        except Exception as e:
            self.status = StepStatus.FAILED
            self.error = str(e)
            raise
        finally:
            self.finished_at = datetime.now()

class PythonStep(Step):
    """Python代码步骤"""
    code: str
    env: Dict[str, str] = {}
    requirements: List[str] = []
    
    async def run(self, context: Context) -> None:
        self.status = StepStatus.RUNNING
        self.started_at = datetime.now()
        
        try:
            # 创建临时目录
            temp_dir = Path(context.workspace) / ".temp" / str(uuid.uuid4())
            temp_dir.mkdir(parents=True)
            
            try:
                # 写入Python代码
                script_file = temp_dir / "script.py"
                with open(script_file, "w") as f:
                    f.write(self.code)
                    
                # 安装依赖
                if self.requirements:
                    requirements_file = temp_dir / "requirements.txt"
                    with open(requirements_file, "w") as f:
                        f.write("\n".join(self.requirements))
                        
                    process = await asyncio.create_subprocess_exec(
                        sys.executable,
                        "-m",
                        "pip",
                        "install",
                        "-r",
                        str(requirements_file),
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    await process.communicate()
                    
                # 准备环境变量
                env = os.environ.copy()
                env.update(context.env)
                env.update(self.env)
                
                # 执行Python脚本
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    str(script_file),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                    cwd=context.workspace
                )
                
                stdout, stderr = await process.communicate()
                
                if process.returncode == 0:
                    self.status = StepStatus.SUCCESS
                else:
                    self.status = StepStatus.FAILED
                    self.error = stderr.decode()
                    
            finally:
                # 清理临时目录
                shutil.rmtree(temp_dir)
                
        except Exception as e:
            self.status = StepStatus.FAILED
            self.error = str(e)
            raise
        finally:
            self.finished_at = datetime.now()

class HttpStep(Step):
    """HTTP请求步骤"""
    url: str
    method: str = "GET"
    headers: Dict[str, str] = {}
    params: Dict[str, str] = {}
    data: Optional[Dict[str, Any]] = None
    json: Optional[Dict[str, Any]] = None
    verify_ssl: bool = True
    timeout: int = 30
    
    async def run(self, context: Context) -> None:
        self.status = StepStatus.RUNNING
        self.started_at = datetime.now()
        
        try:
            async with aiohttp.ClientSession() as session:
                # 准备请求参数
                kwargs = {
                    "headers": self.headers,
                    "params": self.params,
                    "ssl": self.verify_ssl,
                    "timeout": self.timeout
                }
                
                if self.data:
                    kwargs["data"] = self.data
                if self.json:
                    kwargs["json"] = self.json
                    
                # 发送请求
                async with session.request(
                    self.method,
                    self.url,
                    **kwargs
                ) as response:
                    if response.status < 400:
                        self.status = StepStatus.SUCCESS
                    else:
                        self.status = StepStatus.FAILED
                        self.error = await response.text()
                        
        except Exception as e:
            self.status = StepStatus.FAILED
            self.error = str(e)
            raise
        finally:
            self.finished_at = datetime.now()

class ParallelStep(Step):
    """并行步骤"""
    steps: List[Step] = []
    
    async def run(self, context: Context) -> None:
        self.status = StepStatus.RUNNING
        self.started_at = datetime.now()
        
        try:
            # 创建任务
            tasks = [
                asyncio.create_task(step.run(context))
                for step in self.steps
            ]
            
            # 等待所有任务完成
            await asyncio.gather(*tasks)
            
            # 检查所有步骤的状态
            if all(step.status == StepStatus.SUCCESS for step in self.steps):
                self.status = StepStatus.SUCCESS
            else:
                self.status = StepStatus.FAILED
                self.error = "One or more steps failed"
                
        except Exception as e:
            self.status = StepStatus.FAILED
            self.error = str(e)
            raise
        finally:
            self.finished_at = datetime.now()
            
    async def cancel(self) -> None:
        """取消步骤"""
        self.status = StepStatus.CANCELLED
        for step in self.steps:
            await step.cancel() 