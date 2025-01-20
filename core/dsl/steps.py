import asyncio
import os
from pathlib import Path
from typing import Dict, Any, Optional, List

import aiohttp
import docker

from .models import Step, Context
from ..managers.job_manager import JobManager


class ShellStep(Step):
    """Shell命令步骤"""
    def __init__(self, command: str, shell: str = "/bin/bash", cwd: Optional[str] = None, env: Dict[str, str] = None, **kwargs):
        super().__init__(**kwargs)
        self.command = command
        self.shell = shell
        self.cwd = cwd
        self.env = env or {}
    
    async def _execute(self, context: Context) -> None:
        """执行步骤"""
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
        
        # 记录输出
        if stdout:
            self.log(stdout.decode(), level="info")
        if stderr:
            self.log(stderr.decode(), level="error")
        
        if process.returncode != 0:
            raise RuntimeError(f"Command failed with exit code {process.returncode}: {stderr.decode()}")
            
        # 设置输出
        self.set_output("stdout", stdout.decode())
        self.set_output("stderr", stderr.decode())
        self.set_output("exit_code", process.returncode)

class DockerStep(Step):
    """Docker命令步骤"""
    def __init__(self, image: str, command: Optional[str] = None,
                 entrypoint: Optional[str] = None, environment: Dict[str, str] = None,
                 volumes: Dict[str, str] = None, working_dir: Optional[str] = None,
                 network: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.image = image
        self.command = command
        self.entrypoint = entrypoint
        self.environment = environment or {}
        self.volumes = volumes or {}
        self.working_dir = working_dir
        self.network = network
        self._client = docker.from_env()
        
    async def _execute(self, context: Context) -> None:
        """执行步骤"""
        # 准备环境变量
        environment = {**context.env, **self.environment}
        
        # 准备挂载卷
        volumes = {}
        for host_path, container_path in self.volumes.items():
            # 支持环境变量替换
            host_path = os.path.expandvars(os.path.expanduser(host_path))
            volumes[host_path] = {"bind": container_path, "mode": "rw"}
            
        try:
            # 运行容器
            container = self._client.containers.run(
                self.image,
                command=self.command,
                entrypoint=self.entrypoint,
                environment=environment,
                volumes=volumes,
                working_dir=self.working_dir,
                network=self.network,
                detach=True
            )
            
            # 等待容器完成
            result = container.wait()
            
            # 获取日志
            logs = container.logs().decode()
            self.log(logs, level="info")
            
            # 设置输出
            self.set_output("container_id", container.id)
            self.set_output("exit_code", result["StatusCode"])
            self.set_output("logs", logs)
            
            # 检查退出码
            if result["StatusCode"] != 0:
                raise RuntimeError(f"Container exited with code {result['StatusCode']}")
                
        finally:
            # 清理容器
            try:
                container.remove(force=True)
            except Exception as e:
                self.log(f"Failed to remove container: {e}", level="warning")

class PythonStep(Step):
    """Python代码步骤"""
    def __init__(self, code: str, globals: Dict[str, Any] = None, locals: Dict[str, Any] = None, **kwargs):
        super().__init__(**kwargs)
        self.code = code
        self.globals = globals or {}
        self.locals = locals or {}
        
    async def _execute(self, context: Context) -> None:
        """执行步骤"""
        # 准备执行环境
        globals_dict = {
            "context": context,
            "os": os,
            "Path": Path,
            "asyncio": asyncio,
            **self.globals
        }
        locals_dict = {**self.locals}
        
        try:
            # 执行代码
            exec(self.code, globals_dict, locals_dict)
            
            # 收集输出
            for key, value in locals_dict.items():
                if not key.startswith("_"):
                    self.set_output(key, value)
                    
        except Exception as e:
            self.log(f"Python execution failed: {e}", level="error")
            raise RuntimeError(f"Python code execution failed: {e}")

class PythonFunctionStep(Step):
    """Python函数步骤"""
    def __init__(self, func: callable, args: List[Any] = None, kwargs: Dict[str, Any] = None, **step_kwargs):
        super().__init__(**step_kwargs)
        self.func = func
        self.args = args or []
        self.kwargs = kwargs or {}
        
    async def _execute(self, context: Context) -> None:
        """执行步骤"""
        try:
            # 执行函数
            result = self.func(*self.args, **self.kwargs)
            
            # 如果是协程，等待执行完成
            if asyncio.iscoroutine(result):
                result = await result
                
            # 设置输出
            self.set_output("result", result)
            
        except Exception as e:
            self.log(f"Function execution failed: {e}", level="error")
            raise RuntimeError(f"Python function execution failed: {e}")

class HttpStep(Step):
    """HTTP请求步骤"""
    def __init__(self, url: str, method: str = "GET", headers: Dict[str, str] = None,
                 params: Dict[str, str] = None, data: Dict[str, Any] = None,
                 json: Dict[str, Any] = None, verify_ssl: bool = True,
                 timeout: int = 30, **kwargs):
        super().__init__(**kwargs)
        self.url = url
        self.method = method
        self.headers = headers or {}
        self.params = params or {}
        self.data = data
        self.json = json
        self.verify_ssl = verify_ssl
        self.request_timeout = timeout
    
    async def _execute(self, context: Context) -> None:
        """执行步骤"""
        async with aiohttp.ClientSession() as session:
            # 准备请求参数
            kwargs = {
                "headers": self.headers,
                "params": self.params,
                "ssl": self.verify_ssl,
                "timeout": self.request_timeout
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
                content = await response.text()
                self.log(f"Response status: {response.status}", level="info")
                self.log(f"Response body: {content}", level="debug")
                
                # 设置输出
                self.set_output("status", response.status)
                self.set_output("headers", dict(response.headers))
                self.set_output("body", content)
                
                if response.status >= 400:
                    raise RuntimeError(f"HTTP request failed with status {response.status}: {content}")

class CallJobStep(Step):
    """调用其他作业的步骤"""
    def __init__(self, job_manager: JobManager, job_id: str, params: Dict[str, Any] = None, **kwargs):
        super().__init__(**kwargs)
        self.job_manager = job_manager
        self.job_id = job_id
        self.step_params = params or {}
        
    async def _execute(self, context: Context) -> None:
        """执行步骤"""
        # 合并参数
        params = {**self.step_params, **context.params}
        
        # 调用作业
        build = await self.job_manager.call_job(
            job_id=self.job_id,
            params=params
        )
        
        # 设置输出
        self.set_output("build_id", build.id)
        self.set_output("build_number", build.number)
        self.set_output("build_status", build.status.value)

class ParallelStep(Step):
    """并行执行多个步骤"""
    def __init__(self, steps: List[Step], **kwargs):
        super().__init__(**kwargs)
        self.steps = steps
        
    async def _execute(self, context: Context) -> None:
        """执行步骤"""
        # 创建所有步骤的任务
        tasks = [step.run(context) for step in self.steps]
        
        # 并行执行所有步骤
        await asyncio.gather(*tasks)
        
        # 检查所有步骤的状态
        failed_steps = [step for step in self.steps if not step.status.is_successful]
        if failed_steps:
            step_names = ", ".join(s.name for s in failed_steps)
            raise RuntimeError(f"Steps failed: {step_names}")

class ConditionStep(Step):
    """条件步骤"""
    def __init__(self, condition: str, if_step: Step, else_step: Optional[Step] = None, **kwargs):
        super().__init__(**kwargs)
        self.condition = condition
        self.if_step = if_step
        self.else_step = else_step
        
    async def _execute(self, context: Context) -> None:
        """执行步骤"""
        # 评估条件
        try:
            result = eval(self.condition, {"context": context})
        except Exception as e:
            raise RuntimeError(f"Failed to evaluate condition: {e}")
            
        if result:
            await self.if_step.run(context)
            self.set_output("branch", "if")
        elif self.else_step:
            await self.else_step.run(context)
            self.set_output("branch", "else")
        else:
            self.set_output("branch", "skipped")

class RetryStep(Step):
    """重试步骤"""
    def __init__(self, step: Step, retry: int = 3, retry_delay: int = 0, **kwargs):
        super().__init__(**kwargs)
        self.step = step
        self.step.retry = retry
        self.step.retry_delay = retry_delay
        
    async def _execute(self, context: Context) -> None:
        """执行步骤"""
        await self.step.run(context)
        # 复制步骤的输出
        self.outputs = self.step.outputs.copy()

class GroupStep(Step):
    """步骤组"""
    def __init__(self, steps: List[Step], **kwargs):
        super().__init__(**kwargs)
        self.steps = steps
        
    async def _execute(self, context: Context) -> None:
        """执行步骤"""
        # 串行执行所有步骤
        for step in self.steps:
            await step.run(context)
            
        # 收集所有步骤的输出
        for step in self.steps:
            self.outputs.update(step.outputs)

class SetVariableStep(Step):
    """设置变量步骤"""
    def __init__(self, variables: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.variables = variables
        
    async def _execute(self, context: Context) -> None:
        """执行步骤"""
        for name, value in self.variables.items():
            context.set_variable(name, value)
            self.set_output(name, value)

class SetOutputStep(Step):
    """设置输出步骤"""
    def __init__(self, outputs: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.step_outputs = outputs
        
    async def _execute(self, context: Context) -> None:
        """执行步骤"""
        for name, value in self.step_outputs.items():
            self.set_output(name, value) 