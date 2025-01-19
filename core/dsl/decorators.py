from typing import Dict, Any, Optional, Callable, Type, Union, List
from functools import wraps
from contextlib import contextmanager
import inspect
from .models import Pipeline, Stage, Step, Context, Status
import asyncio

class FunctionStep(Step):
    """函数步骤"""
    def __init__(self, func: Callable, **kwargs):
        super().__init__(**kwargs)
        self._func = func
        self.__doc__ = func.__doc__
        
    async def _execute(self, context: Context) -> None:
        """执行函数"""
        if asyncio.iscoroutinefunction(self._func):
            await self._func(context)
        else:
            await asyncio.to_thread(self._func, context)

def step(
    name: Optional[str] = None,
    timeout: Optional[int] = None,
    retry: int = 0,
    retry_delay: int = 0,
    depends_on: Optional[Union[str, List[str]]] = None
) -> Callable:
    """步骤装饰器
    
    Args:
        name: 步骤名称，默认使用函数名
        timeout: 超时时间（秒）
        retry: 重试次数
        retry_delay: 重试延迟（秒）
        depends_on: 依赖的步骤名称
    """
    def decorator(func: Callable) -> Step:
        # 获取当前帧
        frame = inspect.currentframe()
        
        # 查找stage上下文
        while frame:
            if hasattr(frame.f_code, "co_name") and frame.f_code.co_name == "stage":
                break
            frame = frame.f_back
            
        if not frame:
            raise RuntimeError("step must be used within a stage")
            
        # 获取stage实例
        stage_instance = frame.f_locals.get("stage_instance")
        if not stage_instance:
            raise RuntimeError("stage instance not found")
            
        # 创建步骤
        step_instance = FunctionStep(
            func=func,
            name=name or func.__name__,
            timeout=timeout,
            retry=retry,
            retry_delay=retry_delay
        )
        
        # 添加到stage
        stage_instance.steps.append(step_instance)
        
        # 处理依赖关系
        if depends_on:
            deps = [depends_on] if isinstance(depends_on, str) else depends_on
            for dep in deps:
                # 查找依赖的步骤
                dep_step = None
                for s in stage_instance.steps:
                    if s.name == dep:
                        dep_step = s
                        break
                if not dep_step:
                    raise ValueError(f"Dependent step {dep} not found")
                    
                # 添加依赖检查
                original_execute = step_instance._execute
                
                async def wrapped_execute(self, context: Context) -> None:
                    # 检查依赖步骤的状态
                    if not dep_step.status.is_successful:
                        self.status = Status.BLOCKED
                        raise RuntimeError(f"Dependent step {dep} failed or not completed")
                    await original_execute(context)
                    
                step_instance._execute = wrapped_execute.__get__(step_instance)
                
        return step_instance
        
    return decorator

def pipeline(
    name: str = None,
    variables: Dict[str, Any] = None,
    env: Dict[str, str] = None,
    workspace: str = "",
    params: Dict[str, Any] = None,
    secrets: Dict[str, str] = None,
    timeout: Optional[int] = None
) -> Callable:
    """流水线装饰器"""
    def decorator(func: Callable) -> Callable:
        # 获取函数名作为默认流水线名
        pipeline_name = name or func.__name__
        
        # 创建上下文
        context = Context(
            variables=variables or {},
            env=env or {},
            workspace=workspace,
            params=params or {},
            secrets=secrets or {}
        )
        
        # 创建流水线
        pipeline_instance = Pipeline(
            name=pipeline_name,
            context=context,
            timeout=timeout
        )
        
        @wraps(func)
        def wrapper(*args, **kwargs) -> Pipeline:
            # 执行函数
            func(*args, **kwargs)
            return pipeline_instance
            
        # 保存流水线实例
        wrapper._pipeline = pipeline_instance
        return wrapper
        
    return decorator

@contextmanager
def stage(
    name: str,
    parallel: bool = False,
    timeout: Optional[int] = None
) -> Stage:
    """阶段上下文管理器"""
    # 获取当前帧
    frame = inspect.currentframe()
    
    # 查找pipeline装饰器
    while frame:
        if hasattr(frame.f_code, "co_name") and frame.f_code.co_name == "pipeline":
            break
        frame = frame.f_back
        
    if not frame:
        raise RuntimeError("stage must be used within a pipeline")
        
    # 获取流水线实例
    pipeline_func = frame.f_locals.get("wrapper", None)
    if not pipeline_func or not hasattr(pipeline_func, "_pipeline"):
        raise RuntimeError("pipeline instance not found")
        
    pipeline = pipeline_func._pipeline
    
    # 创建阶段
    stage_instance = Stage(
        name=name,
        parallel=parallel,
        timeout=timeout
    )
    
    # 将阶段添加到流水线
    pipeline.stages.append(stage_instance)
    
    try:
        yield stage_instance
    finally:
        pass 