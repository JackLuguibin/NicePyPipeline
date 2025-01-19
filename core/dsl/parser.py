from typing import Dict, List, Any, Optional, Type
import yaml
from pathlib import Path
from .models import Pipeline, Stage, Step, Context
import importlib
import sys
import ast
from textwrap import dedent

class DSLParser:
    """DSL解析器"""
    
    def __init__(self):
        self._step_types: Dict[str, Type[Step]] = {}
        
    def register_step_type(self, name: str, step_class: Type[Step]) -> None:
        """注册步骤类型"""
        self._step_types[name] = step_class
        
    def parse_file(self, file_path: str) -> Pipeline:
        """解析流水线文件"""
        with open(file_path, "r") as f:
            content = f.read()
            
        if file_path.endswith(".yaml") or file_path.endswith(".yml"):
            return self.parse_yaml(content)
        elif file_path.endswith(".py"):
            return self.parse_python(content)
        else:
            raise ValueError("Unsupported file type")
            
    def parse_yaml(self, content: str) -> Pipeline:
        """解析YAML格式的流水线定义"""
        try:
            data = yaml.safe_load(content)
            
            # 创建上下文
            context = Context(
                variables=data.get("variables", {}),
                env=data.get("env", {}),
                workspace=data.get("workspace", ""),
                params=data.get("params", {}),
                secrets=data.get("secrets", {})
            )
            
            # 创建流水线
            pipeline = Pipeline(
                name=data["name"],
                context=context
            )
            
            # 解析阶段
            for stage_data in data.get("stages", []):
                stage = Stage(
                    name=stage_data["name"],
                    parallel=stage_data.get("parallel", False)
                )
                
                # 解析步骤
                for step_data in stage_data.get("steps", []):
                    step_type = step_data["type"]
                    if step_type not in self._step_types:
                        raise ValueError(f"Unknown step type: {step_type}")
                        
                    step_class = self._step_types[step_type]
                    step = step_class(**step_data)
                    stage.steps.append(step)
                    
                pipeline.stages.append(stage)
                
            return pipeline
        except Exception as e:
            raise ValueError(f"Failed to parse YAML: {str(e)}")
            
    def parse_python(self, content: str) -> Pipeline:
        """解析Python格式的流水线定义"""
        try:
            # 创建模块
            module = ast.parse(content)
            
            # 查找pipeline定义
            pipeline_def = None
            for node in module.body:
                if isinstance(node, ast.FunctionDef) and node.name == "pipeline":
                    pipeline_def = node
                    break
                    
            if not pipeline_def:
                raise ValueError("No pipeline definition found")
                
            # 解析装饰器参数
            pipeline_args = {}
            for decorator in pipeline_def.decorator_list:
                if isinstance(decorator, ast.Call):
                    if isinstance(decorator.func, ast.Name) and decorator.func.id == "pipeline":
                        for keyword in decorator.keywords:
                            pipeline_args[keyword.arg] = ast.literal_eval(keyword.value)
                            
            # 创建上下文
            context = Context(
                variables=pipeline_args.get("variables", {}),
                env=pipeline_args.get("env", {}),
                workspace=pipeline_args.get("workspace", ""),
                params=pipeline_args.get("params", {}),
                secrets=pipeline_args.get("secrets", {})
            )
            
            # 创建流水线
            pipeline = Pipeline(
                name=pipeline_args.get("name", pipeline_def.name),
                context=context
            )
            
            # 解析阶段
            current_stage = None
            for stmt in pipeline_def.body:
                if isinstance(stmt, ast.With):
                    # stage上下文
                    stage_args = {}
                    if isinstance(stmt.items[0].context_expr, ast.Call):
                        call = stmt.items[0].context_expr
                        if isinstance(call.func, ast.Name) and call.func.id == "stage":
                            for keyword in call.keywords:
                                stage_args[keyword.arg] = ast.literal_eval(keyword.value)
                                
                    current_stage = Stage(
                        name=stage_args["name"],
                        parallel=stage_args.get("parallel", False)
                    )
                    pipeline.stages.append(current_stage)
                    
                elif isinstance(stmt, ast.Expr) and current_stage:
                    # 步骤调用
                    if isinstance(stmt.value, ast.Call):
                        call = stmt.value
                        step_type = call.func.id
                        if step_type not in self._step_types:
                            raise ValueError(f"Unknown step type: {step_type}")
                            
                        step_args = {}
                        for keyword in call.keywords:
                            step_args[keyword.arg] = ast.literal_eval(keyword.value)
                            
                        step_class = self._step_types[step_type]
                        step = step_class(**step_args)
                        current_stage.steps.append(step)
                        
            return pipeline
        except Exception as e:
            raise ValueError(f"Failed to parse Python: {str(e)}")
            
    def generate_python(self, pipeline: Pipeline) -> str:
        """生成Python格式的流水线定义"""
        lines = [
            "from core.dsl.decorators import pipeline, stage",
            "from core.dsl.steps import *",
            "",
            "@pipeline(",
            f"    name='{pipeline.name}',",
            "    variables={",
            "        " + ",\n        ".join(
                f"'{k}': {repr(v)}"
                for k, v in pipeline.context.variables.items()
            ),
            "    },",
            "    env={",
            "        " + ",\n        ".join(
                f"'{k}': '{v}'"
                for k, v in pipeline.context.env.items()
            ),
            "    },",
            f"    workspace='{pipeline.context.workspace}',",
            "    params={",
            "        " + ",\n        ".join(
                f"'{k}': {repr(v)}"
                for k, v in pipeline.context.params.items()
            ),
            "    }",
            ")",
            "def pipeline():",
        ]
        
        for stage in pipeline.stages:
            lines.extend([
                f"    with stage(name='{stage.name}', parallel={stage.parallel}):",
            ])
            
            for step in stage.steps:
                step_args = []
                for field, value in step.dict().items():
                    if field not in ["status", "started_at", "finished_at", "error"]:
                        step_args.append(f"{field}={repr(value)}")
                        
                lines.append(
                    f"        {step.__class__.__name__}({', '.join(step_args)})"
                )
                
        return "\n".join(lines)
        
    def generate_yaml(self, pipeline: Pipeline) -> str:
        """生成YAML格式的流水线定义"""
        data = {
            "name": pipeline.name,
            "variables": pipeline.context.variables,
            "env": pipeline.context.env,
            "workspace": pipeline.context.workspace,
            "params": pipeline.context.params,
            "stages": []
        }
        
        for stage in pipeline.stages:
            stage_data = {
                "name": stage.name,
                "parallel": stage.parallel,
                "steps": []
            }
            
            for step in stage.steps:
                step_data = step.dict(
                    exclude={"status", "started_at", "finished_at", "error"}
                )
                step_data["type"] = step.__class__.__name__
                stage_data["steps"].append(step_data)
                
            data["stages"].append(stage_data)
            
        return yaml.dump(data, sort_keys=False) 