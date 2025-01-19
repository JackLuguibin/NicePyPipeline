from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import asyncio
import uuid
import os
import json
from pathlib import Path
from anytree import Node, RenderTree, find, findall
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from ..models.job import Job, Build, JobStatus, BuildStatus, BuildLogHandler, JobType
from core.base_manager import BaseManager

class JobTreeNode(Node):
    """作业树节点"""
    def __init__(self, name: str, parent: Optional['JobTreeNode'] = None, job: Optional[Job] = None):
        super().__init__(name, parent)
        self.job = job
        
    @property
    def path(self) -> str:
        """获取节点完整路径"""
        return "/" + "/".join(node.name for node in self.path if node.name != "root")

class JobManager(BaseManager):
    """作业管理器"""
    def __init__(self, app: FastAPI = None):
        super().__init__(app)
        self._jobs: Dict[str, Job] = {}  # 作业字典 {job_id: job}
        self._builds: Dict[str, Build] = {}  # 构建字典 {build_id: build}
        self._job_tree = JobTreeNode("root")  # 作业树根节点
        self._running_builds: Dict[str, asyncio.Task] = {}  # 正在运行的构建 {build_id: task}
        self._storage_path = Path("data/jobs")  # 作业存储路径
        
    async def _initialize(self) -> None:
        """初始化"""
        # 创建存储目录
        self._storage_path.mkdir(parents=True, exist_ok=True)
        # 加载所有作业
        await self._load_jobs()
        # 设置路由
        if self._app:
            self.setup_routes()
            
    def setup_routes(self) -> None:
        """设置路由"""
        # 作业管理
        self._app.get("/jobs", response_model=List[dict])(self.list_jobs)
        self._app.get("/jobs/{job_id}", response_model=dict)(self.get_job)
        self._app.post("/jobs/folder")(self.create_folder)
        self._app.post("/jobs/pipeline")(self.create_pipeline)
        self._app.put("/jobs/{job_id}")(self.update_job)
        self._app.delete("/jobs/{job_id}")(self.delete_job)
        self._app.get("/jobs/search")(self.search_jobs)
        
        # 构建管理
        self._app.get("/jobs/{job_id}/builds", response_model=List[dict])(self.list_builds)
        self._app.get("/jobs/{job_id}/builds/{build_number}", response_model=dict)(self.get_build)
        self._app.post("/jobs/{job_id}/builds")(self.trigger_build)
        self._app.post("/jobs/{job_id}/builds/{build_number}/cancel")(self.cancel_build)
        self._app.post("/jobs/{job_id}/builds/{build_number}/retry")(self.retry_build)
        self._app.get("/jobs/{job_id}/builds/{build_number}/tree")(self.get_build_tree)
        
        # 作业调用
        self._app.post("/jobs/{job_id}/call")(self.call_job)
        
    async def _load_jobs(self) -> None:
        """加载所有作业"""
        for job_file in self._storage_path.glob("**/*.json"):
            try:
                # 读取作业配置
                job_data = json.loads(job_file.read_text())
                # 创建作业
                job = Job(**job_data)
                # 添加到作业字典
                self._jobs[job.id] = job
                # 添加到作业树
                self._add_to_tree(job)
            except Exception as e:
                self._logger.error(f"Failed to load job from {job_file}: {e}")
                
    def _add_to_tree(self, job: Job) -> None:
        """添加作业到树中"""
        # 解析路径
        parts = job.path.strip("/").split("/")
        current = self._job_tree
        
        # 创建或查找路径节点
        for part in parts[:-1]:
            found = None
            for child in current.children:
                if child.name == part:
                    found = child
                    break
            if not found:
                found = JobTreeNode(part, parent=current)
            current = found
            
        # 创建作业节点
        JobTreeNode(parts[-1], parent=current, job=job)
        
    def _remove_from_tree(self, job: Job) -> None:
        """从树中移除作业"""
        node = find(self._job_tree, lambda n: isinstance(n, JobTreeNode) and n.job and n.job.id == job.id)
        if node:
            node.parent = None
            
    async def create_folder(self, name: str, path: str, description: Optional[str] = None, labels: Dict[str, str] = None) -> Job:
        """创建文件夹
        
        Args:
            name: 文件夹名称
            path: 文件夹路径
            description: 文件夹描述
            labels: 标签
        """
        job_data = {
            "id": str(uuid.uuid4()),
            "name": name,
            "path": path,
            "type": JobType.FOLDER,
            "description": description,
            "labels": labels or {}
        }
        
        # 检查路径是否已存在
        if self.get_job_by_path(path):
            raise HTTPException(status_code=400, detail=f"Path {path} already exists")
            
        # 创建作业
        job = Job(**job_data)
        self._jobs[job.id] = job
        self._add_to_tree(job)
        await self._save_job(job)
        
        return job
        
    async def create_pipeline(self, name: str, path: str, pipeline: Pipeline, description: Optional[str] = None,
                            params: Dict[str, Any] = None, labels: Dict[str, str] = None, timeout: Optional[int] = None,
                            max_builds: int = 50) -> Job:
        """创建流水线
        
        Args:
            name: 流水线名称
            path: 流水线路径
            pipeline: 流水线定义
            description: 流水线描述
            params: 默认参数
            labels: 标签
            timeout: 超时时间（秒）
            max_builds: 最大保留构建数
        """
        job_data = {
            "id": str(uuid.uuid4()),
            "name": name,
            "path": path,
            "type": JobType.PIPELINE,
            "description": description,
            "pipeline": pipeline,
            "params": params or {},
            "labels": labels or {},
            "timeout": timeout,
            "max_builds": max_builds
        }
        
        # 检查路径是否已存在
        if self.get_job_by_path(path):
            raise HTTPException(status_code=400, detail=f"Path {path} already exists")
            
        # 创建作业
        job = Job(**job_data)
        self._jobs[job.id] = job
        self._add_to_tree(job)
        await self._save_job(job)
        
        return job
        
    async def update_job(self, job_id: str, job_data: dict) -> Job:
        """更新作业"""
        job = self._jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
            
        # 如果更新了路径，检查新路径是否可用
        if "path" in job_data and job_data["path"] != job.path:
            if self.get_job_by_path(job_data["path"]):
                raise HTTPException(status_code=400, detail=f"Job path {job_data['path']} already exists")
            # 从树中移除旧节点
            self._remove_from_tree(job)
            
        # 更新作业
        job.update(**job_data)
        
        # 如果更新了路径，添加到新位置
        if "path" in job_data:
            self._add_to_tree(job)
            
        # 保存更新
        await self._save_job(job)
        
        return job
        
    async def delete_job(self, job_id: str) -> None:
        """删除作业"""
        job = self._jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
            
        # 标记为已删除
        job.status = JobStatus.DELETED
        await self._save_job(job)
        
        # 从树中移除
        self._remove_from_tree(job)
        # 从字典中移除
        del self._jobs[job_id]
        
    async def _save_job(self, job: Job) -> None:
        """保存作业配置"""
        job_path = self._storage_path / f"{job.id}.json"
        job_path.write_text(json.dumps(job.to_dict(), indent=2))
        
    def get_job(self, job_id: str) -> Optional[Job]:
        """获取作业"""
        return self._jobs.get(job_id)
        
    def get_job_by_path(self, path: str) -> Optional[Job]:
        """通过路径获取作业"""
        node = find(self._job_tree, lambda n: isinstance(n, JobTreeNode) and n.path == path)
        return node.job if node and node.job else None
        
    def list_jobs(self, path: Optional[str] = None, include_children: bool = False) -> List[Job]:
        """列出作业
        
        Args:
            path: 可选的路径过滤
            include_children: 是否包含子路径的作业
        """
        if not path:
            return list(self._jobs.values())
            
        jobs = []
        node = find(self._job_tree, lambda n: isinstance(n, JobTreeNode) and n.path == path)
        if not node:
            return jobs
            
        if include_children:
            # 包含所有子节点的作业
            for n in findall(node, lambda n: isinstance(n, JobTreeNode) and n.job):
                if n.job:
                    jobs.append(n.job)
        else:
            # 只包含当前节点的作业
            if node.job:
                jobs.append(node.job)
                
        return jobs
        
    async def trigger_build(self, job_id: str, params: Dict[str, Any] = None, triggered_by: str = None, triggered_from: str = None) -> Build:
        """触发构建"""
        job = self.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
            
        if job.status != JobStatus.ENABLED:
            raise HTTPException(status_code=400, detail=f"Job {job_id} is not enabled")
            
        if job.type != JobType.PIPELINE:
            raise HTTPException(status_code=400, detail=f"Job {job_id} is not a pipeline")
            
        # 创建构建记录
        build = Build(
            id=str(uuid.uuid4()),
            job_id=job_id,
            number=job.last_build_number + 1,
            params=params or {},
            triggered_by=triggered_by,
            triggered_from=triggered_from
        )
        
        # 更新作业信息
        job.last_build_number = build.number
        job.last_build_id = build.id
        
        # 保存构建记录
        self._builds[build.id] = build
        
        # 创建构建任务
        task = asyncio.create_task(self._run_build(build))
        self._running_builds[build.id] = task
        
        return build
        
    async def _run_build(self, build: Build) -> None:
        """运行构建"""
        job = self.get_job(build.job_id)
        if not job:
            return
            
        build.status = BuildStatus.RUNNING
        build.started_at = datetime.now()
        
        try:
            # 设置构建日志处理器
            log_handler = BuildLogHandler(build)
            job.pipeline.log_handler = log_handler
            
            # 更新流水线参数
            job.pipeline.context.params.update(build.params)
            
            # 运行流水线
            await job.pipeline.run()
            
            # 更新构建状态
            build.status = BuildStatus.SUCCESS
            job.last_success_build_id = build.id
            
        except Exception as e:
            build.status = BuildStatus.FAILED
            build.error = str(e)
            job.last_failure_build_id = build.id
            
        finally:
            build.finished_at = datetime.now()
            if build.started_at:
                build.duration = (build.finished_at - build.started_at).total_seconds()
            
            # 更新作业状态
            job.last_build_status = build.status
            await self._save_job(job)
            
            # 清理旧的构建记录
            await self._cleanup_builds(job)
            
            # 清理运行记录
            if build.id in self._running_builds:
                del self._running_builds[build.id]
                
    async def cancel_build(self, build_id: str) -> None:
        """取消构建"""
        if build_id not in self._running_builds:
            raise HTTPException(status_code=404, detail=f"Build {build_id} not found or not running")
            
        # 取消构建任务
        task = self._running_builds[build_id]
        task.cancel()
        
        try:
            await task
        except asyncio.CancelledError:
            # 更新构建状态
            build = self._builds[build_id]
            build.status = BuildStatus.CANCELLED
            build.finished_at = datetime.now()
            if build.started_at:
                build.duration = (build.finished_at - build.started_at).total_seconds()
                
            # 更新作业状态
            job = self.get_job(build.job_id)
            if job:
                job.last_build_status = BuildStatus.CANCELLED
                await self._save_job(job)
                
    def get_build(self, job_id: str, build_number: int) -> Optional[Build]:
        """获取构建记录"""
        for build in self._builds.values():
            if build.job_id == job_id and build.number == build_number:
                return build
        return None
        
    def list_builds(self, job_id: str, limit: int = 10) -> List[Build]:
        """列出构建记录"""
        builds = [b for b in self._builds.values() if b.job_id == job_id]
        builds.sort(key=lambda x: x.number, reverse=True)
        return builds[:limit]
        
    def print_tree(self) -> str:
        """打印作业树"""
        output = []
        for pre, _, node in RenderTree(self._job_tree):
            status = ""
            if isinstance(node, JobTreeNode) and node.job:
                status = f" [{node.job.status.value}]"
            output.append(f"{pre}{node.name}{status}")
        return "\n".join(output)
        
    async def _cleanup_builds(self, job: Job) -> None:
        """清理旧的构建记录"""
        builds = self.list_builds(job.id)
        if len(builds) > job.max_builds:
            # 保留最新的构建
            builds_to_remove = builds[job.max_builds:]
            for build in builds_to_remove:
                if build.id in self._builds:
                    del self._builds[build.id]
                    
    async def call_job(self, job_id: str, params: Dict[str, Any] = None, caller_build: Build = None) -> Build:
        """调用其他作业
        
        Args:
            job_id: 要调用的作业ID
            params: 调用参数
            caller_build: 调用方构建记录
        """
        build = await self.trigger_build(
            job_id=job_id,
            params=params,
            triggered_by=caller_build.triggered_by if caller_build else None,
            triggered_from=caller_build.id if caller_build else None
        )
        
        if caller_build:
            caller_build.sub_builds.append(build.id)
            
        return build
        
    def get_build_tree(self, build_id: str) -> Dict:
        """获取构建树
        
        返回构建及其所有子构建的树状结构
        """
        build = self._builds.get(build_id)
        if not build:
            return {}
            
        result = build.to_dict()
        result["sub_builds"] = []
        
        for sub_id in build.sub_builds:
            sub_tree = self.get_build_tree(sub_id)
            if sub_tree:
                result["sub_builds"].append(sub_tree)
                
        return result
        
    async def retry_build(self, job_id: str, build_number: int) -> Build:
        """重试构建"""
        old_build = self.get_build(job_id, build_number)
        if not old_build:
            raise HTTPException(status_code=404, detail=f"Build {build_number} not found")
            
        # 创建新的构建，继承原构建的参数
        return await self.trigger_build(
            job_id=job_id,
            params=old_build.params,
            triggered_by=old_build.triggered_by
        )
        
    def search_jobs(self, query: str = None, labels: Dict[str, str] = None, status: JobStatus = None) -> List[Job]:
        """搜索作业
        
        Args:
            query: 搜索关键词（匹配名称和描述）
            labels: 标签过滤
            status: 状态过滤
        """
        jobs = list(self._jobs.values())
        
        if query:
            query = query.lower()
            jobs = [
                job for job in jobs
                if query in job.name.lower() or
                   (job.description and query in job.description.lower())
            ]
            
        if labels:
            jobs = [
                job for job in jobs
                if all(job.labels.get(k) == v for k, v in labels.items())
            ]
            
        if status:
            jobs = [job for job in jobs if job.status == status]
            
        return jobs 