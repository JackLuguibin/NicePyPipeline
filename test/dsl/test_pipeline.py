import unittest
import asyncio
from datetime import datetime
from core.dsl.models import (
    Status, Context, ContextManager,
    Step, Stage, Pipeline
)

class MockStep(Step):
    """模拟步骤"""
    should_fail: bool = False  # 是否应该失败
    sleep_time: float = 0.0  # 睡眠时间
    
    async def _execute(self, context):
        if self.sleep_time > 0:
            await asyncio.sleep(self.sleep_time)
        if self.should_fail:
            raise RuntimeError("Step failed")

class TestStage(unittest.IsolatedAsyncioTestCase):
    """测试Stage类"""
    
    async def asyncSetUp(self):
        """测试前准备"""
        self.context = Context(name="test", type="test")
        self.context_manager = ContextManager()
        
    async def test_stage_sequential_execution(self):
        """测试串行执行"""
        stage = Stage(
            name="test_stage",
            parallel=False,
            context_manager=self.context_manager
        )
        
        # 添加步骤
        for i in range(3):
            step = MockStep(name=f"step_{i}")
            stage.add_step(step)
            
        await stage.run(self.context)
        
        self.assertEqual(stage.status, Status.SUCCESS)
        for step in stage.steps:
            self.assertEqual(step.status, Status.SUCCESS)
            
    async def test_stage_parallel_execution(self):
        """测试并行执行"""
        stage = Stage(
            name="test_stage",
            parallel=True,
            context_manager=self.context_manager
        )
        
        # 添加步骤
        for i in range(3):
            step = MockStep(name=f"step_{i}", sleep_time=0.1)
            stage.add_step(step)
            
        start_time = datetime.now()
        await stage.run(self.context)
        duration = (datetime.now() - start_time).total_seconds()
        
        self.assertEqual(stage.status, Status.SUCCESS)
        self.assertLess(duration, 0.3)  # 验证并行执行
        
    async def test_stage_failure(self):
        """测试阶段失败"""
        stage = Stage(
            name="test_stage",
            context_manager=self.context_manager
        )
        
        # 添加一个会失败的步骤
        stage.add_step(MockStep(name="step_1"))
        stage.add_step(MockStep(name="step_2", should_fail=True))
        stage.add_step(MockStep(name="step_3"))
        
        with self.assertRaises(RuntimeError):
            await stage.run(self.context)
            
        self.assertEqual(stage.status, Status.FAILED)
        self.assertEqual(stage.steps[0].status, Status.SUCCESS)
        self.assertEqual(stage.steps[1].status, Status.FAILED)
        self.assertEqual(stage.steps[2].status, Status.PENDING)
        
    async def test_stage_timeout(self):
        """测试阶段超时"""
        stage = Stage(
            name="test_stage",
            timeout=0.1,
            context_manager=self.context_manager
        )
        
        # 添加一个耗时的步骤
        stage.add_step(MockStep(name="slow_step", sleep_time=0.2))
        
        with self.assertRaises(asyncio.TimeoutError):
            await stage.run(self.context)
            
        self.assertEqual(stage.status, Status.TIMEOUT)
        
    async def test_stage_cancel(self):
        """测试阶段取消"""
        stage = Stage(
            name="test_stage",
            context_manager=self.context_manager
        )
        
        # 添加步骤
        for i in range(3):
            step = MockStep(name=f"step_{i}", sleep_time=0.1)
            stage.add_step(step)
            
        # 在另一个任务中取消阶段
        async def cancel_stage():
            await asyncio.sleep(0.1)
            await stage.cancel()
            
        await asyncio.gather(
            stage.run(self.context),
            cancel_stage()
        )
        
        self.assertEqual(stage.status, Status.CANCELLED)
        self.assertTrue(any(step.status == Status.CANCELLED for step in stage.steps))

class TestPipeline(unittest.IsolatedAsyncioTestCase):
    """测试Pipeline类"""
    
    async def asyncSetUp(self):
        """测试前准备"""
        self.context = Context(name="test", type="test")
        self.context_manager = ContextManager()
        
    def create_pipeline(self, name="test_pipeline"):
        """创建测试流水线"""
        return Pipeline(
            name=name,
            context=self.context,
            context_manager=self.context_manager
        )
        
    async def test_pipeline_execution(self):
        """测试流水线执行"""
        pipeline = self.create_pipeline()
        
        # 添加阶段
        for i in range(3):
            stage = Stage(name=f"stage_{i}")
            # 每个阶段添加步骤
            for j in range(2):
                stage.add_step(MockStep(name=f"step_{i}_{j}"))
            pipeline.add_stage(stage)
            
        await pipeline.run()
        
        self.assertEqual(pipeline.status, Status.SUCCESS)
        for stage in pipeline.stages:
            self.assertEqual(stage.status, Status.SUCCESS)
            for step in stage.steps:
                self.assertEqual(step.status, Status.SUCCESS)
                
    async def test_pipeline_stage_failure(self):
        """测试流水线阶段失败"""
        pipeline = self.create_pipeline()
        
        # 添加正常阶段
        stage1 = Stage(name="stage_1")
        stage1.add_step(MockStep(name="step_1"))
        pipeline.add_stage(stage1)
        
        # 添加失败阶段
        stage2 = Stage(name="stage_2")
        stage2.add_step(MockStep(name="step_2", should_fail=True))
        pipeline.add_stage(stage2)
        
        # 添加不会执行的阶段
        stage3 = Stage(name="stage_3")
        stage3.add_step(MockStep(name="step_3"))
        pipeline.add_stage(stage3)
        
        with self.assertRaises(RuntimeError):
            await pipeline.run()
        
        self.assertEqual(pipeline.status, Status.FAILED)
        self.assertEqual(stage1.status, Status.SUCCESS)
        self.assertEqual(stage2.status, Status.FAILED)
        self.assertEqual(stage3.status, Status.PENDING)
        
    async def test_pipeline_timeout(self):
        """测试流水线超时"""
        pipeline = self.create_pipeline()
        pipeline.timeout = 0.1
        
        # 添加耗时阶段
        stage = Stage(name="slow_stage")
        stage.add_step(MockStep(name="slow_step", sleep_time=0.2))
        pipeline.add_stage(stage)
        
        with self.assertRaises(asyncio.TimeoutError):
            await pipeline.run()
        
        self.assertEqual(pipeline.status, Status.TIMEOUT)
        self.assertEqual(stage.status, Status.TIMEOUT)
        
    async def test_pipeline_cancel(self):
        """测试流水线取消"""
        pipeline = self.create_pipeline()
        
        # 添加多个阶段
        for i in range(3):
            stage = Stage(name=f"stage_{i}")
            stage.add_step(MockStep(name=f"step_{i}", sleep_time=0.1))
            pipeline.add_stage(stage)
            
        # 在另一个任务中取消流水线
        async def cancel_pipeline():
            await asyncio.sleep(0.15)
            await pipeline.cancel()
            
        await asyncio.gather(
            pipeline.run(),
            cancel_pipeline()
        )
        
        self.assertEqual(pipeline.status, Status.CANCELLED)
        self.assertTrue(any(stage.status == Status.CANCELLED for stage in pipeline.stages))
        
    async def test_pipeline_context_inheritance(self):
        """测试流水线上下文继承"""
        pipeline = self.create_pipeline()
        
        # 设置根上下文变量
        self.context.set_variable("root_var", "root_value")
        self.context.set_env("ROOT_ENV", "root_env_value")
        
        # 添加测试阶段和步骤
        stage = Stage(name="test_stage")
        
        class ContextTestStep(Step):
            async def _execute(self, context):
                # 验证变量继承
                assert context.get_variable("root_var") == "root_value"
                assert context.get_env("ROOT_ENV") == "root_env_value"
                # 设置新变量
                context.set_variable("step_var", "step_value")
                
        stage.add_step(ContextTestStep(name="test_step"))
        pipeline.add_stage(stage)
        
        await pipeline.run()
        
        self.assertEqual(pipeline.status, Status.SUCCESS)

if __name__ == '__main__':
    unittest.main() 