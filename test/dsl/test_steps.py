import unittest
import asyncio
from datetime import datetime
from unittest.mock import Mock, patch
from core.dsl.models import Status, Context, ContextManager, Step
from core.dsl.steps import (
    ShellStep, DockerStep, PythonStep, PythonFunctionStep,
    HttpStep, ParallelStep, ConditionStep, RetryStep
)

class MockLogHandler:
    """模拟日志处理器"""
    async def handle_log(self, source: str, level: str, message: str):
        pass

class TestStep(unittest.TestCase):
    """测试Step基类"""
    
    def setUp(self):
        """测试前准备"""
        self.context = Context(name="test", type="test")
        self.log_handler = MockLogHandler()
        
    def create_step(self, name="test_step", **kwargs):
        """创建测试步骤"""
        class TestStep(Step):
            async def _execute(self, context):
                pass
                
        return TestStep(name=name, log_handler=self.log_handler, **kwargs)
        
    def test_step_lifecycle(self):
        """测试步骤生命周期"""
        step = self.create_step()
        
        # 测试初始状态
        self.assertEqual(step.status, Status.PENDING)
        self.assertIsNone(step.started_at)
        self.assertIsNone(step.finished_at)
        
        # 运行步骤
        asyncio.run(step.run(self.context))
        
        # 测试完成状态
        self.assertEqual(step.status, Status.SUCCESS)
        self.assertIsNotNone(step.started_at)
        self.assertIsNotNone(step.finished_at)
        self.assertIsNotNone(step.duration)
        
    def test_step_timeout(self):
        """测试步骤超时"""
        class SlowStep(Step):
            async def _execute(self, context):
                await asyncio.sleep(0.2)
                
        step = SlowStep(
            name="slow_step",
            timeout=0.1,
            log_handler=self.log_handler
        )
        
        with self.assertRaises(asyncio.TimeoutError):
            asyncio.run(step.run(self.context))
            
        self.assertEqual(step.status, Status.TIMEOUT)
        self.assertIsNotNone(step.error)
        
    def test_step_retry(self):
        """测试步骤重试"""
        attempts = 0
        
        class FailingStep(Step):
            async def _execute(self, context):
                nonlocal attempts
                attempts += 1
                if attempts < 3:
                    raise RuntimeError("Simulated failure")
                    
        step = FailingStep(
            name="failing_step",
            retry=2,
            retry_delay=0.1,
            log_handler=self.log_handler
        )
        
        asyncio.run(step.run(self.context))
        
        self.assertEqual(attempts, 3)
        self.assertEqual(step.status, Status.SUCCESS)
        self.assertEqual(step.retry_count, 2)
        
    def test_step_cancel(self):
        """测试步骤取消"""
        step = self.create_step()
        asyncio.run(step.cancel())
        
        self.assertTrue(step.is_cancelled)
        self.assertEqual(step.status, Status.CANCELLED)

class TestShellStep(unittest.TestCase):
    """测试ShellStep类"""
    
    def setUp(self):
        """测试前准备"""
        self.context = Context(name="test", type="test")
        self.log_handler = MockLogHandler()
        
    @patch("asyncio.create_subprocess_shell")
    async def test_shell_execution(self, mock_create_subprocess):
        """测试Shell命令执行"""
        # 模拟进程
        mock_process = Mock()
        mock_process.communicate = Mock(
            return_value=(b"stdout", b"stderr")
        )
        mock_process.returncode = 0
        mock_create_subprocess.return_value = mock_process
        
        step = ShellStep(
            name="shell_step",
            command="echo 'test'",
            log_handler=self.log_handler
        )
        
        await step.run(self.context)
        
        self.assertEqual(step.status, Status.SUCCESS)
        self.assertEqual(step.get_output("stdout"), "stdout")
        self.assertEqual(step.get_output("stderr"), "stderr")
        self.assertEqual(step.get_output("exit_code"), 0)
        
    @patch("asyncio.create_subprocess_shell")
    async def test_shell_failure(self, mock_create_subprocess):
        """测试Shell命令失败"""
        # 模拟失败的进程
        mock_process = Mock()
        mock_process.communicate = Mock(
            return_value=(b"", b"error")
        )
        mock_process.returncode = 1
        mock_create_subprocess.return_value = mock_process
        
        step = ShellStep(
            name="shell_step",
            command="invalid_command",
            log_handler=self.log_handler
        )
        
        with self.assertRaises(RuntimeError):
            await step.run(self.context)
            
        self.assertEqual(step.status, Status.FAILED)
        self.assertIsNotNone(step.error)

class TestHttpStep(unittest.TestCase):
    """测试HttpStep类"""
    
    def setUp(self):
        """测试前准备"""
        self.context = Context(name="test", type="test")
        self.log_handler = MockLogHandler()
        
    @patch("aiohttp.ClientSession")
    async def test_http_request(self, mock_session):
        """测试HTTP请求"""
        # 模拟响应
        mock_response = Mock()
        mock_response.status = 200
        mock_response.text = Mock(
            return_value="response body"
        )
        mock_response.headers = {"Content-Type": "text/plain"}
        
        # 模拟会话
        mock_session_instance = Mock()
        mock_session_instance.request = Mock(
            return_value=mock_response
        )
        mock_session.return_value = mock_session_instance
        
        step = HttpStep(
            name="http_step",
            url="http://example.com",
            method="GET",
            log_handler=self.log_handler
        )
        
        await step.run(self.context)
        
        self.assertEqual(step.status, Status.SUCCESS)
        self.assertEqual(step.get_output("status"), 200)
        self.assertEqual(step.get_output("body"), "response body")
        
    @patch("aiohttp.ClientSession")
    async def test_http_failure(self, mock_session):
        """测试HTTP请求失败"""
        # 模拟失败的响应
        mock_response = Mock()
        mock_response.status = 404
        mock_response.text = Mock(
            return_value="Not Found"
        )
        
        # 模拟会话
        mock_session_instance = Mock()
        mock_session_instance.request = Mock(
            return_value=mock_response
        )
        mock_session.return_value = mock_session_instance
        
        step = HttpStep(
            name="http_step",
            url="http://example.com/not-found",
            method="GET",
            log_handler=self.log_handler
        )
        
        with self.assertRaises(RuntimeError):
            await step.run(self.context)
            
        self.assertEqual(step.status, Status.FAILED)
        self.assertIsNotNone(step.error)

class TestParallelStep(unittest.TestCase):
    """测试ParallelStep类"""
    
    def setUp(self):
        """测试前准备"""
        self.context = Context(name="test", type="test")
        self.log_handler = MockLogHandler()
        
    async def test_parallel_execution(self):
        """测试并行执行"""
        # 创建测试步骤
        steps = []
        for i in range(3):
            step = Step(name=f"step_{i}", log_handler=self.log_handler)
            steps.append(step)
            
        parallel_step = ParallelStep(
            name="parallel_step",
            steps=steps,
            log_handler=self.log_handler
        )
        
        await parallel_step.run(self.context)
        
        self.assertEqual(parallel_step.status, Status.SUCCESS)
        for step in steps:
            self.assertEqual(step.status, Status.SUCCESS)
            
    async def test_parallel_failure(self):
        """测试并行执行失败"""
        # 创建一个会失败的步骤
        class FailingStep(Step):
            async def _execute(self, context):
                raise RuntimeError("Simulated failure")
                
        steps = [
            Step(name="step_1", log_handler=self.log_handler),
            FailingStep(name="failing_step", log_handler=self.log_handler),
            Step(name="step_2", log_handler=self.log_handler)
        ]
        
        parallel_step = ParallelStep(
            name="parallel_step",
            steps=steps,
            log_handler=self.log_handler
        )
        
        with self.assertRaises(RuntimeError):
            await parallel_step.run(self.context)
            
        self.assertEqual(parallel_step.status, Status.FAILED)

if __name__ == '__main__':
    unittest.main() 