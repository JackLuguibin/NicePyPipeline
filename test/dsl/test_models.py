import unittest
import asyncio
from datetime import datetime
from core.dsl.models import Status, Context, ContextManager, LogHandler


class MockLogHandler(LogHandler):
    """模拟日志处理器"""
    async def handle_log(self, source: str, level: str, message: str):
        pass


class TestStatus(unittest.IsolatedAsyncioTestCase):
    """测试Status枚举类"""
    
    async def test_is_finished(self):
        """测试is_finished属性"""
        self.assertTrue(Status.SUCCESS.is_finished)
        self.assertTrue(Status.FAILED.is_finished)
        self.assertTrue(Status.SKIPPED.is_finished)
        self.assertTrue(Status.CANCELLED.is_finished)
        self.assertTrue(Status.TIMEOUT.is_finished)
        self.assertFalse(Status.PENDING.is_finished)
        self.assertFalse(Status.RUNNING.is_finished)
        self.assertFalse(Status.BLOCKED.is_finished)
        
    async def test_is_successful(self):
        """测试is_successful属性"""
        self.assertTrue(Status.SUCCESS.is_successful)
        self.assertFalse(Status.FAILED.is_successful)
        self.assertFalse(Status.PENDING.is_successful)
        
    async def test_can_continue(self):
        """测试can_continue属性"""
        self.assertTrue(Status.SUCCESS.can_continue)
        self.assertTrue(Status.PENDING.can_continue)
        self.assertTrue(Status.RUNNING.can_continue)
        self.assertFalse(Status.FAILED.can_continue)
        self.assertFalse(Status.CANCELLED.can_continue)
        self.assertFalse(Status.TIMEOUT.can_continue)


class TestContext(unittest.IsolatedAsyncioTestCase):
    """测试Context类"""
    
    async def asyncSetUp(self):
        """测试前准备"""
        self.context = Context(
            name="test_context",
            type="test"
        )
        
    async def test_variable_operations(self):
        """测试变量操作"""
        # 测试本地变量
        self.context.set_variable("local_var", "local_value")
        self.assertEqual(self.context.get_variable("local_var"), "local_value")
        
        # 测试父级变量
        parent_context = Context(name="parent", type="test")
        parent_context.set_variable("parent_var", "parent_value")
        self.context.parent = parent_context
        self.assertEqual(self.context.get_variable("parent_var"), "parent_value")
        
        # 测试全局变量
        self.context.set_variable("global_var", "global_value", scope="global")
        self.assertEqual(parent_context.get_variable("global_var"), "global_value")
        
    async def test_env_operations(self):
        """测试环境变量操作"""
        self.context.set_env("TEST_ENV", "test_value")
        self.assertEqual(self.context.get_env("TEST_ENV"), "test_value")
        
    async def test_secret_operations(self):
        """测试密钥操作"""
        self.context.secrets["TEST_SECRET"] = "secret_value"
        self.assertEqual(self.context.get_secret("TEST_SECRET"), "secret_value")
        
    async def test_artifact_operations(self):
        """测试制品操作"""
        self.context.set_artifact("test_artifact", "/path/to/artifact")
        self.assertEqual(
            self.context.get_artifact_path("test_artifact"),
            "/path/to/artifact"
        )
        
    async def test_output_operations(self):
        """测试输出操作"""
        self.context.set_output("test_output", "output_value")
        self.assertEqual(self.context.get_output("test_output"), "output_value")
        
    async def test_workspace_operations(self):
        """测试工作目录操作"""
        self.context.workspace = "/test/workspace"
        self.assertEqual(self.context.get_workspace(), "/test/workspace")
        
    async def test_create_child(self):
        """测试创建子上下文"""
        child = self.context.create_child("child", "test")
        self.assertEqual(child.name, "child")
        self.assertEqual(child.type, "test")
        self.assertEqual(child.get_workspace(), self.context.workspace)


class TestContextManager(unittest.IsolatedAsyncioTestCase):
    """测试ContextManager类"""
    
    async def asyncSetUp(self):
        """测试前准备"""
        self.manager = ContextManager()
        self.root_context = Context(name="root", type="test")
        
    async def test_push_and_pop(self):
        """测试压入和弹出上下文"""
        # 测试压入
        self.manager.push(self.root_context)
        self.assertEqual(self.manager.current, self.root_context)
        
        # 测试压入子上下文
        child_context = Context(name="child", type="test")
        self.manager.push(child_context)
        self.assertEqual(self.manager.current, child_context)
        self.assertEqual(child_context.parent, self.root_context)
        
        # 测试弹出
        popped = self.manager.pop()
        self.assertEqual(popped, child_context)
        self.assertEqual(self.manager.current, self.root_context)
        
    async def test_get_context(self):
        """测试获取上下文"""
        self.manager.push(self.root_context)
        self.assertEqual(
            self.manager.get(self.root_context.id),
            self.root_context
        )
        
    async def test_scope(self):
        """测试上下文作用域"""
        with self.manager.scope(self.root_context):
            self.assertEqual(self.manager.current, self.root_context)
            child_context = Context(name="child", type="test")
            with self.manager.scope(child_context):
                self.assertEqual(self.manager.current, child_context)
            self.assertEqual(self.manager.current, self.root_context)
        self.assertIsNone(self.manager.current)


if __name__ == '__main__':
    unittest.main() 