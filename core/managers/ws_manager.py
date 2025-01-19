from typing import Dict, Set, Any, List
from core.base_manager import BaseManager
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
import json
import logging
from datetime import datetime
from pydantic import BaseModel

class WSClientInfo(BaseModel):
    client_id: str
    connection_count: int
    last_seen: datetime

class WSMessage(BaseModel):
    client_id: str
    message: Any

class WSManager(BaseManager):
    def __init__(self, app: FastAPI):
        super().__init__(app, "ws")
        self._active_connections: Dict[str, Set[WebSocket]] = {}
        self.logger = logging.getLogger(__name__)
        
    def setup_routes(self) -> None:
        """设置路由"""
        self._router.websocket("/connect/{client_id}")(self.websocket_endpoint)
        self._router.get("/clients")(self.list_clients)
        self._router.post("/broadcast")(self.broadcast_message)
        self._router.post("/send/{client_id}")(self.send_message)
        
    async def _initialize(self) -> None:
        """初始化WebSocket管理器"""
        self.logger.info("Initializing WebSocket manager")
        
    async def _start(self) -> None:
        """启动WebSocket管理器"""
        self.logger.info("Starting WebSocket manager")
        
    async def _stop(self) -> None:
        """停止WebSocket管理器"""
        self.logger.info("Stopping WebSocket manager")
        # 关闭所有活动连接
        for connections in self._active_connections.values():
            for connection in connections:
                await connection.close()
        self._active_connections.clear()
        
    async def _cleanup(self) -> None:
        """清理WebSocket管理器"""
        self.logger.info("Cleaning up WebSocket manager")
        await self._stop()
        
    async def connect(self, websocket: WebSocket, client_id: str) -> None:
        """处理新的WebSocket连接"""
        await websocket.accept()
        if client_id not in self._active_connections:
            self._active_connections[client_id] = set()
        self._active_connections[client_id].add(websocket)
        self.logger.info(f"Client {client_id} connected")
        
    async def disconnect(self, websocket: WebSocket, client_id: str) -> None:
        """处理WebSocket断开连接"""
        self._active_connections[client_id].discard(websocket)
        if not self._active_connections[client_id]:
            del self._active_connections[client_id]
        self.logger.info(f"Client {client_id} disconnected")
        
    async def broadcast(self, message: Any, exclude: Set[str] = None) -> None:
        """广播消息给所有连接的客户端"""
        exclude = exclude or set()
        message_data = {
            "timestamp": datetime.now().isoformat(),
            "data": message
        }
        
        for client_id, connections in self._active_connections.items():
            if client_id in exclude:
                continue
            for connection in connections:
                try:
                    await connection.send_json(message_data)
                except Exception as e:
                    self.logger.error(f"Error sending message to client {client_id}: {str(e)}")
                    
    async def send_to_client(self, client_id: str, message: Any) -> bool:
        """发送消息给特定客户端"""
        if client_id not in self._active_connections:
            return False
            
        message_data = {
            "timestamp": datetime.now().isoformat(),
            "data": message
        }
        
        success = False
        for connection in self._active_connections[client_id]:
            try:
                await connection.send_json(message_data)
                success = True
            except Exception as e:
                self.logger.error(f"Error sending message to client {client_id}: {str(e)}")
                
        return success
        
    def get_active_clients(self) -> Set[str]:
        """获取所有活动客户端ID"""
        return set(self._active_connections.keys())
        
    # API路由处理器
    async def websocket_endpoint(self, websocket: WebSocket, client_id: str):
        """WebSocket连接端点"""
        await self.connect(websocket, client_id)
        try:
            while True:
                # 等待消息
                data = await websocket.receive_json()
                # 处理消息
                await websocket.send_json({
                    "timestamp": datetime.now().isoformat(),
                    "type": "echo",
                    "data": data
                })
        except WebSocketDisconnect:
            await self.disconnect(websocket, client_id)
            
    async def list_clients(self) -> List[WSClientInfo]:
        """列出所有活动客户端"""
        now = datetime.now()
        return [
            WSClientInfo(
                client_id=client_id,
                connection_count=len(connections),
                last_seen=now
            )
            for client_id, connections in self._active_connections.items()
        ]
        
    async def broadcast_message(self, message: WSMessage) -> dict:
        """广播消息的路由处理器"""
        await self.broadcast(message.message, {message.client_id})
        return {"status": "success", "message": "Message broadcasted"}
        
    async def send_message(self, client_id: str, message: WSMessage) -> dict:
        """发送消息给特定客户端的路由处理器"""
        success = await self.send_to_client(client_id, message.message)
        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Client {client_id} not found or not connected"
            )
        return {"status": "success", "message": "Message sent"} 