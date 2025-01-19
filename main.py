from fastapi import FastAPI
from core.app import App
import uvicorn
import asyncio

app = FastAPI(title="PyPipelineEx", description="A Jenkins-like Pipeline Execution System")

@app.on_event("startup")
async def startup_event():
    # 初始化应用程序实例，传入FastAPI实例
    app_instance = App(app)
    await app_instance.initialize()
    # 启动应用
    await app_instance.start()

@app.get("/")
async def root():
    return {"message": "Welcome to PyPipelineEx"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True) 