from fastapi import FastAPI,WebSocket,WebSocketDisconnect
from contextlib import asynccontextmanager
import asyncio
from bot import run_bot

class ConnectionManager:
    def __init__(self):
        self.active_connections = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

runningLoop:bool=False

# Background task function
async def background_task():
    """
    This function will run in the background once the server starts.
    You can perform any async operations here.
    """
    print("Background task started")
    while runningLoop:
        run_bot("United States","ETH","USDT")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global runningLoop
    runningLoop=True
    asyncio.create_task(background_task())    
    yield
    runningLoop=False


# Create an app instance
app = FastAPI(lifespan=lifespan)


# Define a route
@app.get("/")
async def root():
    return {"message": "Hello World!!"}

# Define a route with a path parameter
@app.get("/items/{item_id}")
async def read_item(item_id: int):
    return {"item_id": item_id}

