from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from app.websocket_manager import ConnectionManager
import json

app = FastAPI(title="Werewolf Game Backend API")

# Cấu hình CORS để frontend ở cổng khác có thể gọi API/WebSocket
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Khởi tạo Trình quản lý kết nối (Room Manager)
manager = ConnectionManager()

@app.get("/")
async def root():
    return {"status": "ok", "message": "Werewolf API is running!"}

@app.websocket("/ws/room/{room_id}/{player_name}")
async def websocket_endpoint(websocket: WebSocket, room_id: str, player_name: str):
    # Người chơi tham gia phòng
    await manager.connect(websocket, room_id, player_name)
    
    # Thông báo cho cả phòng biết có người mới vào
    join_message = json.dumps({
        "system": True, 
        "message": f"🔥 {player_name} đã tham gia phòng {room_id}!"
    })
    await manager.broadcast_to_room(join_message, room_id)
    
    try:
        while True:
            # Chờ nhận tin nhắn từ người chơi này
            data = await websocket.receive_text()
            
            # Gửi tin nhắn đó cho tất cả người chơi khác trong phòng
            chat_message = json.dumps({
                "system": False,
                "player": player_name,
                "message": data
            })
            await manager.broadcast_to_room(chat_message, room_id)
            
    except WebSocketDisconnect:
        # Xử lý khi người chơi thoát / rớt mạng
        manager.disconnect(websocket, room_id)
        leave_message = json.dumps({
            "system": True, 
            "message": f"💨 {player_name} đã rời khỏi phòng."
        })
        await manager.broadcast_to_room(leave_message, room_id)
