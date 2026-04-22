import json
import random
from typing import Dict, List
from fastapi import WebSocket

class ConnectionManager:
    def __init__(self):
        # room_id -> list of active WebSocket connections
        self.active_connections: Dict[str, List[WebSocket]] = {}
        # room_id -> WebSocket -> player name
        self.player_names: Dict[str, Dict[WebSocket, str]] = {}
        # room_id -> player name -> role
        self.player_roles: Dict[str, Dict[str, str]] = {}
        # room_id -> game started flag
        self.game_started: Dict[str, bool] = {}

    async def connect(self, websocket: WebSocket, room_id: str, player_name: str):
        """Accept a new connection, add it to the room and store the player name."""
        await websocket.accept()
        if room_id not in self.active_connections:
            self.active_connections[room_id] = []
            self.player_names[room_id] = {}
            self.game_started[room_id] = False
        self.active_connections[room_id].append(websocket)
        self.player_names[room_id][websocket] = player_name
        # No role assignment here – will happen when the host starts the game

    def disconnect(self, websocket: WebSocket, room_id: str):
        """Remove a connection from a room when the player leaves."""
        if room_id in self.active_connections and websocket in self.active_connections[room_id]:
            self.active_connections[room_id].remove(websocket)
            self.player_names[room_id].pop(websocket, None)
            if not self.active_connections[room_id]:
                # Clean up room data when empty
                del self.active_connections[room_id]
                self.player_names.pop(room_id, None)
                self.player_roles.pop(room_id, None)
                self.game_started.pop(room_id, None)

    async def broadcast_to_room(self, message: str, room_id: str):
        """Send a message to every connection in the specified room."""
        if room_id in self.active_connections:
            for conn in self.active_connections[room_id]:
                await conn.send_text(message)

    async def start_game(self, room_id: str):
        """Trigger role assignment and notify each player of their secret role."""
        if self.game_started.get(room_id, False):
            return  # Game already started
        self.game_started[room_id] = True
        await self.assign_roles(room_id)
        # Notify everyone that the game has started (optional)
        start_msg = json.dumps({"system": True, "message": "🔔 Game started! Roles have been assigned."})
        await self.broadcast_to_room(start_msg, room_id)

    async def assign_roles(self, room_id: str):
        """Assign random secret roles to each player in the room and send them privately.
        Roles: Werewolf, Seer, Protector, remaining players become Villager.
        """
        if room_id not in self.player_names:
            return
        player_names = list(self.player_names[room_id].values())
        base_roles = ["Werewolf", "Seer", "Protector"]
        # Ensure we have enough roles for all players
        extra_villagers = max(0, len(player_names) - len(base_roles))
        roles = base_roles + ["Villager"] * extra_villagers
        random.shuffle(roles)
        self.player_roles[room_id] = {}
        for ws, role in zip(self.player_names[room_id].keys(), roles):
            name = self.player_names[room_id][ws]
            self.player_roles[room_id][name] = role
            private_msg = {
                "system": True,
                "type": "role_assign",
                "role": role,
                "message": f"Bạn là {role}"
            }
            await ws.send_text(json.dumps(private_msg))
