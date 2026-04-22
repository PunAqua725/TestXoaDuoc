import random

class GameRoom:
    """Manage game state for a room.

    Attributes
    ----------
    players: dict
        Mapping of player names to their assigned role.
    phase: str
        Current phase of the game: 'LOBBY', 'NIGHT', or 'DAY'.
    """

    def __init__(self):
        self.players: dict[str, str] = {}
        self.phase: str = "LOBBY"
        self.round: int = 0
        self.actions: list[dict] = []  # mỗi dict: {"player": str, "type": str, "target": str}
        self.game_over: bool = False
        self.winner: str | None = None

    def assign_roles(self, player_names: list[str]) -> dict[str, str]:
        """Phân vai ngẫu nhiên cho mỗi người chơi trong phòng.

        Returns
        -------
        dict[str, str]
            Mapping of player name to assigned role.
        """
        base_roles = ["Werewolf", "Seer", "Protector"]
        extra_villagers = max(0, len(player_names) - len(base_roles))
        roles = base_roles + ["Villager"] * extra_villagers
        random.shuffle(roles)
        self.players = dict(zip(player_names, roles))
        return self.players

    def start_game(self) -> None:
        """Khởi động trò chơi: xác định vai, thiết lập vòng và chuyển sang đêm đầu tiên."""
        if self.phase != "LOBBY":
            raise RuntimeError("Game already started")
        self.round = 1
        self.set_phase("NIGHT")
        self.actions.clear()
        self.game_over = False
        self.winner = None

    def set_phase(self, phase: str) -> None:
        """Cập nhật giai đoạn hiện tại (LOBBY, NIGHT, DAY)."""
        if phase not in {"LOBBY", "NIGHT", "DAY"}:
            raise ValueError("Invalid game phase")
        self.phase = phase

    def add_action(self, player: str, action_type: str, target: str | None = None) -> None:
        """Thêm một hành động của người chơi cho vòng hiện tại.
        action_type có thể: "kill", "see", "heal", "vote".
        """
        if self.game_over:
            raise RuntimeError("Game already ended")
        # Kiểm tra người chơi còn sống
        role = self.players.get(player)
        if not role or (self.is_dead(player)):
            raise ValueError(f"Player {player} không tồn tại hoặc đã chết")
        # Kiểm tra quyền hành động dựa trên phase và role
        if self.phase == "NIGHT":
            if role == "Werewolf" and action_type != "kill":
                raise ValueError("Sói chỉ có thể kill vào ban đêm")
            if role == "Seer" and action_type != "see":
                raise ValueError("Tiên tri chỉ có thể see vào ban đêm")
            if role == "Protector" and action_type != "heal":
                raise ValueError("Phù thủy chỉ có thể heal vào ban đêm")
        elif self.phase == "DAY":
            if action_type != "vote":
                raise ValueError("Chỉ có thể vote vào ban ngày")
        self.actions.append({"player": player, "type": action_type, "target": target})

    def is_dead(self, player: str) -> bool:
        """Kiểm tra người chơi đã chết chưa (đánh dấu bằng None trong self.players)."""
        return self.players.get(player) is None

    def process_night(self) -> None:
        """Xử lý các hành động ban đêm và cập nhật trạng thái người chơi."""
        if self.phase != "NIGHT":
            raise RuntimeError("Not night phase")
        # Tập hợp các hành động
        kill_targets = []
        heal_targets = []
        for act in self.actions:
            if act["type"] == "kill":
                kill_targets.append(act["target"])
            if act["type"] == "heal":
                heal_targets.append(act["target"])
        # Nếu có nhiều sói, họ có thể cùng chỉ định cùng 1 target – ta lấy target đầu tiên
        if kill_targets:
            victim = kill_targets[0]
            # Kiểm tra có bảo vệ?
            if victim not in heal_targets:
                # Người bị giết
                self.players[victim] = None
        # Dọn actions cho vòng ngày
        self.actions.clear()
        self.set_phase("DAY")
        self.round += 1
        self.check_game_over()

    def process_day(self) -> None:
        """Xử lý bỏ phiếu ngày và chuyển sang đêm nếu trò chơi chưa kết thúc."""
        if self.phase != "DAY":
            raise RuntimeError("Not day phase")
        vote_counts: dict[str, int] = {}
        for act in self.actions:
            if act["type"] == "vote" and act["target"]:
                vote_counts[act["target"]] = vote_counts.get(act["target"], 0) + 1
        if vote_counts:
            # tìm người có phiếu cao nhất (đơn giản, nếu tie thì không chết)
            max_votes = max(vote_counts.values())
            candidates = [p for p, v in vote_counts.items() if v == max_votes]
            if len(candidates) == 1:
                eliminated = candidates[0]
                self.players[eliminated] = None
        self.actions.clear()
        self.set_phase("NIGHT")
        self.check_game_over()

    def check_game_over(self) -> None:
        """Kiểm tra điều kiện thắng và đánh dấu game_over, winner."""
        alive_roles = [role for role in self.players.values() if role]
        werewolves = sum(1 for r in alive_roles if r == "Werewolf")
        others = len(alive_roles) - werewolves
        if werewolves == 0:
            self.game_over = True
            self.winner = "Villagers"
        elif werewolves >= others:
            self.game_over = True
            self.winner = "Werewolves"
        # Nếu game_over, chuyển phase về LOBBY
        if self.game_over:
            self.set_phase("LOBBY")

    def get_state(self) -> dict:
        """Trả về trạng thái hiện tại của phòng, dùng để truyền cho client."""
        return {
            "phase": self.phase,
            "round": self.round,
            "players": {p: r for p, r in self.players.items() if r is not None},
            "dead": [p for p, r in self.players.items() if r is None],
            "game_over": self.game_over,
            "winner": self.winner,
        }


    def assign_roles(self, player_names: list[str]) -> dict[str, str]:
        """Assign random secret roles to each player.

        Parameters
        ----------
        player_names: list[str]
            List of player names present in the room.

        Returns
        -------
        dict[str, str]
            Mapping of player name to assigned role.
        """
        base_roles = ["Werewolf", "Seer", "Protector"]
        roles = base_roles + ["Villager"] * (len(player_names) - len(base_roles))
        random.shuffle(roles)
        self.players = dict(zip(player_names, roles))
        return self.players

    def set_phase(self, phase: str) -> None:
        """Set the current game phase.

        Allowed values: 'LOBBY', 'NIGHT', 'DAY'.
        """
        if phase not in {"LOBBY", "NIGHT", "DAY"}:
            raise ValueError("Invalid game phase")
        self.phase = phase
