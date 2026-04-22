# 🐺 Dự Án Game Ma Sói (Werewolf Game)

Đây là bộ khung (Boilerplate) cho hệ thống Backend của trò chơi Ma Sói nhiều người chơi (Multiplayer), sử dụng giao tiếp thời gian thực (Real-time).

## 🛠 Công nghệ sử dụng
- **Backend:** Python (FastAPI)
- **Real-time Communication:** WebSockets (Cho phép chat và cập nhật trạng thái ngay lập tức)
- **Frontend (Demo):** HTML, CSS, JavaScript thuần

---

## 🚀 Hướng dẫn khởi chạy
### Bước 1: Khởi động Server (Backend)
Bạn cần mở Terminal (hoặc Command Prompt / PowerShell) và trỏ vào thư mục dự án:

1. Di chuyển vào thư mục backend:
   ```bash
   cd backend
   ```

2. Cài đặt các thư viện cần thiết (Chỉ cần chạy 1 lần đầu tiên):
   ```bash
   pip install -r requirements.txt
   ```

3. Khởi chạy Server:
   ```bash
   uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
   ```
   *Lưu ý: Đừng tắt cửa sổ Terminal này trong suốt quá trình test. Nếu thấy dòng `Application startup complete.` nghĩa là Server đang chạy ngon lành.*

### Bước 2: Khởi động Giao diện (Frontend)
1. Dùng trình duyệt (Chrome/Edge/Cốc Cốc) mở trực tiếp file `frontend/index.html`.
2. Hoặc bạn có thể click đúp chuột vào file `index.html` trong thư mục `frontend` trên máy tính.

---

## 🎮 Hướng dẫn Demo (Cách test tính năng cho giáo viên xem)

Để chứng minh hệ thống có thể kết nối nhiều người chơi cùng lúc (Multiplayer):

1. **Mở 2 tab trình duyệt** (Mở file `index.html` ở 2 tab khác nhau).
2. **Tab 1:**
   - Tên người chơi: `Tiên Tri` (hoặc tên bất kỳ)
   - ID Phòng: `room1` (bắt buộc phải giống nhau)
   - Bấm **Vào Phòng**.
3. **Tab 2:**
   - Tên người chơi: `Sói`
   - ID Phòng: `room1`
   - Bấm **Vào Phòng**.
4. **Test Chat:** Gõ tin nhắn ở Tab 1 và nhấn Gửi. Bạn sẽ thấy tin nhắn ngay lập tức nảy sang Tab 2 (và ngược lại) mà không cần tải lại trang!
5. **Bắt đầu Game:** Khi mọi người đã vào phòng, nhấn nút **Bắt đầu Game** (được thêm mới trên giao diện). Server sẽ tự động phân vai và gửi thông báo bí mật cho từng người chơi. Mỗi người sẽ thấy một khung hiển thị vai trò và các nút hành động tương ứng (Cắn, Soi, Bảo vệ).

---

## 📂 Cấu trúc thư mục
```text
Werewolf/
├── backend/
│   ├── app/
│   │   ├── main.py                # Khởi tạo API và định tuyến WebSocket
│   │   ├── websocket_manager.py   # Quản lý Phòng (Room) và Kết nối mạng
│   │   └── game_state.py         # Quản lý trạng thái game, phân vai và chuyển pha
│   └── requirements.txt           # Danh sách thư viện Python
├── frontend/
│   ├── index.html                 # Giao diện Test WebSockets
│   └── style.css                  # Styles cho UI
└── README.md                      # File hướng dẫn này
```
