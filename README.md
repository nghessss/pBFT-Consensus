# Raft Consensus Simulator

## 1. Giới thiệu

**Raft Consensus Simulator** là một ứng dụng mô phỏng thuật toán đồng thuận **RAFT** trong môi trường mạng ngang hàng (peer-to-peer).  
Hệ thống được thiết kế theo kiến trúc **tách biệt rõ ràng giữa tầng xử lý consensus và tầng giao diện**, giúp phản ánh đúng cách một hệ thống phân tán thực tế hoạt động.

- Các **RAFT node** chạy độc lập dưới dạng **process riêng**
- Các node **giao tiếp trực tiếp với nhau qua RPC**
- **Streamlit** chỉ đóng vai trò **UI quan sát và điều khiển**, không tham gia vào quá trình đồng thuận

---

## 2. Kiến trúc tổng thể

```

+--------------------+
|    Streamlit UI    |
|     (Observer)     |
+----------+---------+
|
| Status 
v
+--------------------+
|    Raft Node 1     | <---- RPC ----> Raft Node 2
|  (Leader/Follower) | <---- RPC ----> Raft Node 3
+--------------------+ <---- RPC ----> Raft Node 4
^
| 
RPC
| 
+--------------------+
|    Raft Node N     |
+--------------------+

```

### Phân tách trách nhiệm

| Thành phần | Vai trò |
|----------|--------|
| Raft Node | Thực hiện bầu leader, replicate log, commit |
| RPC Node ↔ Node | RequestVote, AppendEntries, Heartbeat |
| Streamlit | Hiển thị trạng thái & mô phỏng lỗi |
| RPC UI ↔ Node | Lấy trạng thái, kill node, partition |

**Streamlit không phải là một node RAFT**.

---

## 3. Công nghệ sử dụng

- **gRPC** cho giao tiếp RPC
- **Streamlit** cho giao diện mô phỏng
- **Multiprocessing / Subprocess** để chạy nhiều node trên **1 máy**

---

## 4. Cấu trúc thư mục

```

raft/
│
├── core/
│   ├── node.py            # Raft Node implementation
│   ├── raft.py            # Core Raft logic (leader election, log replication)
│   ├── state.py           # Node state & persistent data
│   ├── cluster.py         # Cluster manager (spawn nodes)
│   └── ...         
|
├── rpc/
│   ├── raft.proto
│   ├── raft_pb2.py
│   ├── raft_pb2_grpc.py
│   ├── server.py          # RPC server for each node
│   └── client.py          # RPC client utilities
│
├── ui/
│   ├── sidebar.py         # Streamlit sidebar controls
│   ├── cluster_view.py    # HTML visualization
│   └── utils/          
│
├── streamlit_app.py       # Streamlit entry point
├── run_node.py            # Init a Raft Node
├── requirements.txt
└── README.md

```

---

## 5. Cách cài đặt

### 5.1 Tạo môi trường ảo

```bash
python -m venv venv
source venv/bin/activate
```

### 5.2 Cài đặt thư viện

```bash
pip install -r requirements.txt
```

---

## 6. Chạy hệ thống

### 6.1 Khởi động các Raft node

Mỗi node chạy như **một process riêng**:

```bash
python run_node.py --id 1 --port 5001
python run_node.py --id 2 --port 5002
python run_node.py --id 3 --port 5003
...
```

---

### 6.2 Chạy Streamlit UI

```bash
streamlit run streamlit_app.py
```

Mở trình duyệt tại:

```
http://localhost:<PORT>
```

---

## 7. Các chức năng cài đặt

### 7.1 Leader Election

* RequestVote RPC
* Timeout ngẫu nhiên
* Xử lý xung đột bầu leader

### 7.2 Log Replication

* AppendEntries
* Đồng bộ log giữa leader và follower
* Commit khi đạt đa số

### 7.3 Mô phỏng lỗi

| Tình huống        | Mô tả                                     |
| ----------------- | ----------------------------------------- |
| Leader crash      | Kill leader → cluster tự bầu leader mới   |
| Node crash        | Node offline → khi online lại sẽ sync log |
| Network partition | Chia cluster thành 2 nhóm                 |
| Heal network      | Kết nối lại, đảm bảo consistency          |

### 7.4 Visualization

* Màu sắc thể hiện vai trò node
* Hiển thị term, commit index
* Realtime update qua polling RPC

---

## 8. Streamlit có vai trò gì?

* ❌ Không tham gia consensus
* ❌ Không trung gian message giữa các node
* ✅ Quan sát trạng thái cluster
* ✅ Gửi lệnh điều khiển (kill, partition, heal)

> Nếu Streamlit dừng, cluster RAFT vẫn tiếp tục hoạt động bình thường.

---
