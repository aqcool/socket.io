# Socket.IO 聊天室示例

经典 Socket.IO 聊天室的 Go 实现。

## 特性

- 多名用户可使用唯一用户名加入聊天室
- 用户可向所有已连接用户发送聊天消息
- 向其他用户广播输入状态
- 提供加入/离开通知及在线人数统计

## 运行方式

```bash
go run main.go
```

服务端默认监听 `http://localhost:3000`。可通过 `PORT` 环境变量指定其他端口。

## 事件

### 客户端 → 服务端

| 事件 | 载荷 | 说明 |
|-------|---------|-------------|
| `add user` | `string`（用户名） | 为连接注册用户名 |
| `new message` | `string`（消息） | 向聊天室发送消息 |
| `typing` | — | 通知其他用户当前用户正在输入 |
| `stop typing` | — | 通知其他用户当前用户停止输入 |

### 服务端 → 客户端

| 事件 | 载荷 | 说明 |
|-------|---------|-------------|
| `login` | `{ numUsers }` | 成功执行 `add user` 后的确认 |
| `new message` | `{ username, message }` | 其他用户发送的聊天消息 |
| `user joined` | `{ username, numUsers }` | 新用户加入聊天室 |
| `user left` | `{ username, numUsers }` | 用户离开聊天室 |
| `typing` | `{ username }` | 其他用户正在输入 |
| `stop typing` | `{ username }` | 其他用户停止输入 |

## 运行测试

```bash
go test -v -race ./...
```
