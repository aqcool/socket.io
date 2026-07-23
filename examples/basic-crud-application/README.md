# 基础 CRUD 应用

使用 Socket.IO Go 实现的实时 CRUD（创建、读取、更新、删除）应用。

## 特性

- 创建、读取、更新和删除待办事项
- 向所有已连接客户端实时广播改动
- 使用确认回调（ack）确认操作结果
- 线程安全的内存存储

## 运行方式

```bash
go run main.go
```

服务端默认监听 `http://localhost:3000`。可通过 `PORT` 环境变量指定其他端口。

## 事件

### 客户端 → 服务端

| 事件 | 载荷 | 确认返回 | 说明 |
|-------|---------|-----|-------------|
| `todo:create` | `{ title }` | `TodoItem` | 创建事项 |
| `todo:read` | — | `[]TodoItem` | 列出所有事项 |
| `todo:update` | `{ id, title, completed }` | `TodoItem` | 更新现有事项 |
| `todo:delete` | `{ id }` | `{ id }` | 删除事项 |

### 服务端 → 客户端

| 事件 | 载荷 | 说明 |
|-------|---------|-------------|
| `todo:list` | `[]TodoItem` | 连接时发送完整事项列表 |
| `todo:created` | `TodoItem` | 已创建新事项 |
| `todo:updated` | `TodoItem` | 已更新事项 |
| `todo:deleted` | `{ id }` | 已删除事项 |

### TodoItem

```json
{
  "id": 1,
  "title": "Buy groceries",
  "completed": false
}
```

## 运行测试

```bash
go test -v -race ./...
```
