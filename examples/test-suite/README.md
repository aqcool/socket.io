# 测试套件

## 快速开始

### 1. 启动示例服务端

请确认当前位于本示例目录，然后运行：

```bash
go run servers/cmd.go
```

---

### 2. 运行测试套件

在本示例目录运行：

```bash
go test -race -cover -covermode=atomic ./...
```

说明：

* `-race` 启用**数据竞争检测**
* `-cover` 生成**覆盖率报告**
* 并发测试推荐使用 `-covermode=atomic`

测试同时覆盖 **HTTP 长轮询**和 **WebSocket** 传输（参阅 `test-suite_test.go`）。

---

## 环境要求

* Go 1.26.0+

---

## 故障排查

* **测试失败**
  请确认本地服务端已经运行并监听预期端口，可使用以下命令验证：

```bash
curl http://localhost:3000
```
