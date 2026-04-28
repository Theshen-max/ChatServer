# 聊天核心服务器 (ChatServer)

**高性能分布式长连接消息中转中心**

![C++](https://img.shields.io/badge/C++-20-blue.svg) ![Boost](https://img.shields.io/badge/Boost.Asio-1.80+-orange.svg) ![RabbitMQ](https://img.shields.io/badge/RabbitMQ-AMQP-red.svg)

## 项目目标
承载海量用户的 TCP 长连接，负责实时消息的路由、转发与在线状态管理。它是整个 IM 系统中压力最大、逻辑最核心的节点。

## 技术栈
- **核心引擎**: Boost.Asio (基于 Strand 的无锁化并发设计)
- **通信协议**: 自定义二进制协议帧 + JSON/Protobuf 负载
- **安全性**: OpenSSL (TLS 1.3 链路加密)
- **可靠性组件**: RabbitMQ (异步落盘队列), Redis (热点会话缓存与未读计数)

## 核心特性
- **最终一致性模型**: 采用“入队即确认”策略，消息投递至 RabbitMQ 后立即回包，由后台消费者完成 MySQL持久化，显著提升吞吐。
- **跨服中转**: 通过 gRPC 联动 `RouteServer`，实现不同物理服务器间用户的透明通信。
- **高并发防护**:
    - **SendRateLimiter**: 限制单连接瞬时发包速率。
    - **Heartbeat**: 应用层双向心跳监测，及时清理僵尸连接。