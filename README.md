# TitanIM - ChatServer (核心长连接微服务)

![C++](https://img.shields.io/badge/C++-20-blue.svg) ![Boost](https://img.shields.io/badge/Boost.Asio-1.80+-orange.svg) ![RabbitMQ](https://img.shields.io/badge/RabbitMQ-AMQP-red.svg)

> **TitanIM 分布式即时通讯生态**的核心长连接承载节点。负责维持客户端的 SSL/TCP 长连接，处理 IM 核心业务逻辑（消息收发、状态同步），单机可承载 10 万+ QPS 的极高并发。

## 核心架构亮点
* **无栈协程底座**：全面采用 **C++20 Coroutines (`co_await`)** 重构网络 I/O 与数据库调用，彻底消除 Callback Hell，实现极致的并发上下文切换。
* **无锁多车道设计 (Partitioned Strands)**：根据 UserID 哈希将请求分发至 16 条独立的 Asio Strand 协程车道，严格保证单一用户的消息时序，消除线程竞态。
* **极限背压防御 (Backpressure)**：当底层落盘极度拥塞时，通过实时监控飞行队列 (`_inflightMsgs`) 实施 Fail-Fast 降级，免疫 OOM 雪崩。
* **跨线程状态机防撕裂**：采用 `std::atomic<bool>` CAS 硬件级并发锁配合 `boost::asio::dispatch` 归一化执行器，彻底根除高并发断线重连时的 SSL 状态机崩溃 (`0xC0000005`)。

## 技术栈
C++20 / Boost.Asio / Boost.Beast / OpenSSL / MySQL 8.0 / Redis++ / RabbitMQ (AMQP-CPP) / gRPC