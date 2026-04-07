# DataFabric Full Migration Plan

`datafabric` 的目标不是继续做一个轻量 demo，而是逐步承接 Dremio OSS 的完整前后端能力。

当前已经完成的一步：

- 前端入口切换为直接承载 Dremio 现成 UI build
- `datafabric` 通过 Spring Boot 提供 Dremio 的 `index.html` 与 `static/*` 资源

这意味着前端“同代码”已经开始成立，后续核心工作集中在后端 API 与执行控制面的迁移。

## 迁移原则

1. 前端保持 Dremio 同代码
2. 后端使用 `Spring Boot` 微服务化承接
3. 先兼容已有 Dremio UI 需要的 API 面，再深入执行内核替换
4. 领域边界清晰，避免把原本单体 DAC 再平移成另一个 Spring 单体

## 建议的微服务拆分

### 1. `datafabric-gateway`

职责：

- 前端静态资源分发
- 登录鉴权入口
- 网关路由
- SSE/WebSocket 转发
- 聚合型前端接口

主要承接 Dremio 入口：

- `dac/backend`
- UI 首页与页面入口

### 2. `datafabric-catalog-service`

职责：

- source / space / folder / dataset 元数据管理
- dataset summary / dataset graph / schema 浏览
- source 配置、连接和健康检查

主要参考：

- `/Users/hanbingtao/data/dremio-oss/services/namespace`
- `/Users/hanbingtao/data/dremio-oss/sabot/kernel/src/main/java/com/dremio/exec/catalog`
- `/Users/hanbingtao/data/dremio-oss/dac/backend/src/main/java/com/dremio/dac/explore`

### 3. `datafabric-job-service`

职责：

- SQL 提交
- job 生命周期管理
- job 状态、summary、details、profile
- result 落盘与分页读取

主要参考：

- `/Users/hanbingtao/data/dremio-oss/services/jobs`
- `/Users/hanbingtao/data/dremio-oss/dac/backend/src/main/java/com/dremio/dac/api/SQLResource.java`
- `/Users/hanbingtao/data/dremio-oss/dac/backend/src/main/java/com/dremio/dac/resource/JobResource.java`

### 4. `datafabric-reflection-service`

职责：

- reflection goal 管理
- materialization / refresh / dependency 管理
- 定时刷新与失败重试

主要参考：

- `/Users/hanbingtao/data/dremio-oss/services/accelerator`

### 5. `datafabric-execution-service`

职责：

- SQL 解析与执行适配
- 执行引擎编排
- 本地执行 / 外部计算引擎桥接

主要参考：

- `/Users/hanbingtao/data/dremio-oss/sabot/kernel`

说明：

这一块是最重的，不建议一开始就试图 1:1 复制 Sabot 内核。第一阶段可先用 JDBC/H2/Calcite 兼容层承接部分查询能力，再逐步替换。

### 6. `datafabric-admin-service`

职责：

- 系统参数
- 节点信息
- 支持项、feature flag
- 统计页所需接口

主要参考：

- `/Users/hanbingtao/data/dremio-oss/dac/backend/src/main/java/com/dremio/dac/admin`
- `/Users/hanbingtao/data/dremio-oss/services/options`

### 7. `datafabric-identity-service`

职责：

- 用户、角色、权限
- token / session
- 登录、注销、用户资料

主要参考：

- `/Users/hanbingtao/data/dremio-oss/dac/backend/src/main/java/com/dremio/dac/resource/LoginResource.java`
- `dac/backend` 中认证和用户相关资源

## 迁移阶段

### 阶段 A：前端接管期

目标：

- `datafabric` 直接承载 Dremio 前端
- 补齐前端首屏依赖的最小 API

优先补齐：

- 登录相关接口
- bootstrap/config/supportFlags
- 首页统计接口
- jobs 列表和详情接口
- dataset 列表和 summary 接口

### 阶段 B：核心工作流恢复期

目标：

- Explore 页面基本可用
- SQL 提交、结果读取、job 详情可用
- Reflection 页面可用

优先补齐：

- `apiv2` / `api/v3` 查询接口
- dataset version / run / preview
- reflection CRUD / refresh

### 阶段 C：控制平面迁移期

目标：

- Catalog / Namespace / Option / Admin 能力稳定迁移
- Source 管理、元数据刷新、系统配置逐步恢复

### 阶段 D：执行内核替换期

目标：

- 从“兼容层执行”逐步走向更完整的执行控制面
- 视范围决定是否继续深度替代 Sabot/Arrow/Flight 体系

## 当前事实

当前 `datafabric` 还没有完成“全部后端功能迁移”。

已经完成的是：

- 用 Spring Boot 承载 Dremio 同一套前端静态资源
- 保留一套轻量后端 API，支持 job / metadata / reflection 演示能力

下一步最合理的工程动作，不是空泛地说“全部迁过来”，而是：

1. 先把 Dremio UI 首屏需要的 API 补齐
2. 让登录、首页、Jobs、Catalog、Explore 逐页恢复
3. 再把内部服务拆分成真正的微服务

## 建议的下一批任务

1. 实现 `bootstrap/login/supportFlags` 兼容接口，先让 Dremio 前端首屏跑起来
2. 建立 `datafabric-gateway` 与 `datafabric-job-service` 的真正分模块结构
3. 逐页对照 Dremio UI 的网络请求清单补 API
