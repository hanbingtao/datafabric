# datafabric

`datafabric` 是一个参考 `dremio-oss` 交互模型实现的独立工程，使用 `Spring Boot` 承载后端能力，并在仓库内内置一套可直接访问的 Dremio 风格前端。

它的目标不是完整复刻 `dremio-oss`，而是优先打通最常用的产品链路：

- 资源树浏览
- Space / Source 基础管理
- SQL Runner 会话与脚本
- Job 查询与结果分页
- Reflection / Metadata 基础接口
- ClickHouse 数据源的真实库表浏览、字段读取和数据预览

## 当前能力

当前工程已经可以独立运行，并覆盖了以下几类接口与页面行为：

- `GET /`：直接返回内置前端页面
- `POST /api/v3/sql`：提交 SQL 并异步生成 job
- `GET /api/v2/job/{jobId}`、`GET /api/v2/job/{jobId}/data`：查询 job 状态和结果
- `GET/POST/PUT/DELETE /api/v3/catalog`：Space / Catalog 基础操作
- `GET /apiv2/sources`、`GET/PUT /apiv2/source/{sourceName}`：Source 基础操作
- `GET /apiv2/space/{spaceName}`、`GET /apiv2/home/{homeName}`：旧版资源详情页兼容
- `GET/POST/PUT/DELETE /apiv2/{source|space|home}/{root}/folder/...`：Folder 兼容接口
- `GET/PUT/DELETE /apiv2/{source|space|home}/{root}/folder_format/...`
- `GET/PUT/DELETE /apiv2/{source|space|home}/{root}/file_format/...`
- `POST /apiv2/{source|home}/{root}/{file_preview|folder_preview}/...`
- `GET /apiv2/datasets/summary/{path}`：数据集字段摘要
- `GET /apiv2/dataset/{path}/version/{version}/preview`：物理表 / 临时数据集预览
- `GET /apiv2/dataset/{path}/version/{version}/review`：兼容 review 流程
- `GET/PUT /apiv2/sql-runner/session`：SQL Runner 标签页会话
- `GET/POST/PUT/DELETE /apiv2/scripts`、`/api/v3/scripts`：SQL 脚本
- `GET /api/v1/reflections`、`POST /api/v1/reflections`：Reflection 基础管理
- `GET /api/v1/metadata/datasets`：元数据入口

## 与 dremio-oss 的关系

这个仓库参考了 `dremio-oss` 的接口契约、页面路由和前端调用方式，但当前定位是“兼容运行”和“功能验证”，不是一比一替代品。

当前已经对齐得比较好的部分：

- Dremio 风格 UI 可直接在本仓库中打开
- 旧版 `apiv2` 与新版 `api/v3` 的常用接口都提供了兼容层
- SQL Runner、Catalog、Job、Dataset Summary、Source 浏览等主流程可以走通
- ClickHouse Source 支持真实读取数据库、表、字段和预览数据

当前仍然是精简实现的部分：

- 权限模型、组织管理、协作能力仍是轻量 mock
- Acceleration / Reflection 只覆盖基础链路，不等同于 Dremio 完整引擎行为
- 并非所有 `dremio-oss` 页面或接口都已完全实现
- 单机模式下主要面向本地联调、演示和兼容验证

## 内置前端

仓库已经内置从 `dremio-oss` 同步过来的前端代码与构建产物，不再依赖外部前端目录：

- `frontend/dremio-ui`：主前端工程
- `frontend/ui-common`：共享能力与 API 类型
- `frontend/ui-lib`：组件库
- `frontend/vendor/*`：设计系统、图标、JS SDK、语法工具等依赖包

Spring Boot 默认直接托管 `frontend/dremio-ui/build` 下的静态文件，所以仓库本身可以独立启动和访问 UI。

## 运行要求

- Java 17+
- Maven 3.9+
- 本地如果要验证 ClickHouse，请准备一个可访问的 ClickHouse 实例

说明：

- 默认端口是 `59047`
- 默认使用本地文件 H2：`runtime/db/datafabric`
- 同一时间建议只启动一个 `datafabric` 进程，否则会竞争同一个 H2 文件锁

## 启动方式

开发模式：

```bash
mvn spring-boot:run
```

打包运行：

```bash
mvn package
java -jar target/datafabric-0.0.1-SNAPSHOT.jar
```

自定义端口：

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=59047
```

启动后可直接访问：

- 首页：`http://localhost:59047/`
- 健康检查：`http://localhost:59047/apiv2/server_status`
- SQL Runner 会话：`http://localhost:59047/apiv2/sql-runner/session`

## 目录说明

- `src/main/java/com/datafabric`：Spring Boot 后端实现
- `src/test/java/com/datafabric`：回归测试
- `frontend/`：内置前端代码与构建产物
- `runtime/results`：job 结果文件
- `runtime/accelerator`：reflection 物化结果
- `runtime/db`：本地 H2 文件
- `runtime/sources`：Source 配置持久化目录

## ClickHouse 支持

`datafabric` 当前对 ClickHouse 做了真实接入，而不是纯 mock。

已支持的能力：

- 读取 source 配置
- 浏览数据库列表
- 浏览数据库下的表列表
- 读取表字段信息
- 预览物理表数据
- 执行带 source 前缀的 SQL，例如 `SELECT * FROM "ch2"."test_db"."users" LIMIT 3`

Source 配置文件位于 `runtime/sources/*.json`。一个可工作的示例：

```json
{
  "id": "b7dd91bd-5998-44de-acaa-b5cb1fc60e9f",
  "name": "ch2",
  "type": "CLICKHOUSE",
  "description": "",
  "tag": "source-1775536681076",
  "createdAt": "2026-04-07T04:30:55.525791Z",
  "config": {
    "hostname": "localhost",
    "port": 8123,
    "username": "default",
    "password": "",
    "tls": false,
    "rootPath": "test_db"
  }
}
```

其中：

- `name` 是 UI 和 SQL 中使用的 source 名称
- `rootPath` 可选；配置后会直接把该库作为 source 根目录
- `port` 默认按 ClickHouse HTTP 端口 `8123` 使用

## 常用验证命令

健康检查：

```bash
curl http://127.0.0.1:59047/apiv2/server_status
```

查询 SQL：

```bash
curl -X POST http://127.0.0.1:59047/api/v3/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql":"select * from SALES_FACT"}'
```

查询 ClickHouse 表：

```bash
curl -X POST http://127.0.0.1:59047/api/v3/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT * FROM \"ch2\".\"test_db\".\"users\" LIMIT 3"}'
```

读取 Source 根目录：

```bash
curl http://127.0.0.1:59047/apiv2/source/ch2/
```

读取表字段摘要：

```bash
curl http://127.0.0.1:59047/apiv2/datasets/summary/ch2/test_db/users
```

读取物理表预览：

```bash
curl http://127.0.0.1:59047/apiv2/dataset/ch2/test_db/users/version/current/preview
```

## 测试

运行测试：

```bash
mvn test
```

当前仓库已经包含针对最近兼容修复的基础回归测试，主要覆盖：

- UI 首页回退行为
- job/data 分页参数校验
- 若干关键兼容接口的启动可用性

## 后续方向

如果继续沿着 `dremio-oss` 补齐，这个仓库下一步最值得完善的方向通常是：

- 更完整的 Source 表单与参数校验
- 更多 `apiv2` 旧接口兼容
- SQL Runner 与 Explore 页面的联动细节
- 更接近 Dremio 的 Catalog / Dataset 行为
- 更多真实数据源接入，而不只 ClickHouse
