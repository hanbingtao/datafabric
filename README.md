# datafabric

`datafabric` 是一个参考 Dremio 核心链路的新工程，使用 `Spring Boot` 独立运行，包名统一为 `com.datafabric`。

当前实现包含 4 组核心能力：

- `POST /api/v3/sql`：异步提交 SQL，生成 job
- `GET /api/v2/job/{jobId}` 与 `GET /api/v2/job/{jobId}/data`：查询 job 状态与结果
- `POST /api/v1/reflections`：创建 reflection，并把物化结果落到 `runtime/accelerator`
- `GET /api/v1/metadata/datasets`：读取元数据与列信息

## 启动

```bash
mvn spring-boot:run
```

或者：

```bash
mvn package
java -jar target/datafabric-0.0.1-SNAPSHOT.jar
```

服务默认监听 `19047`。

## 目录

- `runtime/results`：job 查询结果
- `runtime/accelerator`：reflection 物化数据
- `runtime/db`：内置 H2 数据库文件

## 示例

提交 SQL：

```bash
curl -X POST http://127.0.0.1:19047/api/v3/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql":"select * from SALES_FACT"}'
```

创建 reflection：

```bash
curl -X POST http://127.0.0.1:19047/api/v1/reflections \
  -H 'Content-Type: application/json' \
  -d '{"name":"sales-summary","sql":"select region, sum(amount) as total_amount from SALES_FACT group by region","refreshIntervalSeconds":300}'
```
