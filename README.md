# 黑马点评后端

基于 Spring Boot、MyBatis-Plus、MySQL 和 Redis 的点评项目后端，包含用户登录、商铺查询、探店笔记、关注、优惠券和秒杀订单等功能。

## 项目内容

- `src/main/java/com/hmdp`：控制器、业务逻辑、实体、Mapper 和工具类。
- `src/main/resources/db/hmdp.sql`：数据库表结构和示例数据。
- `src/main/resources/mapper`：MyBatis XML 映射。
- `src/main/resources/*.lua`：Redis 秒杀和锁相关脚本。
- `src/test/java`：依赖 MySQL、Redis 的集成测试。

本仓库仅包含后端。配套前端、Nginx 程序、本机构建产物和 IDE 配置未纳入版本管理。

## 开发环境

当前 `pom.xml` 配置为 Java 21、Spring Boot 4.1.1、MyBatis-Plus 3.5.17 和 Redisson 4.7.0，使用 Maven 构建。运行前需要准备 MySQL 和支持 Streams 的 Redis。

## 本地运行

1. 在 MySQL 中创建 `hmdp` 数据库，并导入 `src/main/resources/db/hmdp.sql`。该脚本会重建同名表，请在专用开发数据库中执行。

2. 将配置示例复制为实际配置。PowerShell 命令：

   ```powershell
   Copy-Item src/main/resources/application.example.yaml src/main/resources/application.yaml
   ```

   按实际环境调整数据库地址、用户名和 Redis 配置。数据库密码可通过环境变量设置：

   ```powershell
   $env:DB_PASSWORD = '你的本地数据库密码'
   ```

   `application.yaml` 已被 Git 忽略，本机配置和密码不会随源码提交。修改通用配置时，请同步更新不含凭据的 `application.example.yaml`。

3. 启动本地 Redis。当前 `RedissonConfig.java` 单独配置了 `redis://127.0.0.1:6379`；如果更换 Redis 地址或启用认证，需要同步调整该配置与 Spring Redis 配置。

4. 首次运行秒杀订单功能前，创建 Redis Stream 消费者组：

   ```shell
   redis-cli XGROUP CREATE stream.orders g1 0 MKSTREAM
   ```

   若返回 `BUSYGROUP`，表示该组已存在。

5. 图片上传目录在 `src/main/java/com/hmdp/utils/SystemConstants.java` 的 `IMAGE_UPLOAD_DIR` 中定义。使用图片上传功能前，将其调整为本机图片目录，并配置静态文件服务。

6. 编译、打包并启动：

   ```shell
   mvn -DskipTests package
   java -jar target/hm-dianping-0.0.1-SNAPSHOT.jar
   ```

   默认服务端口为 `8081`。

## 测试

```shell
mvn test
```

现有测试会连接 MySQL、Redis，并写入缓存或商铺 GEO 数据。请先完成环境配置，并使用专用测试数据；仅检查编译和打包时使用 `mvn -DskipTests package`。
