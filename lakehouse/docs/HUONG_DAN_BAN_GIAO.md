# Hướng dẫn bàn giao — Hạ tầng TLCN Lakehouse (Giai đoạn 0)

> Tài liệu này ghi lại **toàn bộ hạ tầng đã dựng**: từng dịch vụ làm gì, cấu hình và tham số mang ý nghĩa gì, vì sao chọn như vậy, cách vận hành, kiểm tra, và các sự cố đã gặp.
> Người nhận bàn giao đọc theo thứ tự từ mục 1 tới mục 6 là dựng lại và vận hành được hệ thống.

**Phạm vi:** Giai đoạn 0, hạ tầng. Đã kiểm thử end-to-end bằng DAG `smoke_test_lakehouse` và dbt test.
**Chưa có:** pipeline dữ liệu thật (Landing → Bronze → Silver → Gold).

---

## Mục lục

1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Danh sách dịch vụ](#2-danh-sách-dịch-vụ)
3. [Cấu trúc thư mục](#3-cấu-trúc-thư-mục)
4. [Tham số môi trường (.env)](#4-tham-số-môi-trường-env)
5. [Cấu hình chi tiết từng dịch vụ](#5-cấu-hình-chi-tiết-từng-dịch-vụ)
6. [Vận hành](#6-vận-hành)
7. [Kiểm tra hệ thống](#7-kiểm-tra-hệ-thống)
8. [Quyết định thiết kế và lý do](#8-quyết-định-thiết-kế-và-lý-do)
9. [Sự cố đã gặp và cách xử lý](#9-sự-cố-đã-gặp-và-cách-xử-lý)
10. [Đánh giá độ ổn định, hạn chế và việc tiếp theo](#10-đánh-giá-độ-ổn-định-hạn-chế-và-việc-tiếp-theo)
11. [Phụ lục: kiến thức nền](#11-phụ-lục-kiến-thức-nền)

---

## 1. Tổng quan kiến trúc

```
                     ┌──────────────────── Airflow (điều phối, lập lịch) ────────────────────┐
                     │                                                                        │
  Nguồn (Nhanh,      ▼                                                                        ▼
  Shopee, Excel) ─► MinIO: bucket landing ─► Spark ─────────────────► MinIO: bucket lakehouse ◄── Trino ◄── Power BI / ML / ad-hoc
                     (file thô JSONL.gz)     (Bronze → Silver)        (bảng Iceberg: Parquet      ▲
                                               │                       + metadata)                │
                                               └──► Iceberg REST Catalog ◄────────────────────────┤
                                                    ("mục lục" dùng chung)                   dbt (Gold + data quality)

  Giám sát:  Airflow ──StatsD/UDP──► statsd-exporter ──┐
             MinIO, Spark (trang /metrics) ────────────┴──► Prometheus ──► Grafana
```

**Ý tưởng cốt lõi của lakehouse:** dữ liệu chỉ lưu **một nơi** (MinIO, định dạng bảng Iceberg). **Nhiều engine** (Spark, Trino, sau này dbt) cùng đọc và ghi qua **một catalog chung**, không copy dữ liệu qua lại. Đã kiểm chứng: bảng do Trino tạo thì Spark đọc được, dòng do Spark ghi thì Trino thấy ngay, cùng chung danh sách snapshot.

Tất cả container nằm chung một **mạng Docker** (`tlcn_default`, do compose tự tạo):
- **Trong mạng**, container gọi nhau bằng **tên service**, ví dụ `http://minio:9000`.
- **Từ máy Windows**, gọi qua `localhost:<port đã mở>`, ví dụ `http://localhost:9001`.

---

## 2. Danh sách dịch vụ

| Service | Image (phiên bản) | Port máy → container | Vai trò | Phụ thuộc | Lưu trữ |
|---|---|---|---|---|---|
| `minio` | `pgsty/silo:RELEASE.2026-09-16T00-00-00Z` | 9000→9000 (API S3), 9001→9001 (console) | Object storage tương thích S3 | — | volume `minio_data` |
| `minio-init` | (như trên) | — | Chạy 1 lần: tạo bucket `landing`, `lakehouse` | `minio` healthy | — |
| `iceberg-rest` | `tabulario/iceberg-rest:1.6.0` | 8181→8181 | Iceberg REST Catalog | `minio-init` hoàn tất | volume `iceberg_catalog` (SQLite) |
| `trino` | `trinodb/trino:460` | 8090→8080 | SQL engine, catalog `lakehouse` | `iceberg-rest` | — |
| `spark-master` | `tlcn/spark:3.5.3-iceberg1.6.1` (tự build) | 8081→8080 (UI), 4040→4040 (UI job) | Quản lý cụm Spark | `iceberg-rest` | — |
| `spark-worker` | (như trên) | 8082→8081 | Cung cấp CPU/RAM cho executor | `spark-master` | — |
| `postgres` | `postgres:16` | — | Metadata DB của Airflow | — | volume `pg_data` |
| `airflow-init` | `apache/airflow:2.10.3-python3.11` | — | Chạy 1 lần: tạo bảng metadata, tài khoản admin | `postgres` healthy | — |
| `airflow-webserver` | (như trên) | 8088→8080 | Giao diện web Airflow | `airflow-init` hoàn tất | — |
| `airflow-scheduler` | (như trên) | — | Lập lịch và chạy task (LocalExecutor) | `airflow-init` hoàn tất | — |
| `prometheus` | `prom/prometheus:v2.54.1` | 9090→9090 | Thu thập và lưu số liệu theo thời gian | — | volume `prom_data` |
| `statsd-exporter` | `prom/statsd-exporter:v0.27.1` | — | Nhận StatsD từ Airflow, mở `/metrics` cho Prometheus | — | — |
| `grafana` | `grafana/grafana:11.2.2` | 3000→3000 | Dashboard | `prometheus` | volume `grafana_data` |
| `dbt` | `tlcn/dbt-trino:1.9.3` (tự build) | 8085→8080 (chỉ khi `docs serve`) | Công cụ CLI: model Gold + test chất lượng | `trino` | thư mục `./dbt` |

**Thứ tự khởi động** (do `depends_on` quyết định):

```
minio ──healthy──► minio-init ──xong──► iceberg-rest ──► trino ──► (dbt khi gọi)
                                             └──────────► spark-master ──► spark-worker
postgres ──healthy──► airflow-init ──xong──► airflow-webserver, airflow-scheduler
prometheus ──► grafana        statsd-exporter (độc lập)
```

---

## 3. Cấu trúc thư mục

```
lakehouse/
├── docker-compose.yml               # định nghĩa 14 service
├── .env                             # tham số + mật khẩu thật (KHÔNG commit)
├── .env.example                     # mẫu .env (commit)
├── .gitignore
├── README.md
├── docker/
│   ├── spark/Dockerfile             # Spark 3.5.3 + 4 jar Iceberg/S3
│   └── dbt/Dockerfile               # python slim + git + dbt-core + dbt-trino
├── conf/
│   ├── spark/spark-defaults.conf    # Spark ↔ catalog ↔ MinIO
│   ├── spark/metrics.properties     # bật số liệu Prometheus cho Spark
│   ├── trino/catalog/lakehouse.properties   # catalog "lakehouse" của Trino
│   ├── prometheus/prometheus.yml    # các nguồn số liệu (scrape job)
│   └── grafana/provisioning/datasources/prometheus.yml
├── airflow/
│   ├── dags/smoke_test_lakehouse.py # DAG kiểm tra toàn hệ thống
│   ├── logs/                        # log task (tự sinh, không commit)
│   └── plugins/
├── jobs/                            # Spark job (Bronze/Silver) — giai đoạn sau
├── dbt/                             # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/staging/_sources.yml
│   └── tests/assert_ping_not_in_future.sql
└── docs/HUONG_DAN_BAN_GIAO.md       # tài liệu này
```

**Ba loại file và khi nào chúng có hiệu lực:**

| Loại | Ví dụ | Có hiệu lực khi | Sửa xong cần |
|---|---|---|---|
| Dockerfile | `docker/spark/Dockerfile` | build image | `docker compose build <service>` rồi `up -d` |
| Compose + `.env` | `docker-compose.yml` | tạo container | `docker compose up -d` (tự tạo lại container đổi cấu hình) |
| File cấu hình gắn vào (bind mount) | `spark-defaults.conf`, `lakehouse.properties`, `prometheus.yml` | tiến trình đọc file | restart service (Spark: job lần sau tự đọc; Prometheus: gọi `/-/reload`) |

---

## 4. Tham số môi trường (.env)

Compose tự đọc `.env` cùng thư mục và thay vào mọi chỗ `${TEN_BIEN}`. **Nguyên tắc:** không ghi cứng giá trị trong compose hay file cấu hình; mọi thứ đi qua `.env`. Phiên bản image luôn ghi cụ thể (không dùng `latest`) để chạy lại ra cùng kết quả.

### 4.1 Chung và MinIO

| Biến | Giá trị | Ý nghĩa |
|---|---|---|
| `COMPOSE_PROJECT_NAME` | `tlcn` | Tiền tố tên mạng và volume (`tlcn_default`, `tlcn_minio_data`…), tránh đụng project khác |
| `MINIO_IMAGE` | `pgsty/silo:RELEASE.2026-09-16T00-00-00Z` | Bản fork tương thích MinIO (xem mục 9.1) |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | (bí mật) | Tài khoản root MinIO. Mật khẩu **≥ 8 ký tự**. Cũng dùng làm `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` cho Spark, Trino, catalog, Airflow |
| `MINIO_API_PORT` | `9000` | Cổng API S3 trên máy |
| `MINIO_CONSOLE_PORT` | `9001` | Cổng giao diện web |
| `LANDING_BUCKET` | `landing` | Bucket chứa file thô từ nguồn |
| `LAKEHOUSE_BUCKET` | `lakehouse` | Bucket chứa bảng Iceberg (warehouse) |
| `AWS_REGION` | `us-east-1` | MinIO không có khái niệm region, nhưng thư viện AWS bắt buộc phải có giá trị |

### 4.2 Catalog và Trino

| Biến | Giá trị | Ý nghĩa |
|---|---|---|
| `ICEBERG_REST_IMAGE` | `tabulario/iceberg-rest:1.6.0` | Khớp dòng Iceberg 1.6 của Spark và Trino |
| `ICEBERG_REST_PORT` | `8181` | |
| `TRINO_IMAGE` | `trinodb/trino:460` | |
| `TRINO_PORT` | `8090` | Cổng **ngoài**. Trong container Trino dùng 8080; chọn 8090 để tránh trùng các dịch vụ hay dùng 8080 |
| `TRINO_MEM_LIMIT` | `3g` | Giới hạn RAM container. JVM của Trino tự lấy khoảng 80% |

### 4.3 Spark

| Biến | Giá trị | Ý nghĩa |
|---|---|---|
| `SPARK_BASE_IMAGE` | `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` | Spark 3.5.3, Scala 2.12, Java 17, có Python |
| `SPARK_IMAGE` | `tlcn/spark:3.5.3-iceberg1.6.1` | Tên image tự build |
| `ICEBERG_VERSION` | `1.6.1` | Sinh ra 2 jar: `iceberg-spark-runtime-3.5_2.12`, `iceberg-aws-bundle` |
| `HADOOP_AWS_VERSION` | `3.3.4` | **Bắt buộc** bằng bản Hadoop đi kèm Spark 3.5.3 |
| `AWS_SDK_BUNDLE_VERSION` | `1.12.262` | Bản `hadoop-aws` 3.3.4 yêu cầu |
| `SPARK_MASTER_UI_PORT` | `8081` | UI master (trong container 8080) |
| `SPARK_WORKER_UI_PORT` | `8082` | UI worker (trong container 8081) |
| `SPARK_APP_UI_PORT` | `4040` | UI của job đang chạy (driver) |
| `SPARK_WORKER_CORES` / `SPARK_WORKER_MEMORY` | `2` / `3g` | Tài nguyên worker **cho executor mượn** |
| `SPARK_WORKER_MEM_LIMIT` | `4g` | Giới hạn RAM cả container worker (phải lớn hơn 3g vì tiến trình worker cũng dùng RAM) |

**Chuỗi ràng buộc phiên bản:**
```
apache/spark:3.5.3-scala2.12
 ├─ Spark 3.5 + Scala 2.12 ─► iceberg-spark-runtime-3.5_2.12-1.6.1 ─► Iceberg 1.6 khớp REST catalog 1.6.0 và Trino 460
 └─ đi kèm Hadoop 3.3.4    ─► hadoop-aws-3.3.4 ─► aws-java-sdk-bundle-1.12.262
```
Chỉ có 2 lựa chọn tự do: **bản Spark** và **bản Iceberg**. Các số còn lại bị kéo theo. Muốn nâng cấp thì đi lại chuỗi này từ trên xuống.

### 4.4 Postgres và Airflow

| Biến | Ý nghĩa |
|---|---|
| `POSTGRES_IMAGE` = `postgres:16` | |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | Tài khoản và database cho metadata Airflow (`POSTGRES_DB=airflow`) |
| `AIRFLOW_IMAGE` = `apache/airflow:2.10.3-python3.11` | Image chính thức (chưa có Java/PySpark/dbt) |
| `AIRFLOW_UID` = `50000` | UID của user `airflow` trong image |
| `AIRFLOW_PORT` = `8088` | Cổng web |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | Tài khoản đăng nhập web, tạo bởi `airflow-init` |
| `AIRFLOW_SECRET_KEY` | Ký phiên đăng nhập web |
| `AIRFLOW_FERNET_KEY` | Mã hoá mật khẩu lưu trong Airflow (Connections, Variables). **Mất key = không giải mã được dữ liệu đã mã hoá** |

### 4.5 Giám sát và dbt

| Biến | Giá trị | Ý nghĩa |
|---|---|---|
| `PROMETHEUS_IMAGE`, `STATSD_IMAGE`, `GRAFANA_IMAGE` | v2.54.1, v0.27.1, 11.2.2 | |
| `PROMETHEUS_PORT` / `GRAFANA_PORT` | `9090` / `3000` | |
| `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` | (bí mật) | Đăng nhập Grafana |
| `DBT_BASE_IMAGE` | `python:3.11-slim-bookworm` | |
| `DBT_CORE_VERSION` / `DBT_TRINO_VERSION` | `1.9.8` / `1.9.3` | **Ghi rõ cả hai.** Nếu chỉ ghi adapter, pip tự kéo dbt-core bản mới nhất (đã thử: ra 1.11), lệch với adapter |
| `DBT_IMAGE` | `tlcn/dbt-trino:1.9.3` | |
| `DBT_DOCS_PORT` | `8085` | Trang tài liệu dbt |

---

## 5. Cấu hình chi tiết từng dịch vụ

### 5.1 MinIO và minio-init

```yaml
minio:
  command: server /data --console-address ":9001"
  environment: { MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_PROMETHEUS_AUTH_TYPE: public }
  ports: ["9000:9000", "9001:9001"]
  volumes: [minio_data:/data]
  healthcheck: { test: ["CMD","mc","ready","local"], interval: 5s, timeout: 5s, retries: 20, start_period: 10s }
```

| Khoá | Ý nghĩa |
|---|---|
| `command` | Lưu dữ liệu ở `/data`, mở console ở cổng 9001 |
| `MINIO_PROMETHEUS_AUTH_TYPE: public` | Cho Prometheus đọc số liệu không cần token (chấp nhận được trong mạng nội bộ) |
| `ports "A:B"` | `cổng_máy:cổng_container` |
| `minio_data:/data` | **Named volume**: xoá container thì dữ liệu vẫn còn. `docker compose down -v` mới xoá |
| `healthcheck` | `mc ready local` trả mã 0 nghĩa là sẵn sàng. `retries: 20`: thất bại 20 lần liên tiếp mới tính `unhealthy`. `start_period`: 10 giây đầu không tính lỗi |

`minio-init` dùng lại image Silo (có sẵn `mc`) để tạo bucket:

| Khoá | Ý nghĩa |
|---|---|
| `depends_on: minio: condition: service_healthy` | Chỉ chạy khi MinIO đã sẵn sàng thật, tránh race condition "connection refused" |
| `entrypoint: /bin/sh -c "mc alias set … && mc mb -p … && mc ls local"` | Thay lệnh mặc định bằng chuỗi lệnh. `&&`: lệnh trước thành công mới chạy lệnh sau |
| `http://minio:9000` | Container khác gọi sang MinIO nên dùng tên service, **không** dùng `localhost` |
| `$$MINIO_ROOT_USER` | Hai dấu `$`: compose không thay biến này, để shell **trong container** đọc |
| `mc mb -p` | Tạo bucket; `-p` = bucket có rồi thì bỏ qua → chạy lại bao nhiêu lần cũng an toàn |
| `restart: "no"` | Làm xong thì tắt hẳn. Trạng thái đúng là `Exited (0)` |

### 5.2 Iceberg REST Catalog

**Vai trò:** bảng Iceberg = file dữ liệu (Parquet) + file metadata, đều trên MinIO. Mỗi lần ghi tạo metadata **mới**. Catalog trả lời câu hỏi *"bảng X hiện tại dùng file metadata nào?"* và đổi con trỏ đó một cách **nguyên tử** khi có commit.

```yaml
iceberg-rest:
  depends_on: { minio-init: { condition: service_completed_successfully } }
  environment:
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION
    CATALOG_WAREHOUSE: s3://${LAKEHOUSE_BUCKET}/
    CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
    CATALOG_S3_ENDPOINT: http://minio:9000
    CATALOG_S3_PATH__STYLE__ACCESS: "true"
    CATALOG_URI: jdbc:sqlite:file:/home/iceberg/iceberg_catalog.db
  volumes: [iceberg_catalog:/home/iceberg]
```

**Quy tắc đặt tên biến:** image đọc biến `CATALOG_*`, bỏ tiền tố, đổi `__` thành `-` và `_` thành `.`, rồi viết thường. Ví dụ `CATALOG_S3_PATH__STYLE__ACCESS` → `s3.path-style-access`.

| Biến | Ý nghĩa |
|---|---|
| `service_completed_successfully` | Chờ `minio-init` thoát mã 0, tức bucket `lakehouse` đã có |
| `CATALOG_WAREHOUSE` | Thư mục gốc chứa mọi bảng. Bảng `ns.t` nằm dưới `s3://lakehouse/ns/t-<uuid>/` |
| `CATALOG_IO__IMPL` | Đọc/ghi file bằng S3FileIO |
| `CATALOG_S3_ENDPOINT` | Gửi lệnh S3 tới MinIO thay vì Amazon |
| `CATALOG_S3_PATH__STYLE__ACCESS` | Gọi `http://minio:9000/lakehouse/...` thay vì `http://lakehouse.minio:9000/...` (tên miền này không tồn tại) |
| `CATALOG_URI` | **Bắt buộc phải đặt.** Mặc định catalog lưu trong RAM → restart là mất sạch. Trỏ vào file SQLite trên volume để giữ lại |
| volume `iceberg_catalog:/home/iceberg` | Dùng **named volume** (không dùng thư mục ổ D:) để Docker chép đúng quyền thư mục của image → container ghi được file |

Log khởi động đúng có dòng `Creating catalog with properties: {... uri=jdbc:sqlite:file:/home/iceberg/...}` và `Started ServerConnector ... {0.0.0.0:8181}`.

### 5.3 Trino

```yaml
trino:
  environment: { AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, LAKEHOUSE_BUCKET }
  ports: ["8090:8080"]
  volumes: [./conf/trino/catalog:/etc/trino/catalog:ro]
  mem_limit: 3g
```

**`conf/trino/catalog/lakehouse.properties`:**

| Dòng | Ý nghĩa |
|---|---|
| *(tên file)* `lakehouse.properties` | Tên file = **tên catalog**. Truy vấn dạng `lakehouse.<schema>.<bảng>` |
| `connector.name=iceberg` | Catalog đọc/ghi bảng Iceberg |
| `iceberg.catalog.type=rest`, `iceberg.rest-catalog.uri=http://iceberg-rest:8181` | Dùng chung REST catalog với Spark |
| `iceberg.rest-catalog.warehouse=s3://${ENV:LAKEHOUSE_BUCKET}/` | `${ENV:X}` = đọc biến môi trường của container → file không chứa giá trị cứng |
| `fs.native-s3.enabled=true` | Bật thư viện S3 của Trino |
| `s3.endpoint`, `s3.region`, `s3.path-style-access=true` | Trỏ MinIO, kiểu đường dẫn |
| `s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}`, `s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}` | Tài khoản lấy từ biến môi trường |
| `iceberg.file-format=PARQUET`, `iceberg.compression-codec=ZSTD` | Định dạng file và nén (khớp diagram) |

Ghi chú:
- Mount đè `/etc/trino/catalog` nên các catalog mẫu của image (tpch…) bị ẩn, chỉ còn `lakehouse` và `system`.
- Image có sẵn healthcheck: trạng thái chuyển `(health: starting)` rồi `(healthy)` sau khoảng 20–40 giây; log có `SERVER STARTED`.
- Trino **không có catalog mặc định**: phải ghi đủ `lakehouse.schema.bảng`, hoặc `USE lakehouse.<schema>;`, hoặc mở CLI với `--catalog lakehouse`.
- Trino hiển thị thời gian theo **UTC**, Spark theo **giờ VN** (cấu hình `session.timeZone`). Cùng một thời điểm, chỉ khác cách hiển thị.
- Thư mục bảng có đuôi uuid (`don_hang-9100...`) do Trino mặc định `iceberg.unique-table-location=true`, để xoá rồi tạo lại cùng tên không bị lẫn file.

### 5.4 Spark

#### Dockerfile (`docker/spark/Dockerfile`)

| Dòng | Ý nghĩa |
|---|---|
| `ARG SPARK_BASE_IMAGE` (trước `FROM`) | Tham số dùng trong `FROM` phải khai báo **trước** `FROM` |
| `FROM ${SPARK_BASE_IMAGE}` | Image gốc |
| `ARG ICEBERG_VERSION …` (sau `FROM`) | `FROM` xoá các ARG trước nó nên phải khai báo lại |
| `USER root` | Image gốc chạy bằng user `spark`, không ghi được `/opt/spark/jars` |
| `ADD <url maven> /opt/spark/jars/` ×4 | Tải 4 jar. Spark tự nạp mọi jar trong thư mục này |
| `RUN chmod 644 /opt/spark/jars/*.jar` | File `ADD` từ URL có quyền `600`. Không chmod thì user `spark` không đọc được → `ClassNotFoundException` |
| `USER spark` | Trả về user thường (không chạy container bằng root) |

**Bốn jar:**

| Jar | Để làm gì |
|---|---|
| `iceberg-spark-runtime-3.5_2.12-1.6.1` | Spark hiểu bảng Iceberg: đọc/ghi, `MERGE INTO`, time travel, REST catalog |
| `iceberg-aws-bundle-1.6.1` | Iceberg đọc/ghi file S3 (S3FileIO, AWS SDK v2) → dùng cho **bảng** |
| `hadoop-aws-3.3.4` | Đường dẫn `s3a://` → dùng để đọc **file thô** ở bucket `landing` |
| `aws-java-sdk-bundle-1.12.262` | Thư viện AWS SDK v1 mà `hadoop-aws` cần |

Kiểm tra: `docker run --rm --entrypoint ls tlcn/spark:3.5.3-iceberg1.6.1 -l /opt/spark/jars` → đủ 4 jar, quyền `-rw-r--r--`.

#### Service

| Khoá | Ý nghĩa |
|---|---|
| `build.context` + `build.args` | Build từ `docker/spark`, truyền phiên bản từ `.env` |
| `image: ${SPARK_IMAGE}` | Đặt tên cho image vừa build. `spark-worker` chỉ khai `image` → dùng lại, không build lần hai |
| `hostname: spark-master` / `spark-worker` | **Quan trọng.** Driver báo địa chỉ cho executor theo tên máy; không đặt thì tên là mã ngẫu nhiên → executor không gọi ngược được → job treo |
| `command: spark-class org.apache.spark.deploy.master.Master` | Cùng một image, khác `command` → khác vai trò |
| `command: … Worker spark://spark-master:7077` | Worker đăng ký với master |
| `environment AWS_*` (cả hai) | **Executor** (chạy trong worker) mới là bên đọc/ghi file nên worker bắt buộc có. Executor kế thừa biến môi trường của worker |
| `SPARK_WORKER_CORES/MEMORY` | Tài nguyên worker cho mượn |
| Mount `spark-defaults.conf`, `metrics.properties` | Mọi nơi chạy Spark dùng chung cấu hình |
| Mount `./jobs:/opt/spark/jobs` | Nơi đặt Spark job |
| `mem_limit` (worker) | Giới hạn RAM container |

**Ba vai trò khi chạy job:** master phân việc; worker cho mượn tài nguyên và chạy **executor**; **driver** là chương trình điều khiển job (khi gõ `spark-sql` trong `spark-master` thì driver ở container master).

#### `conf/spark/spark-defaults.conf`

| Khoá | Giá trị | Ý nghĩa |
|---|---|---|
| `spark.master` | `spark://spark-master:7077` | Job gửi tới master (7077 là cổng nội bộ nhận job) |
| `spark.sql.extensions` | `…IcebergSparkSessionExtensions` | Bật cú pháp Iceberg: `MERGE INTO`, `CALL` |
| `spark.sql.catalog.lakehouse` | `org.apache.iceberg.spark.SparkCatalog` | Đăng ký catalog tên `lakehouse` (**trùng tên** với Trino) |
| `….type` / `….uri` | `rest` / `http://iceberg-rest:8181` | Dòng nối Spark vào catalog chung |
| `….io-impl`, `….s3.endpoint`, `….s3.path-style-access` | S3FileIO, MinIO, `true` | Đọc/ghi file bảng trên MinIO |
| `spark.sql.defaultCatalog` | `lakehouse` | Viết `schema.bảng` là đủ |
| `spark.hadoop.fs.s3a.endpoint` / `path.style.access` / `connection.ssl.enabled` / `impl` | MinIO / `true` / `false` / `S3AFileSystem` | Cấu hình cho đường dẫn `s3a://` (file thô) |
| `spark.hadoop.fs.s3a.aws.credentials.provider` | `EnvironmentVariableCredentialsProvider` | Lấy tài khoản từ biến `AWS_*` → không có mật khẩu trong file |
| `spark.sql.session.timeZone` | `Asia/Ho_Chi_Minh` | Hiển thị giờ VN |
| `spark.sql.shuffle.partitions` | `8` | Mặc định 200, quá nhiều cho dữ liệu khoảng 200 MB trên 2 nhân |
| `spark.executor.memory` | `2g` | Phải ≤ `SPARK_WORKER_MEMORY` (3g), nếu không job treo ở *"Initial job has not accepted any resources"* |

#### `conf/spark/metrics.properties`

| Dòng | Ý nghĩa |
|---|---|
| `*.sink.prometheusServlet.class=…PrometheusServlet` | Bật xuất số liệu dạng Prometheus cho mọi thành phần |
| `*.sink.prometheusServlet.path=/metrics/prometheus` | Đường dẫn mặc định (worker: `spark-worker:8081/metrics/prometheus`) |
| `master.sink.prometheusServlet.path=/metrics/master/prometheus` | Đường dẫn của master |
| `applications.sink.prometheusServlet.path=/metrics/applications/prometheus` | Số liệu tổng hợp các ứng dụng |

### 5.5 Postgres và Airflow

**Hai khối dùng chung (YAML anchor):**
- `x-…`: compose bỏ qua mọi mục bắt đầu bằng `x-`, dùng làm chỗ khai báo chung.
- `&tên`: đặt tên cho khối. `*tên`: dùng lại khối đó. `<<: *tên`: trộn khối vào service.
- **Bẫy hay gặp:** phép trộn `<<` chỉ hoạt động **một cấp**. Service nào tự viết `environment:` thì phải trộn lại `<<: *airflow-env` bên trong `environment` (như `airflow-init`), nếu không sẽ mất hết biến chung.

**`x-airflow-env`:**

| Biến | Ý nghĩa |
|---|---|
| `AIRFLOW__CORE__EXECUTOR: LocalExecutor` | Task chạy ngay trong container scheduler, song song được. Đủ cho một máy (Celery cần thêm Redis và worker) |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://user:pass@postgres:5432/db` |
| `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__WEBSERVER__SECRET_KEY` | Xem mục 4.4 |
| `AIRFLOW__CORE__LOAD_EXAMPLES: "false"` | Không nạp khoảng 50 DAG mẫu |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"` | DAG mới ở trạng thái tạm dừng, phải bật tay |
| `AIRFLOW__CORE__DEFAULT_TIMEZONE`, `…DEFAULT_UI_TIMEZONE` | Lịch và giờ hiển thị theo giờ VN |
| `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL: 30` | Quét thư mục DAG mỗi 30 giây (mặc định 5 phút) |
| `AWS_*`, `LANDING_BUCKET`, `LAKEHOUSE_BUCKET` | Để DAG gọi MinIO bằng boto3 |
| `MINIO_ENDPOINT`, `ICEBERG_REST_URI`, `TRINO_URI`, `SPARK_MASTER_UI` | Địa chỉ **nội bộ** các dịch vụ cho DAG (không ghi cứng trong code) |
| `AIRFLOW__METRICS__STATSD_ON/HOST/PORT/PREFIX` | Đẩy số liệu tới `statsd-exporter:9125` (UDP), tiền tố `airflow` |

Quy tắc tên biến: `AIRFLOW__<MỤC>__<KHOÁ>` tương ứng mục `[mục]`, khoá `khoá` trong `airflow.cfg`.

**`x-airflow-common`:** `image`, `user: "${AIRFLOW_UID}:0"` (user airflow, group root để ghi được log), `environment: *airflow-env`, mount `dags/`, `logs/`, `plugins/`.

**Các service:**

| Service | Điểm chính |
|---|---|
| `postgres` | healthcheck `pg_isready -U <user> -d <db>`; volume `pg_data` |
| `airflow-init` | `command: version` + `_AIRFLOW_DB_MIGRATE=true` (tạo/cập nhật bảng) + `_AIRFLOW_WWW_USER_CREATE=true` (tạo admin) → làm xong thì thoát mã 0 |
| `airflow-webserver` | `command: webserver`, cổng 8088, healthcheck `curl --fail http://localhost:8080/health`, `restart: unless-stopped` |
| `airflow-scheduler` | `command: scheduler`, `restart: unless-stopped` |

**Postgres chứa gì:** trạng thái DAG, lần chạy, task, và **XCom** (giá trị nhỏ truyền giữa task). **Không** chứa dữ liệu nghiệp vụ.

#### DAG `airflow/dags/smoke_test_lakehouse.py`

- Viết bằng **TaskFlow API** (`@dag`, `@task`). Chạy tay (`schedule=None`), `retries: 1` sau 30 giây.

| Task | Kiểm tra |
|---|---|
| `check_minio` | `GET minio:9000/minio/health/live` trả 200 |
| `check_buckets` | boto3 `list_buckets` có `landing` và `lakehouse` |
| `check_catalog` | `GET iceberg-rest:8181/v1/config` |
| `check_spark` | `GET spark-master:8080/json/` có `aliveworkers ≥ 1` |
| `check_trino` | `GET trino:8080/v1/info` không còn `starting`, và `SHOW CATALOGS` có `lakehouse` |
| `trino_write_iceberg` | Trino tạo `lakehouse.airflow_smoke.ping` (nếu chưa có), ghi 1 dòng `(run_id, current_timestamp)`, trả về số dòng |
| `verify_catalog_has_table` | Hỏi thẳng catalog `/v1/namespaces/airflow_smoke/tables/ping` → lấy `metadata-location` |
| `verify_files_on_minio` | Từ `metadata-location` suy ra thư mục bảng, đếm file `.parquet` và metadata trên MinIO |
| `report` | In tổng kết |

**Thứ tự:** 5 task `check_*` song song → `trino_write_iceberg` → `verify_catalog_has_table` → `verify_files_on_minio` → `report`.

**Kỹ thuật dùng trong DAG:**
- Truyền giá trị trả về giữa task (XCom) tự tạo quan hệ phụ thuộc. Với task không cần dữ liệu thì dùng `>>`.
- Gọi Trino qua **HTTP API** (POST `/v1/statement` rồi lặp GET `nextUri` tới khi hết; có `error` thì báo lỗi), vì image Airflow chính thức chưa có thư viện `trino`.
- Tham số `run_id=None`: Airflow tự truyền mã lần chạy vào.

### 5.6 Giám sát

**Mô hình:** Prometheus **kéo (pull)** số liệu từ trang `/metrics` của từng nguồn mỗi 15 giây. Airflow lại **đẩy (push)** số liệu theo StatsD qua UDP, nên cần `statsd-exporter` trung chuyển. UDP là kiểu "gửi rồi quên": statsd-exporter chết thì Airflow vẫn chạy, chỉ mất số liệu.

**Prometheus service:**

| Cờ | Ý nghĩa |
|---|---|
| `--config.file=/etc/prometheus/prometheus.yml` | File cấu hình (gắn từ `conf/prometheus`) |
| `--storage.tsdb.path=/prometheus` | Lưu trên volume `prom_data` |
| `--storage.tsdb.retention.time=15d` | Giữ 15 ngày |
| `--web.enable-lifecycle` | Nạp lại cấu hình bằng `POST /-/reload`, không cần restart |

**`conf/prometheus/prometheus.yml` (6 job):**

| job | target | metrics_path |
|---|---|---|
| `prometheus` | `localhost:9090` | `/metrics` |
| `minio-cluster` | `minio:9000` | `/minio/v2/metrics/cluster` |
| `minio-bucket` | `minio:9000` | `/minio/v2/metrics/bucket` |
| `spark-master` | `spark-master:8080` | `/metrics/master/prometheus` |
| `spark-worker` | `spark-worker:8081` | `/metrics/prometheus` |
| `airflow` | `statsd-exporter:9102` | `/metrics` |

Port trong target là port **trong container**.

**Grafana:**
- Biến `GF_SECURITY_ADMIN_USER/PASSWORD` theo quy tắc `GF_<MỤC>_<KHOÁ>`.
- Thư mục `provisioning` tự khai datasource `Prometheus` (`uid: prometheus`, `access: proxy`, `url: http://prometheus:9090`, mặc định).
- Volume `grafana_data` lưu dashboard.

**Dashboard gợi ý "TLCN Lakehouse":**

| Panel | PromQL | Kiểu |
|---|---|---|
| Dịch vụ sống/chết | `up` (legend `{{job}}`, mapping 1=UP, 0=DOWN) | Stat |
| Dung lượng bucket | `minio_bucket_usage_total_bytes` | Time series, đơn vị bytes(IEC) |
| Spark worker sống | `metrics_master_aliveWorkers_Value` | Stat |
| Scheduler còn sống | `rate(airflow_scheduler_heartbeat[1m])` | Time series |
| Task Airflow thành công | `increase(airflow_ti_successes[1h])` | Time series |

`rate()` = tốc độ tăng mỗi giây của **counter**. `increase()` = tăng thêm bao nhiêu trong khoảng thời gian. Mẹo: gõ tiền tố (`minio_`, `airflow_`) trong ô truy vấn để xem gợi ý tên số liệu thật, vì tên thay đổi theo phiên bản.

### 5.7 dbt

**Bản chất:** công cụ **dòng lệnh** (chạy xong thì thoát), không phải dịch vụ chạy liên tục. dbt dịch model và test thành SQL rồi gửi **Trino** chạy. Mỗi test là câu SQL **tìm dòng sai**: 0 dòng là PASS, có dòng là FAIL.

**Dockerfile (`docker/dbt/Dockerfile`):**

| Dòng | Ý nghĩa |
|---|---|
| `FROM python:3.11-slim-bookworm` | dbt là thư viện Python |
| `apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*` | `dbt debug` và `dbt deps` (cài package như `dbt_utils`) cần git. Gộp trong 1 `RUN` để xoá cache apt thật sự giảm dung lượng; đặt **trước** pip để tận dụng cache build |
| `pip install --no-cache-dir "dbt-core==…" "dbt-trino==…"` | Ghi rõ cả hai phiên bản |
| `WORKDIR /usr/app/dbt` | Nơi gắn project |
| `ENTRYPOINT ["dbt"]`, `CMD ["--version"]` | `docker compose run --rm dbt test` → chạy `dbt test` |

**Service:**

| Khoá | Ý nghĩa |
|---|---|
| `profiles: ["tools"]` | `docker compose up` bỏ qua. Gọi đích danh bằng `docker compose run` vẫn chạy |
| không có `container_name` | Mỗi lần `run` tạo container mới, tránh trùng tên |
| `DBT_TRINO_HOST: trino`, `DBT_TRINO_PORT: 8080` | Địa chỉ Trino **nội bộ** (khác `TRINO_PORT=8090` là cổng ngoài) |
| `DBT_PROFILES_DIR: /usr/app/dbt` | Tìm `profiles.yml` trong project (mặc định là `~/.dbt/`) |
| `./dbt:/usr/app/dbt` | Sửa project trên ổ D: là container thấy ngay |
| `ports 8085:8080` | Chỉ mở khi chạy kèm `--service-ports` (`docs serve`) |

**`dbt/dbt_project.yml`:** `profile: tlcn`; các thư mục `models/`, `tests/`, `macros/`; `target/` chứa SQL đã dịch (đọc để học); `+materialized: table` (model tạo thành bảng Iceberg, chưa dùng view vì chưa chắc catalog hỗ trợ); `data_tests: +severity: error` (test fail thì lệnh thoát mã khác 0, Airflow sẽ đỏ).

**`dbt/profiles.yml`:**

| Khoá | Ý nghĩa |
|---|---|
| `type: trino`, `method: none`, `http_scheme: http` | Kết nối Trino không xác thực |
| `host: "{{ env_var('DBT_TRINO_HOST') }}"` | Jinja đọc biến môi trường |
| `port: "{{ env_var('DBT_TRINO_PORT') \| as_number }}"` | Biến môi trường là chuỗi, cổng phải là số |
| `database: lakehouse` | Trong dbt-trino, `database` = **catalog** Trino |
| `schema: dbt_dev` | Model tạo ra nằm ở `lakehouse.dbt_dev` (dbt tự tạo schema) |
| `threads: 4` | Chạy song song tối đa 4 node |

**Đã có:**
- `models/staging/_sources.yml`: khai báo nguồn `airflow_smoke.ping` với test `not_null` + `unique` (`run_id`), `not_null` (`checked_at`) và **freshness** (`loaded_at_field: checked_at`; quá 1 ngày thì cảnh báo, quá 3 ngày thì lỗi).
- `tests/assert_ping_not_in_future.sql`: singular test, không có dòng nào có `checked_at` vượt hiện tại quá 5 phút.

**Đã kiểm tra:** `dbt debug` (All checks passed), `dbt test --select source:airflow_smoke` (PASS).

---

## 6. Vận hành

### 6.1 Dựng mới từ đầu

```powershell
copy .env.example .env                    # rồi đổi các giá trị <...>
docker compose build spark-master         # image Spark (lần đầu tải ~330 MB jar)
docker compose build dbt                  # image dbt
docker compose up -d
docker compose ps -a
```
Trạng thái đúng: `minio`, `postgres`, `airflow-webserver`, `trino` là `(healthy)`; `minio-init`, `airflow-init` là `Exited (0)`; còn lại `Up`.

Chuẩn bị máy Windows: Docker Desktop (WSL 2), file `%UserProfile%\.wslconfig`:
```ini
[wsl2]
memory=10GB
processors=4
```
rồi chạy `wsl --shutdown`.

### 6.2 Lệnh hằng ngày

| Việc | Lệnh |
|---|---|
| Khởi động / áp dụng thay đổi compose | `docker compose up -d` |
| Trạng thái (kể cả container đã thoát) | `docker compose ps -a` |
| Log | `docker compose logs -f <service>` |
| Chạy lệnh trong container | `docker compose exec <service> <lệnh>` |
| Trino CLI | `docker compose exec trino trino --catalog lakehouse` |
| Spark SQL | `docker compose exec spark-master /opt/spark/bin/spark-sql` |
| dbt | `docker compose run --rm dbt <debug\|test\|build\|source freshness\|docs generate>` |
| Liệt kê DAG / lỗi import | `docker compose exec airflow-scheduler airflow dags list` / `… dags list-import-errors` |
| Chạy DAG bằng lệnh | `docker compose exec airflow-scheduler airflow dags trigger smoke_test_lakehouse` |
| Nạp lại Prometheus | `Invoke-RestMethod -Method Post -Uri http://localhost:9090/-/reload` |
| Build lại image | `docker compose build --no-cache <service>` |
| Tắt (giữ dữ liệu) | `docker compose down` |
| Tắt và **xoá mọi dữ liệu** (MinIO, catalog, Postgres, Prometheus, Grafana) | `docker compose down -v` |

**PowerShell và Git Bash:**

| PowerShell | Git Bash |
|---|---|
| `… \| Select-String "a\|b"` | `… \| grep -E "a\|b"` |
| `Invoke-RestMethod <url>` | `curl -s <url>` |
| `Invoke-RestMethod -Method Post -Uri <url> -ContentType "application/json" -Body '{...}'` | `curl -s -X POST <url> -H "Content-Type: application/json" -d '{...}'` |
| — | lệnh tương tác (`trino`, `spark-sql`) báo lỗi TTY thì thêm `winpty` phía trước |

### 6.3 Địa chỉ truy cập

| Dịch vụ | URL |
|---|---|
| MinIO console | http://localhost:9001 |
| Iceberg REST | http://localhost:8181/v1/config, /v1/namespaces |
| Trino UI | http://localhost:8090 |
| Spark master / worker / job | http://localhost:8081 / 8082 / 4040 |
| Airflow | http://localhost:8088 |
| Prometheus | http://localhost:9090 (/targets) |
| Grafana | http://localhost:3000 |
| dbt docs | http://localhost:8085 (khi chạy `docs serve`) |

---

## 7. Kiểm tra hệ thống

| # | Kiểm tra | Cách | Kết quả đúng |
|---|---|---|---|
| 1 | Container | `docker compose ps -a` | như mục 6.1 |
| 2 | Bucket | MinIO console | có `landing`, `lakehouse` |
| 3 | Catalog giữ dữ liệu | tạo namespace → `docker compose restart iceberg-rest` → liệt kê | namespace vẫn còn |
| 4 | Trino ↔ Spark dùng chung catalog | Trino tạo bảng; Spark `SELECT`; Spark `INSERT`; Trino `SELECT` + `"$snapshots"` | thấy dữ liệu hai chiều, snapshot trùng ID |
| 5 | Toàn hệ thống | DAG `smoke_test_lakehouse` | 9 task xanh (khoảng 13 giây) |
| 6 | Giám sát | http://localhost:9090/targets | 6 job UP |
| 7 | dbt | `dbt debug`, `dbt test --select source:airflow_smoke` | All checks passed; PASS |

**Kiểm tra khả năng phát hiện lỗi:** `docker compose stop spark-worker`, rồi chạy DAG: `check_spark` đỏ, các task sau `upstream_failed`; Grafana panel `up` chuyển DOWN. `docker compose start spark-worker`, rồi **Clear** task để chạy lại.

**Khi task DAG đỏ:**

| Task | Nguyên nhân thường gặp | Xem |
|---|---|---|
| `check_minio` / `check_buckets` | MinIO chết / thiếu bucket | `logs minio-init` |
| `check_catalog` | catalog không chạy | `logs iceberg-rest` |
| `check_spark` | worker chưa đăng ký | `logs spark-worker` |
| `check_trino` | Trino đang khởi động / thiếu file catalog | đợi 30 giây rồi Clear; `logs trino` |
| `trino_write_iceberg` | Trino không ghi được MinIO | biến `AWS_*`, cấu hình `s3.*` |
| `verify_files_on_minio`, `NoCredentialsError` | thiếu biến môi trường của Airflow | `docker compose exec airflow-scheduler env` |

---

## 8. Quyết định thiết kế và lý do

| Quyết định | Lý do |
|---|---|
| Kiến trúc **lakehouse** (Iceberg trên object storage) | Một bản dữ liệu, nhiều engine; có ACID, time travel, MERGE như data warehouse nhưng lưu rẻ như data lake |
| **Một REST catalog dùng chung** | Spark (xử lý) và Trino (truy vấn, dbt) thấy cùng một bảng ngay sau commit; không cần đồng bộ |
| Catalog lưu **SQLite** trên volume | Đơn giản cho demo một máy. Catalog cần thao tác đổi con trỏ nguyên tử, việc mà database làm tốt (xem mục 11) |
| Metadata Airflow ở **Postgres** | Nhiều tiến trình cập nhật trạng thái nhỏ liên tục, cần giao dịch và khoá |
| Dữ liệu bảng ở **MinIO** | Lớn, ghi một lần, đọc song song → object storage phù hợp |
| **Ghi rõ phiên bản** mọi image và thư viện | Chạy lại ra cùng kết quả; chuỗi tương thích Spark/Hadoop/Iceberg |
| **Named volume** cho dữ liệu, **bind mount** cho cấu hình | Dữ liệu bền, đúng quyền; cấu hình sửa trên ổ D: là có hiệu lực |
| `.env` chứa mọi tham số, file cấu hình đọc biến môi trường (`${ENV:…}`, `env_var()`, `EnvironmentVariableCredentialsProvider`) | Không lộ mật khẩu trong repo; đổi môi trường chỉ sửa `.env` |
| **Healthcheck + `depends_on` có điều kiện** | Tránh race condition khi khởi động |
| **LocalExecutor** | Đủ cho một máy, không cần thêm Redis/Celery |
| dbt là service `profiles: ["tools"]` | dbt là CLI, chạy theo lệnh, không cần chạy nền |
| `dbt build` thay vì `run` rồi mới `test` | Chạy theo thứ tự phụ thuộc; test nguồn fail thì model phía sau bị **SKIP** → chặn dữ liệu xấu lan xuống |
| Giám sát pull (Prometheus) + push qua StatsD (Airflow) | Theo cơ chế mỗi công cụ hỗ trợ; UDP không làm hệ thống chính phụ thuộc vào giám sát |
| Partition bảng theo ngày; tránh partition quá nhỏ | Partition pruning; tránh vấn đề file nhỏ |

---

## 9. Sự cố đã gặp và cách xử lý

| # | Hiện tượng | Nguyên nhân | Xử lý |
|---|---|---|---|
| 9.1 | `pull access denied for minio/minio` | MinIO **gỡ image khỏi Docker Hub (11/09/2026)**, quay.io chặn tải ẩn danh (24/09/2026) | Dùng bản fork tương thích **`pgsty/silo`** (cùng lệnh, biến, cổng; có sẵn `mc`). Nên ghi vào phần "rủi ro phụ thuộc phần mềm" của báo cáo |
| 9.2 | Compose báo lỗi hoặc service không nhận healthcheck | **Thụt lề YAML sai**: `healthcheck` và `minio-init` bị đặt ở cấp ngoài cùng | Healthcheck thụt 4 dấu cách (thuộc tính của service); service mới thụt 2 dấu cách dưới `services:`; `volumes:` cấp cao để cuối file. Chỉ dùng dấu cách. Luôn chạy `docker compose config --quiet` |
| 9.3 | Tưởng catalog mất dữ liệu sau `DELETE` | `DELETE` thành công trả **HTTP 204 No Content** nên PowerShell không in gì | Không in gì là thành công; lỗi thật sẽ hiện chữ đỏ |
| 9.4 | Trino: `Catalog must be specified when session catalog is not set` | Trino không có catalog mặc định | Ghi `lakehouse.schema.bảng`, hoặc `USE lakehouse.<schema>`, hoặc `--catalog lakehouse` |
| 9.5 | Time travel ra 0 dòng | Trino tạo **snapshot rỗng lúc CREATE TABLE** | Dùng snapshot **sau** lần insert cần xem; hoặc `FOR TIMESTAMP AS OF` |
| 9.6 | `Select-String: command not found` | Đang ở Git Bash, lệnh đó chỉ có trong PowerShell | Dùng `grep -E` (xem mục 6.2) |
| 9.7 | `FutureWarning [core/sql_alchemy_conn] deprecated` | Code nội bộ Airflow đọc tên cũ (compose đã dùng `AIRFLOW__DATABASE__…`) | Bỏ qua |
| 9.8 | `dbt debug`: `git [ERROR]` | Image `python:slim` không có git | Cài git trong Dockerfile dbt |
| 9.9 | Cảnh báo `NativeCodeLoader`, `HiveConf`, `ObjectStore` khi mở `spark-sql` | Thiếu thư viện native của Hadoop; spark-sql khởi tạo Hive metastore nội bộ cho catalog cũ `spark_catalog` | Vô hại, không dùng catalog đó |
| 9.10 | dbt cảnh báo SSL certificate validation | Thay đổi mặc định trong tương lai của dbt | Kết nối `http` nội bộ, có thể đặt `flags: require_certificate_validation: true` để tắt cảnh báo |

---

## 10. Đánh giá độ ổn định, hạn chế và việc tiếp theo

### 10.1 Đánh giá hiện tại

**Ổn định cho giai đoạn phát triển:**
- Compose hợp lệ, đủ 14 service, không thiếu biến môi trường.
- Đã xác nhận đạt các kiểm tra 1–5 và 7 ở mục 7.
- Kiểm tra 6 (giám sát): Prometheus/Grafana đã cấu hình đủ 6 nguồn, nhưng chưa ghi nhận ảnh trang `/targets` và dashboard. Cần chạy lại một lần để xác nhận và lưu ảnh cho báo cáo.
- Dữ liệu bền qua restart nhờ named volume.

**Cần chú ý:**

| Vấn đề | Ảnh hưởng | Đề xuất |
|---|---|---|
| Chỉ Airflow có `restart: unless-stopped` | Khởi động lại Docker Desktop hoặc máy: Airflow tự lên nhưng MinIO, Trino, Spark… thì không → DAG lỗi | Thêm `restart: unless-stopped` cho các service chạy lâu dài (minio, iceberg-rest, trino, spark-*, postgres, prometheus, statsd-exporter, grafana); **không** thêm cho `minio-init`, `airflow-init`, `dbt` |
| Catalog dùng SQLite | Chịu kém khi nhiều bên ghi đồng thời | Production: JdbcCatalog trên Postgres |
| `iceberg-rest` không có healthcheck | Trino/Spark có thể khởi động trước khi catalog sẵn sàng (thực tế chưa gặp vì hai engine kết nối catalog khi cần, không phải lúc khởi động) | Thêm healthcheck nếu gặp lỗi khởi động |
| Không có xác thực giữa các dịch vụ (Trino, metrics MinIO public) | Chỉ an toàn trong mạng nội bộ máy cá nhân | Production: bật xác thực, TLS |
| Dùng tài khoản root MinIO cho mọi dịch vụ | Quyền quá rộng | Tạo user/policy riêng từng dịch vụ |
| Log Airflow tích luỹ trong `airflow/logs` | Tốn dung lượng theo thời gian | DAG dọn log định kỳ |
| Image Airflow chưa có Java/PySpark/dbt | Airflow chưa gửi được Spark job hay chạy dbt | Build image Airflow riêng (giai đoạn Bronze) |

### 10.2 Việc tiếp theo

| Giai đoạn | Nội dung |
|---|---|
| dbt (tiếp) | 10.5 singular test đã có file; 10.6 model `stg_smoke_ping` + `dbt build`; 10.7 freshness; 10.8 làm hỏng dữ liệu + `--store-failures`; 10.9 docs/lineage. Dự định làm cùng dữ liệu thật |
| 1. Landing | Script "replay" đẩy `dataset/` lên bucket `landing` theo từng `dt` |
| 2. Bronze | Spark đọc `s3a://landing/...` → append bảng Iceberg, thêm `_ingested_at`/`_batch_id`, partition `dt`; dbt kiểm tra ① |
| 3. Silver | Spark parse/flatten, dedup (orders theo `updatedAt`, performance theo batch mới nhất, reviews theo `mtime`), tách combo, che PII, MERGE; dbt kiểm tra ② |
| 4. Gold | dbt-trino: dim/fact/mart + test ③ |
| 5. Điều phối | Image Airflow riêng; DAG `nhanh_daily`, `shopee_weekly`, `iceberg_maintenance`; backfill |
| 6. Sử dụng | Power BI qua Trino; mô hình dự báo nhu cầu |
| Mở rộng | Cảnh báo Telegram trong Grafana (`up == 0` kéo dài 2 phút) |

---

## 11. Phụ lục: kiến thức nền

**Iceberg lưu gì trên MinIO** (ví dụ bảng `thunghiem.don_hang`):
```
lakehouse/thunghiem/don_hang-<uuid>/
├── data/ngay=2026-11-01/xxx.parquet      ← file dữ liệu, mỗi partition một thư mục
├── data/ngay=2026-11-02/...
└── metadata/
    ├── 0000x-....metadata.json           ← schema, partition spec, danh sách snapshot
    ├── snap-....avro                     ← danh sách manifest của một snapshot
    └── ....-m0.avro                      ← manifest: file nào thuộc partition nào, thống kê cột
```

**Partition:**
- Khai báo một lần lúc tạo bảng (`WITH (partitioning = ARRAY['ngay'])` trong Trino, `PARTITIONED BY (days(ts))` trong Spark).
- Khi ghi, engine tự tính giá trị partition từng dòng và ghi ra file riêng.
- Iceberg biết file thuộc partition nào nhờ **manifest**, không dựa vào tên thư mục.
- Khi truy vấn có `WHERE` theo cột partition thì chỉ đọc file liên quan (**partition pruning**).
- Có **hidden partitioning**: `day()`, `month()`, `bucket()`.
- Xem bằng `"bảng$partitions"`, `"bảng$files"`.

**Snapshot và time travel:**
- Mỗi lần ghi tạo snapshot mới; file cũ **không bị sửa**. Nhờ đó đọc được phiên bản cũ, và người đọc không bao giờ thấy dữ liệu ghi dở.
- Trino: `FOR VERSION AS OF <id>`, `FOR TIMESTAMP AS OF TIMESTAMP '…'`. Spark: bảng metadata `bảng.snapshots`.
- Cái giá phải trả là nhiều file nhỏ → bảo trì định kỳ: `ALTER TABLE … EXECUTE optimize` (gộp file, tạo snapshot `replace`), expire snapshot, xoá file mồ côi.

**Ai lưu gì:**

| | Airflow metadata | Iceberg catalog | Dữ liệu bảng |
|---|---|---|---|
| Nội dung | trạng thái DAG/task, XCom | con trỏ "bảng → metadata hiện tại" | Parquet + metadata/manifest |
| Kích thước | nhỏ | rất nhỏ | lớn |
| Kiểu ghi | sửa liên tục | đổi con trỏ nguyên tử mỗi commit | ghi file mới, không sửa |
| Nơi lưu | Postgres | SQLite (database) | MinIO |

**`localhost` và tên service:** trình duyệt và PowerShell chạy trên Windows nên gọi `localhost:<port mở>`. Container gọi nhau bằng tên service trong mạng Docker. Trong một container, `localhost` là **chính nó**.

**HTTP method khi gọi API:** `GET` để xem, `POST` để tạo, `DELETE` để xoá. Trong PowerShell dùng `Invoke-RestMethod`; trong Git Bash dùng `curl`.
