# TLCN Lakehouse — Data Lakehouse cho quản lý thương mại điện tử đa kênh

Hạ tầng Data Lakehouse chạy hoàn toàn trên Docker cho tiểu luận chuyên ngành
*"Xây dựng Data Lakehouse cho bài toán quản lý thương mại điện tử đa kênh"*.
Dữ liệu (đơn hàng, tồn kho, hiệu quả sản phẩm, đánh giá từ Nhanh.vn / Shopee / TikTok)
đi theo kiến trúc Medallion **Landing → Bronze → Silver → Gold**, xử lý theo **batch**.

> Trạng thái: **Giai đoạn 0 — hạ tầng đã hoàn tất và kiểm thử**. Pipeline dữ liệu (Bronze/Silver/Gold) là bước tiếp theo.
> Tài liệu bàn giao chi tiết (cấu hình, tham số, ý nghĩa, sự cố đã gặp): [`docs/HUONG_DAN_BAN_GIAO.md`](docs/HUONG_DAN_BAN_GIAO.md)

## Kiến trúc

```
                ┌──────────────── Airflow (điều phối) ─────────────────┐
                ▼                                                      ▼
 Nguồn ──► MinIO bucket landing ──► Spark (Bronze → Silver) ──► Iceberg trên MinIO bucket lakehouse ──► Trino ──► Power BI / ML / ad-hoc
                                         │                             ▲                                   ▲
                                         └──── Iceberg REST Catalog ───┘                           dbt (Gold + kiểm tra chất lượng)

 Giám sát: Airflow ─statsd─► statsd-exporter ─┐
           MinIO, Spark ──────────────────────┴─► Prometheus ──► Grafana
```

## Dịch vụ

| Dịch vụ | Image | URL trên máy | Vai trò |
|---|---|---|---|
| MinIO (Silo) | `pgsty/silo` | http://localhost:9001 (console), :9000 (S3 API) | Object storage tương thích S3 |
| minio-init | `pgsty/silo` | — | Chạy 1 lần: tạo bucket `landing`, `lakehouse` |
| Iceberg REST Catalog | `tabulario/iceberg-rest:1.6.0` | http://localhost:8181/v1/config | "Mục lục" bảng Iceberg dùng chung cho Spark và Trino |
| Trino | `trinodb/trino:460` | http://localhost:8090 | SQL engine (catalog `lakehouse`) |
| Spark master / worker | `tlcn/spark` (tự build) | http://localhost:8081 / :8082, job UI :4040 | Xử lý batch, ghi bảng Iceberg |
| Postgres | `postgres:16` | — (nội bộ) | Metadata DB của Airflow |
| Airflow | `apache/airflow:2.10.3` | http://localhost:8088 | Lập lịch, điều phối DAG |
| Prometheus | `prom/prometheus:v2.54.1` | http://localhost:9090 | Thu thập số liệu vận hành |
| statsd-exporter | `prom/statsd-exporter:v0.27.1` | — (nội bộ) | Đổi số liệu StatsD của Airflow sang Prometheus |
| Grafana | `grafana/grafana:11.2.2` | http://localhost:3000 | Dashboard giám sát |
| dbt | `tlcn/dbt-trino` (tự build) | http://localhost:8085 (khi chạy `docs serve`) | Mô hình hoá Gold + data quality test (chạy theo lệnh) |

## Cấu trúc thư mục

```
lakehouse/
├── docker-compose.yml          # toàn bộ dịch vụ
├── .env.example                # mẫu tham số → copy thành .env
├── docker/
│   ├── spark/Dockerfile        # Spark + jar Iceberg/S3
│   └── dbt/Dockerfile          # dbt-core + dbt-trino + git
├── conf/
│   ├── spark/                  # spark-defaults.conf, metrics.properties
│   ├── trino/catalog/          # lakehouse.properties (catalog Iceberg REST)
│   ├── prometheus/             # prometheus.yml (danh sách nguồn số liệu)
│   └── grafana/provisioning/   # datasource Prometheus tự khai báo
├── airflow/
│   ├── dags/                   # smoke_test_lakehouse.py
│   ├── logs/                   # log task (không commit)
│   └── plugins/
├── jobs/                       # Spark job (Bronze, Silver) — sắp làm
├── dbt/                        # dbt project: sources, models, tests
└── docs/HUONG_DAN_BAN_GIAO.md  # tài liệu bàn giao chi tiết
```

## Chạy nhanh

**Yêu cầu:** Docker Desktop (WSL 2), khuyến nghị cấp ≥ 10 GB RAM cho WSL (`%UserProfile%\.wslconfig`).

```powershell
# 1. Tạo file tham số và đổi các giá trị <...>
copy .env.example .env

# 2. Build image tự viết (lần đầu mất vài phút)
docker compose build spark-master
docker compose build dbt

# 3. Khởi động toàn bộ dịch vụ
docker compose up -d
docker compose ps -a        # minio-init, airflow-init = Exited (0); còn lại Up/healthy
```

## Kiểm tra hệ thống

1. **Airflow** http://localhost:8088 → bật và chạy DAG `smoke_test_lakehouse`.
   9 task xanh = MinIO, catalog, Spark, Trino đều sẵn sàng và Trino ghi/đọc được bảng Iceberg.
2. **Prometheus** http://localhost:9090/targets → 6 job `UP`.
3. **dbt**:
   ```powershell
   docker compose run --rm dbt debug                              # All checks passed!
   docker compose run --rm dbt test --select source:airflow_smoke # kiểm tra chất lượng bảng smoke
   ```

## Lệnh hay dùng

| Việc | Lệnh |
|---|---|
| Trạng thái | `docker compose ps -a` |
| Log một dịch vụ | `docker compose logs -f trino` |
| Trino CLI | `docker compose exec trino trino --catalog lakehouse` |
| Spark SQL | `docker compose exec spark-master /opt/spark/bin/spark-sql` |
| Chạy dbt | `docker compose run --rm dbt <lệnh>` |
| Nạp lại cấu hình Prometheus | `Invoke-RestMethod -Method Post -Uri http://localhost:9090/-/reload` |
| Tắt (giữ dữ liệu) | `docker compose down` |
| Tắt và **xoá toàn bộ dữ liệu** | `docker compose down -v` |

## Lộ trình

- [x] Giai đoạn 0 — Hạ tầng: MinIO, Iceberg REST, Trino, Spark, Airflow, Prometheus/Grafana, dbt
- [ ] Giai đoạn 1 — Landing: đưa dataset giả lập vào bucket `landing` theo ngày
- [ ] Giai đoạn 2 — Bronze (Spark) + kiểm tra ① (dbt)
- [ ] Giai đoạn 3 — Silver (Spark MERGE, dedup, che PII) + kiểm tra ②
- [ ] Giai đoạn 4 — Gold (dbt-trino: dim/fact/mart) + kiểm tra ③
- [ ] Giai đoạn 5 — DAG hằng ngày/hằng tuần, backfill
- [ ] Giai đoạn 6 — Power BI + mô hình dự báo nhu cầu

## Dữ liệu

Dataset giả lập (≈205 MB, 2024-01 → 2026-11) nằm **ngoài repo** (`../dataset`), không đưa lên GitHub.
Thư mục dữ liệu mẫu thật đã che PII **tuyệt đối không commit**.
