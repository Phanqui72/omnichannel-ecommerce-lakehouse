"""
DAG smoke_test_lakehouse — kiểm tra phần lõi lakehouse (Bước 2–8) chạy được cùng nhau.

Nhóm 1 (song song): MinIO, bucket, Iceberg REST Catalog, Spark, Trino có sẵn sàng không.
Nhóm 2 (tuần tự):   Trino ghi bảng Iceberg → catalog ghi nhận bảng → file nằm trên MinIO.

Chạy tay từ giao diện Airflow (schedule=None).
"""
from __future__ import annotations

import os
from datetime import timedelta

import boto3
import pendulum
import requests
from botocore.config import Config
from airflow.decorators import dag, task

# ---------------------------------------------------------------------------
# Cấu hình: đọc từ biến môi trường (khai báo trong x-airflow-env của compose)
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
TRINO_URI = os.getenv("TRINO_URI", "http://trino:8080")
SPARK_MASTER_UI = os.getenv("SPARK_MASTER_UI", "http://spark-master:8080")
LANDING_BUCKET = os.getenv("LANDING_BUCKET", "landing")
LAKEHOUSE_BUCKET = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")

TRINO_CATALOG = "lakehouse"
SMOKE_SCHEMA = "airflow_smoke"
SMOKE_TABLE = "ping"
HTTP_TIMEOUT = 30  # giây


# ---------------------------------------------------------------------------
# Hàm tiện ích (không phải task)
# ---------------------------------------------------------------------------
def s3_client():
    """Tạo client S3 trỏ vào MinIO.

    Tài khoản lấy tự động từ biến AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION.
    addressing_style="path" → gọi http://minio:9000/<bucket>/... (MinIO cần kiểu này).
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        config=Config(s3={"addressing_style": "path"}),
    )


def run_trino(sql: str) -> list:
    """Chạy 1 câu SQL trên Trino qua HTTP API, trả về toàn bộ dòng kết quả.

    Cách Trino trả kết quả:
      1. POST câu SQL tới /v1/statement → nhận JSON có "nextUri".
      2. Gọi GET nextUri lặp lại; mỗi lần có thể kèm một phần "data".
      3. Hết "nextUri" nghĩa là câu lệnh đã xong. Có "error" nghĩa là câu lệnh lỗi.
    """
    headers = {"X-Trino-User": "airflow", "X-Trino-Catalog": TRINO_CATALOG}
    resp = requests.post(f"{TRINO_URI}/v1/statement", data=sql.encode("utf-8"),
                         headers=headers, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    payload = resp.json()

    rows: list = []
    while True:
        if "error" in payload:
            raise RuntimeError(f"Trino lỗi khi chạy [{sql}]: {payload['error'].get('message')}")
        rows.extend(payload.get("data", []))
        next_uri = payload.get("nextUri")
        if not next_uri:
            return rows
        payload = requests.get(next_uri, headers=headers, timeout=HTTP_TIMEOUT).json()


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="smoke_test_lakehouse",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["tlcn", "smoke-test"],
    doc_md=__doc__,
)
def smoke_test_lakehouse():

    # ---------------- Nhóm 1: hạ tầng ----------------
    @task
    def check_minio() -> str:
        """MinIO còn sống: endpoint health trả HTTP 200."""
        r = requests.get(f"{MINIO_ENDPOINT}/minio/health/live", timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return "MinIO OK"

    @task
    def check_buckets() -> list:
        """Đăng nhập được MinIO và đã có đủ 2 bucket do minio-init tạo."""
        names = {b["Name"] for b in s3_client().list_buckets()["Buckets"]}
        missing = {LANDING_BUCKET, LAKEHOUSE_BUCKET} - names
        if missing:
            raise ValueError(f"Thiếu bucket: {missing}. Xem log: docker compose logs minio-init")
        return sorted(names)

    @task
    def check_catalog() -> dict:
        """Iceberg REST Catalog trả lời /v1/config."""
        r = requests.get(f"{ICEBERG_REST_URI}/v1/config", timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()

    @task
    def check_spark() -> int:
        """Spark master có ít nhất 1 worker ALIVE (đọc JSON của Master UI)."""
        info = requests.get(f"{SPARK_MASTER_UI}/json/", timeout=HTTP_TIMEOUT).json()
        alive = info.get("aliveworkers", 0)
        if alive < 1:
            raise RuntimeError("Spark master không có worker nào sống")
        print(f"Workers sống: {alive} | cores: {info.get('cores')} | memory MB: {info.get('memory')}")
        return alive

    @task
    def check_trino() -> str:
        """Trino đã khởi động xong và nhìn thấy catalog 'lakehouse'."""
        info = requests.get(f"{TRINO_URI}/v1/info", timeout=HTTP_TIMEOUT).json()
        if info.get("starting"):
            raise RuntimeError("Trino vẫn đang khởi động")
        catalogs = [row[0] for row in run_trino("SHOW CATALOGS")]
        if TRINO_CATALOG not in catalogs:
            raise RuntimeError(f"Trino không có catalog '{TRINO_CATALOG}': {catalogs}")
        return f"Trino {info['nodeVersion']['version']} OK"

    # ---------------- Nhóm 2: luồng dữ liệu ----------------
    @task
    def trino_write_iceberg(run_id=None) -> int:
        """Trino tạo schema + bảng Iceberg (nếu chưa có) và ghi 1 dòng cho lần chạy này.

        run_id do Airflow tự truyền vào (mỗi lần chạy DAG có một run_id riêng).
        Trả về tổng số dòng trong bảng → mỗi lần chạy tăng thêm 1.
        """
        full_table = f"{TRINO_CATALOG}.{SMOKE_SCHEMA}.{SMOKE_TABLE}"
        safe_run_id = run_id.replace("'", "''")
        run_trino(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{SMOKE_SCHEMA}")
        run_trino(f"CREATE TABLE IF NOT EXISTS {full_table} ("
                  f"run_id VARCHAR, checked_at TIMESTAMP(6) WITH TIME ZONE)")
        run_trino(f"INSERT INTO {full_table} VALUES ('{safe_run_id}', current_timestamp)")
        total = run_trino(f"SELECT count(*) FROM {full_table}")[0][0]
        print(f"Bảng {full_table} hiện có {total} dòng")
        return total

    @task
    def verify_catalog_has_table() -> str:
        """Hỏi thẳng catalog (không qua Trino): bảng có tồn tại, metadata đang ở đâu?"""
        url = f"{ICEBERG_REST_URI}/v1/namespaces/{SMOKE_SCHEMA}/tables/{SMOKE_TABLE}"
        r = requests.get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        location = r.json()["metadata-location"]
        print(f"Catalog trỏ tới metadata: {location}")
        return location

    @task
    def verify_files_on_minio(metadata_location: str) -> dict:
        """Từ đường dẫn metadata, kiểm tra file thật trên MinIO.

        metadata_location dạng: s3://lakehouse/airflow_smoke/ping-<uuid>/metadata/xxxx.metadata.json
        → thư mục bảng là phần trước '/metadata/' → đếm file trong data/ và metadata/.
        """
        without_scheme = metadata_location.removeprefix("s3://")
        bucket, key = without_scheme.split("/", 1)
        table_root = key.split("/metadata/")[0]

        counts = {"parquet": 0, "metadata": 0}
        paginator = s3_client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{table_root}/"):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    counts["parquet"] += 1
                elif "/metadata/" in obj["Key"]:
                    counts["metadata"] += 1

        if counts["parquet"] < 1:
            raise RuntimeError(f"Không thấy file Parquet nào dưới s3://{bucket}/{table_root}/")
        print(f"s3://{bucket}/{table_root}/ → {counts}")
        return counts

    @task
    def report(total_rows: int, file_counts: dict, trino_status: str, spark_workers: int) -> None:
        """In tổng kết cuối cùng."""
        print("=" * 60)
        print("SMOKE TEST LAKEHOUSE: TẤT CẢ ĐẠT")
        print(f"- {trino_status} | Spark workers: {spark_workers}")
        print(f"- Bảng {SMOKE_SCHEMA}.{SMOKE_TABLE}: {total_rows} dòng (mỗi lần chạy +1)")
        print(f"- File trên MinIO: {file_counts['parquet']} Parquet, {file_counts['metadata']} metadata")
        print("=" * 60)

    # ---------------- Thứ tự chạy ----------------
    minio_ok = check_minio()
    buckets_ok = check_buckets()
    catalog_ok = check_catalog()
    spark_ok = check_spark()
    trino_ok = check_trino()

    total = trino_write_iceberg()
    [minio_ok, buckets_ok, catalog_ok, spark_ok, trino_ok] >> total

    location = verify_catalog_has_table()
    total >> location

    files = verify_files_on_minio(location)
    report(total, files, trino_ok, spark_ok)


smoke_test_lakehouse()