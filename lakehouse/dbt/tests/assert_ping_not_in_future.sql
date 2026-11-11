-- Không được có lần kiểm tra nào nằm ở tương lai (lệch giờ hệ thống quá 5 phút).
-- Trả về dòng sai: 0 dòng -> PASS, có dòng -> FAIL.
select *
from {{ source('airflow_smoke', 'ping') }}
where checked_at > current_timestamp + interval '5' minute