# Dữ liệu mẫu đầu vào — TMR Data Warehouse

Có **3 nguồn, 12 bảng đầu vào**. Mỗi bảng có 50 dòng mẫu (bảng nào ít hơn 50 dòng thì lấy hết). Đây là dữ liệu thật đã che thông tin cá nhân, dùng làm khuôn để sinh dữ liệu giả lập.

| Nguồn | Bảng (entity) | Dòng | Cách lấy về | Tần suất |
|---|---|---|---|---|
| nhanh | orders | 50 | API Nhanh v3 (Lambda) | hằng ngày, kéo lại 3 ngày |
| nhanh | products | 50 | API Nhanh (snapshot) | hằng ngày |
| nhanh | inventory | 50 | API Nhanh (snapshot) | hằng ngày |
| nhanh | combos | 50 | API Nhanh (snapshot) | hằng ngày |
| nhanh | categories | 29 | API Nhanh (snapshot) | hằng ngày |
| nhanh | brands | 50 | API Nhanh (snapshot) | hằng ngày |
| nhanh | depots | 4 | API Nhanh (snapshot) | hằng ngày |
| nhanh | inventory_bills | 50 | API Nhanh bill/imexs | hằng ngày, kéo lại 3 ngày |
| shopee | product_performance | 50 | Seller Centre (session lưu trên SSM) | hằng tuần (Chủ nhật), 8 ngày |
| shopee | reviews | 50 | File xuất tay | nhập tay mỗi tuần |
| manual | category | 28 | danh_muc_san_pham.xlsx, sheet `nhom` | khi có thay đổi |
| manual | product_category | 50 | danh_muc_san_pham.xlsx, sheet `sku` | khi có thay đổi |

## Cấu trúc thư mục
```
du_lieu_mau/
├── README.md
├── tong_hop.json            # số dòng, số cột, số trường đã che
├── jsonl/                   # bản gốc (giữ phong bì landing) — dùng cho code
│   ├── nhanh/   orders, products, inventory, combos, categories, brands, depots, inventory_bills
│   ├── shopee/  product_performance, reviews
│   └── manual/  category, product_category
└── csv/                     # bản phẳng để mở Excel — cùng cấu trúc với jsonl/
    ├── nhanh/
    ├── shopee/
    └── manual/
```

## Định dạng
- `*.jsonl`: mỗi dòng giữ nguyên "phong bì" landing:
  - `_source`, `_entity`, `_batch_id`, `_extracted_at`, `_request` là metadata.
  - `data` là bản ghi gốc của nguồn.
  - Sinh dữ liệu giả lập theo đúng khuôn này thì pipeline (stg → int → core → mart) sẽ đọc được luôn.
- `*.csv`: phần `data` đã được làm phẳng (`a.b.c`) để xem nhanh bằng Excel.
  - Các mảng như `products`, `depots` hay `models` được giữ ở dạng chuỗi JSON.
- `tong_hop.json`: số dòng, số cột và số trường đã che của từng bảng.

## Cách chọn mẫu
Mẫu được chọn rải đều chứ không lấy 50 dòng đầu:
- orders: rải theo trạng thái × kênh bán, có đơn chứa combo.
- products: có cả combo (type 9).
- inventory_bills: có cả phiếu nhập (type 1) lẫn phiếu xuất (type 2).
- product_performance: lấy cả 2 shop.
- reviews: rải từ 1 đến 5 sao.
- product_category: rải đều các nhóm.

## Thông tin cá nhân đã che
- **orders:**
  - Tên, địa chỉ, `location` của người nhận → `[MASKED_...]`.
  - SĐT người nhận → số giả sinh từ hash.
  - `shippingAddress.id` → số hash.
  - `saleName`, `packName`, `createdByName`, `description`, `trackingUrl`, tên người chuyển khoản → `[MASKED_...]`.
- **reviews:**
  - `user_name` → `user_<hash>`.
  - `user_portrait` → null.
  - `user_id` → số hash.
  - Ảnh → `[IMAGE_i]`.
  - SĐT nằm trong nội dung bình luận → `[MASKED_PHONE]`.
- **depots:** SĐT kho → `[MASKED_mobile]`.
- Hash là SHA-256 cắt ngắn, nên cùng một giá trị gốc luôn ra cùng một mã. Nhờ vậy vẫn đếm được khách mua lặp lại.

> ⚠️ Vẫn còn **thông tin nội bộ thật**: shop_id Shopee, id/tên sản phẩm, SKU, giá, doanh số, tên kho, id kênh.
> Không đưa thư mục này lên repo public hay chia sẻ ra ngoài. Khi làm đồ án/demo thì dùng **dữ liệu giả lập** sinh từ khuôn này.

## Gợi ý sinh dữ liệu giả lập
1. Lấy cấu trúc từ `*.jsonl`. Phân phối (trạng thái, kênh, giá, số dòng/đơn, số sao) thì tính từ dữ liệu thật trong Athena, ở `mart_orders` và `mart_order_lines`.
2. Sinh **master trước** (depots → brands → categories → products → combos), sau đó mới sinh **giao dịch** (orders, inventory_bills, product_performance, reviews). Giao dịch phải tham chiếu đúng id của master.
3. Giữ ràng buộc:
   - `products.id` trong đơn và phiếu kho phải có trong `products`.
   - Combo phải có SP con.
   - SKU trong `product_category` phải tồn tại.
   - Đơn hủy thì không được có ngày giao.
4. Ghi ra theo layout `source=X/entity=Y/dt=YYYY-MM-DD/<batch>-part0001.jsonl.gz` là chạy được dbt trên dữ liệu giả.
