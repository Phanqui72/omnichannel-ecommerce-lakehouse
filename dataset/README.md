# Dataset giả lập — Data Lakehouse quản lý TMĐT đa kênh

Bộ dữ liệu **giả lập 100% phần giao dịch** cho tiểu luận *"Xây dựng Data Lakehouse cho bài toán quản lý thương mại điện tử đa kênh"*. Bộ này được sinh từ khuôn `du_lieu_mau/` và giữ nguyên cấu trúc "phong bì" landing. Nhờ vậy pipeline (bronze → silver → gold) viết cho dữ liệu thật đọc được bộ này luôn.

- **Giai đoạn:** 01/01/2024 → 09/11/2026 (1.044 ngày, giờ Việt Nam).
- **Kênh bán:** Shopee (2 shop: HCM và Hà Nội), TikTok Shop (2 shop, shop phụ mở từ 06/2026) và Admin (bán sỉ B2B, giao bù, Zalo/FB).
- **Kho:** TMR OFFICIAL STORE (HCM), TMR Store Hà Nội, Kho Hàng Thô (kho tổng) và Shopee FBS (có từ 01/03/2025).
- **Dung lượng:** khoảng 205 MB nén gzip, ~1,24 triệu bản ghi, ~6.000 file.

## 1. Dữ liệu nào thật, dữ liệu nào giả

| Loại | Xử lý |
|---|---|
| Sản phẩm, SKU, tên, barcode, giá niêm yết, combo, danh mục Nhanh, thương hiệu, kho, nhóm quản trị | **Dùng lại** từ dữ liệu mẫu (thông tin cơ bản). Trường còn thiếu được suy ra theo quy tắc cố định. |
| Đơn hàng, phiếu kho, tồn kho theo ngày, hiệu quả sản phẩm Shopee, đánh giá | **Sinh hoàn toàn** bằng mô phỏng. Không có số liệu doanh thu hay hiệu suất thật nào. |
| id đơn, mã đơn sàn, mã vận đơn, shop_id, item_id/model_id Shopee, comment_id, khách hàng | **Giả**, nằm ngoài dải id thật. `validate.py` đã kiểm tra không trùng với mẫu. |
| Tên, SĐT, địa chỉ khách | **Giả nhưng trông như thật**, để bạn tự làm bước che/hash PII ở tầng Silver. |

## 2. Cấu trúc thư mục

```
dataset/
├── README.md                 # file này
├── source=nhanh/entity=<orders|products|inventory|combos|categories|brands|depots|inventory_bills>/dt=YYYY-MM-DD/<batch>-part0001.jsonl.gz
├── source=shopee/entity=<product_performance|reviews>/dt=YYYY-MM-DD/...
├── source=manual/entity=<category|product_category|shopee_listing_map>/dt=YYYY-MM-DD/...
├── _reference/               # bảng tra cứu: tỉnh/phường, shop, mã trạng thái, mode phiếu kho, summary.json
├── _preview/                 # ≤200 dòng mỗi bảng dạng CSV phẳng để mở bằng Excel
└── _generator/               # script sinh lại và kiểm tra (catalog.py, common.py, generate.py, validate.py)
```

Mỗi dòng `.jsonl.gz` có dạng:

```json
{"_source":"nhanh","_entity":"orders","_batch_id":"20260101T230512Z-1a2b3c4d","_extracted_at":"...","_request":{...},"data":{...}}
```

`dt` là ngày landing của dữ liệu. Với job hằng ngày, đó là ngày dữ liệu; job chạy lúc 06:05 sáng hôm sau, tức `23:05Z`.

## 3. Các bảng

| Nguồn / entity | Tần suất | Bản ghi | Ghi chú |
|---|---|---|---|
| nhanh / orders | hằng ngày, incremental theo `updatedAt` | 504.514 (193.017 đơn) | **Một đơn xuất hiện nhiều lần**, mỗi ngày nó đổi trạng thái là một lần (trung bình 2,6 phiên bản/đơn). Ở Silver cần lấy bản có `updatedAt` mới nhất. |
| nhanh / inventory | snapshot hằng ngày | 237.574 | Tồn theo từng SP × 4 kho: `remain`, `holding`, `shipping`, `transfering`, `available`. |
| nhanh / inventory_bills | hằng ngày | 263.169 | Mỗi dòng phiếu là một SP. Combo đã được tách ra SP con. Tồn = tổng nhập − tổng xuất (đã đối soát khớp 100%, có phiếu "Tồn đầu kỳ" ngày 01/01/2024). |
| nhanh / products, combos, categories, brands, depots | snapshot ngày 1 hằng tháng + ngày cuối | 8.222 / 1.943 / … | Giá bán thay đổi theo thời gian, SP mới ra mắt, SP ngừng kinh doanh (`status=2`), kho FBS mới xuất hiện → dùng để làm **SCD type 2**. |
| shopee / product_performance | crawl Chủ nhật hằng tuần, mỗi lần 8 ngày | 170.153 | Phễu theo listing × ngày: impressions → clicks → uv → add-to-cart → placed → confirmed/paid, có thêm `models[]` theo phân loại. **Mỗi ngày đầu kỳ bị crawl 2 lần** (2 batch, số liệu confirmed có thể khác nhau) → cần dedup. |
| shopee / reviews | file xuất tay thứ Hai hằng tuần | 43.706 | 1–5 sao, nội dung, ảnh và lý do đánh giá thấp. ~3% review bị sửa (cùng `comment_id`, `mtime` mới, số sao tăng) xuất hiện lại ở tuần sau. |
| manual / category | theo quý | 28 nhóm | "Nhóm New" xuất hiện từ 07/2025. |
| manual / product_category | theo quý | 272 SKU | SKU → nhóm quản trị. |
| manual / shopee_listing_map | theo quý | ~350 model | **Bảng nối** listing/model Shopee với SKU Nhanh. Đây là bảng mới so với mẫu: Seller Centre không trả SKU, nên cần bảng này để nối với phễu và review. |

Khoá nối chính:
- `orders.products[].id`, `inventory.productId` và `inventory_bills.product.id` nối với `products.id`.
- `inventory_bills.reference.orderId` nối với `orders.info.id`.
- `reviews.order_sn` nối với `orders.channel.appOrderId` (đơn Shopee).
- `reviews.item_id/model_id` và `product_performance.id` nối với `shopee_listing_map.item_id/model_id`, rồi từ đó sang `sku_code`.
- `product_category.sku_code` nối với `products.code`. SP không có mã thì nối qua `product_id`.

Bảng mã trạng thái đơn và mode phiếu kho nằm ở `_reference/order_status.csv` và `_reference/inventory_bill_modes.csv`.

## 4. Kịch bản kinh doanh đã "cài" vào dữ liệu

- **Tăng trưởng:** Shopee HCM tăng ~17%/năm. TikTok Shop mở từ 15/03/2024 và tăng mạnh, đến cuối 2026 đạt khoảng 30% số đơn.
- **Mùa vụ:** ngày đôi (1/1 … 12/12, mạnh nhất là 11/11 và 12/12), giữa tháng (15) và ngày lương (25), Black Friday, 8/3, 20/10.
- **Tết** (10/02/2024, 29/01/2025, 17/02/2026): đơn tăng ~28% trong 3–4 tuần trước Tết, gần như dừng từ mùng −3 đến +4, sau đó hồi phục. Combo/quà tặng bán mạnh dịp Tết và lễ.
- **Vòng đời SP:** SP mới có giai đoạn tăng dần, SP "viral" có đợt tăng vọt (nhất là TikTok), mỹ phẩm phân phối (không phải TMR) giảm dần rồi ngừng bán trong 2024–2025. Độ phổ biến của từng SP còn dao động ngẫu nhiên theo tuần, nên có tín hiệu để **dự báo nhu cầu**.
- **Giá:** giá bán của một số SP tăng 5–12% tại những ngày ngẫu nhiên. Phí sàn Shopee tăng dần (~17% năm 2024 lên ~35% năm 2026). Giá vốn bình quân thay đổi theo từng lô nhập.
- **Kho:** kho bán được bổ sung từ Kho Hàng Thô khi xuống dưới điểm đặt hàng. Kho Hàng Thô đặt hàng xưởng vào thứ Hai, lead time 18–42 ngày, và nhập dự trữ trước Tết. Khi hết hàng, sàn chặn đặt nên doanh số bị "kiểm duyệt" (censored), và một phần đơn rơi vào trạng thái 68 "Hết hàng".
- **Hủy/hoàn:** Shopee hủy ~11%, hoàn ~4,5%. TikTok hủy và hoàn cao hơn. Một số SP chất lượng kém (VD: TMR-SCR-SLV, TMR-SCR-BLK, TMR-SWB-LFW, TMR-HROL-LP40) có tỉ lệ hoàn và review 1–2 sao cao hơn rõ, dễ phát hiện khi phân tích review.
- **Khách hàng:** ~148 nghìn khách, có mua lặp lại. Có thể nhận diện khách bằng tên + địa chỉ (đơn sàn) hoặc SĐT (đơn Admin), dùng cho RFM/cohort.

## 5. "Bẫy" chất lượng dữ liệu (cố ý giữ như dữ liệu thật)

1. Đơn có nhiều phiên bản; trạng thái có thể đổi sau 1–3 tuần (hoàn hàng), nên cần làm MERGE/upsert.
2. Hiệu quả sản phẩm Shopee bị trùng một ngày giữa 2 lần crawl.
3. Review bị sửa nên lặp `comment_id`.
4. Một số SP có `brandId`/`categoryId` null. SP nội bộ (`Giao bù`, `test 1/2`, `Ahamove`) không có `code`. Có SKU kênh cũ `[TIKI ONLY]`/`AMAZON`.
5. Đơn Shopee nhiều sản phẩm có `price` lẻ thập phân (voucher phân bổ). Tổng tiền đúng nằm ở `totalAmount`.
6. Đơn Admin "giao bù" có dòng giá 1.000 và giảm 1.000, tổng bằng 0, kèm phí ship (chi phí CSKH).
7. Tồn kho có thể âm vài đơn vị do đơn Admin được xuất khi thiếu hàng (Nhanh cho phép).
8. `orders.carrier` hiển thị "Tự Vận Chuyển" khi đơn sàn chưa đóng gói. Đơn hủy không có `deliveryAt`. Đơn hoàn có `codAmount = 0`.

## 6. Sinh lại / kiểm tra

```bash
cd _generator
python generate.py --sample ../../du_lieu_mau/jsonl --out ../../dataset_v2 --start 2024-01-01 --end 2026-11-09 --scale 1.0 --seed 2026
python validate.py ../../dataset_v2 ../../du_lieu_mau/jsonl
```

- `--scale 0.3` cho bộ nhẹ hơn; `--scale 2` cho bộ nặng hơn (số đơn tăng/giảm theo tỉ lệ).
- Cùng `--seed` thì ra cùng kết quả.
- Thời gian chạy khoảng 1,5 phút, chỉ cần Python 3.9+ (thư viện chuẩn).
- `validate.py` kiểm tra: FK đơn/phiếu → SP, combo có SP con, SKU trong `product_category` tồn tại, đơn hủy không có ngày giao, phiếu xuất khớp đơn, review/phễu khớp listing map, và không trùng id/shop_id/item_id thật.

> Các mã trạng thái và mode phiếu kho được đối chiếu từ dữ liệu mẫu. Những mode chưa có trong mẫu (VD: nhập hàng hoàn) là giả định của bộ giả lập; xem `_reference/`.
