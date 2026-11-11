#!/usr/bin/env python3
"""Kiểm tra ràng buộc của bộ dữ liệu giả lập. Chạy: python validate.py <dataset> [<du_lieu_mau/jsonl>]"""
import glob, gzip, json, os, sys, collections

root = sys.argv[1]
sample = sys.argv[2] if len(sys.argv) > 2 else None


def rows(src, ent, pattern="*"):
    for f in sorted(glob.glob(os.path.join(root, f"source={src}", f"entity={ent}", f"dt={pattern}", "*.jsonl.gz"))):
        with gzip.open(f, "rt", encoding="utf-8") as fh:
            for l in fh:
                yield json.loads(l)


ok = True


def check(name, cond, detail=""):
    global ok
    print(("  OK   " if cond else "  FAIL ") + name + (f" — {detail}" if detail else ""))
    ok &= bool(cond)


print("Đọc master...")
prod_ids, prod_codes, combos = set(), set(), {}
for r in rows("nhanh", "products"):
    d = r["data"]
    prod_ids.add(d["id"])
    if d.get("code"):
        prod_codes.add(d["code"])
    if d["type"] == 9:
        combos[d["id"]] = d.get("combos") or []
check("Mọi combo (type 9) đều có SP con", all(len(v) > 0 for v in combos.values()), f"{len(combos)} combo")
check("SP con của combo tồn tại trong products", all(c["id"] in prod_ids for v in combos.values() for c in v))

print("Đọc orders...")
last, versions, bad_ref, bad_cancel, bad_mono = {}, collections.Counter(), 0, 0, 0
prev = {}
for r in rows("nhanh", "orders"):
    o = r["data"]
    oid = o["info"]["id"]
    versions[oid] += 1
    if oid in prev and o["info"]["updatedAt"] < prev[oid]:
        bad_mono += 1
    prev[oid] = o["info"]["updatedAt"]
    last[oid] = o
    for p in o["products"]:
        if p["id"] not in prod_ids:
            bad_ref += 1
    if o["info"]["status"] in (63, 64, 58, 68) and o["carrier"]["deliveryAt"]:
        bad_cancel += 1
check("products.id trong đơn có trong products", bad_ref == 0, f"{bad_ref} dòng lỗi")
check("Đơn hủy (63/64/58/68) không có ngày giao", bad_cancel == 0, f"{bad_cancel} bản ghi lỗi")
check("updatedAt tăng dần giữa các phiên bản của 1 đơn", bad_mono == 0, f"{bad_mono} lỗi")
print(f"  info: {len(last):,} đơn duy nhất, {sum(versions.values()):,} bản ghi (trung bình {sum(versions.values())/len(last):.2f} phiên bản/đơn)")

print("Đọc inventory_bills...")
bad_bill, exp_orders = 0, set()
for r in rows("nhanh", "inventory_bills"):
    b = r["data"]
    if b["product"]["id"] not in prod_ids:
        bad_bill += 1
    if b["type"] == 2 and b["reference"]["orderId"]:
        exp_orders.add(b["reference"]["orderId"])
check("products.id trong phiếu kho có trong products", bad_bill == 0, f"{bad_bill} lỗi")
check("Phiếu xuất bán tham chiếu đúng đơn", all(x in last for x in exp_orders), f"{len(exp_orders):,} đơn có phiếu xuất")
canc_exp = sum(1 for x in exp_orders if last[x]["info"]["status"] in (63, 64, 58, 68))
check("Đơn hủy trước khi giao không có phiếu xuất", canc_exp == 0, f"{canc_exp}")

print("Đọc manual...")
latest = sorted(glob.glob(os.path.join(root, "source=manual", "entity=product_category", "dt=*")))[-1].split("dt=")[-1]
skus = [r["data"] for r in rows("manual", "product_category", latest)]
groups = {r["data"]["category_name"] for r in rows("manual", "category")}
check("SKU trong product_category tồn tại trong products",
      all((s["sku_code"] in prod_codes) if s["sku_code"] else (s["product_id"] in prod_ids) for s in skus), f"{len(skus)} SKU")
check("category_name của SKU có trong bảng nhóm", all(s["category_name"] in groups for s in skus))
mp = [r["data"] for r in rows("manual", "shopee_listing_map")]
check("shopee_listing_map trỏ đúng sku", all(m["sku_code"] in prod_codes for m in mp))

print("Đọc shopee...")
items = {m["item_id"] for m in mp}
models = {m["model_id"] for m in mp}
sns = {o["channel"].get("appOrderId") for o in last.values() if o["channel"]["saleChannel"] == 42}
rv = [r["data"] for r in rows("shopee", "reviews")]
check("Review trỏ đúng đơn Shopee (order_sn)", all(x["order_sn"] in sns for x in rv), f"{len(rv):,} review")
check("Review trỏ đúng item/model", all(x["item_id"] in items and x["model_id"] in models for x in rv))
pp_ids = set()
for r in rows("shopee", "product_performance"):
    pp_ids.add(r["data"]["id"])
check("product_performance.id có trong listing map", pp_ids <= items, f"{len(pp_ids)} listing")

if sample:
    print("Kiểm tra không lộ dữ liệu giao dịch thật...")
    real_oid, real_sn, real_shop, real_item, real_comment = set(), set(), set(), set(), set()
    for l in open(os.path.join(sample, "nhanh", "orders.jsonl"), encoding="utf-8"):
        o = json.loads(l)["data"]
        real_oid.add(o["info"]["id"])
        real_sn.add(o["channel"].get("appOrderId"))
        real_shop.add(o["channel"].get("shopId"))
    for l in open(os.path.join(sample, "shopee", "product_performance.jsonl"), encoding="utf-8"):
        r = json.loads(l)
        real_item.add(r["data"]["id"])
        real_shop.add(r["_request"]["shop_id"])
    for l in open(os.path.join(sample, "shopee", "reviews.jsonl"), encoding="utf-8"):
        d = json.loads(l)["data"]
        real_comment.add(d["comment_id"])
        real_item.add(d["product_id"])
    real_shop.discard(None)
    check("Không trùng id đơn thật", not (set(last) & real_oid))
    check("Không trùng mã đơn sàn thật", not ({o["channel"].get("appOrderId") for o in last.values()} & (real_sn - {None})))
    check("Không dùng shop_id thật", not ({o["channel"].get("shopId") for o in last.values()} & real_shop)
          and not ({m["shop_id"] for m in mp} & real_shop))
    check("Không dùng item_id Shopee thật", not (items & real_item))
    check("Không trùng comment_id thật", not ({x["comment_id"] for x in rv} & real_comment))

print("\nKẾT QUẢ:", "TẤT CẢ ĐẠT" if ok else "CÓ LỖI")
sys.exit(0 if ok else 1)
