"""Xây master data (sản phẩm, combo, danh mục, thương hiệu, kho) từ dữ liệu mẫu.

Thông tin cơ bản (tên/mã SP, giá niêm yết, danh mục, thương hiệu, kho) được dùng lại
từ du_lieu_mau. Các trường thiếu được suy ra theo quy tắc cố định (seed cố định).
"""
import json, os, re, random, unicodedata
from datetime import datetime, timezone

R = random.Random(20261110)


def load(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def strip_accents(s):
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.replace("đ", "d").replace("Đ", "D")


BRAND_KEYWORDS = [
    ("black rouge", 71112), ("nohj", 71098), ("jm solution", 71100), ("mamonde", 71101),
    ("lei lani", 71104), ("huxley", 71105), ("eunyul", 71118), ("innisfree", 71125),
    ("banobagi", 77196), ("unpa", 71110), ("bubi bubi", 71110), ("mediheal", 71099),
    ("care zone", 71106), ("vaseline", 71119), ("illiyoon", 71103), ("illiyooon", 71103),
    ("simple", 74963), ("blessed moon", 74835), ("hatherine", 76286), ("natureby", 71097),
    ("vitahalo", 71130), ("sooayeon", 71126), ("hidden orga", 71115), ("april skin", 75823),
    ("maycreate", 71132),
]

# Danh mục Nhanh (categories) theo từ khoá tên SP
NHANH_CAT_RULES = [
    ("máy rửa mặt", 552448), ("nước rửa cọ", 584183), ("rửa vệ sinh cọ", 584183),
    ("cushion", 556135), ("tint", 552460), ("son ", 552460), ("dưỡng môi", 552465),
    ("lip", 552465), ("toner", 552461), ("serum", 552459), ("essence mask", 549415),
    ("mặt nạ", 549415), ("mask", 549415), ("sữa rửa mặt", 549417), ("face wash", 549417),
    ("tẩy trang", 566043), ("micellar", 566043), ("pore clear", 576124), ("dán mụn", 552455),
    ("gel", 552457), ("moisturizer", 552457), ("dưỡng ẩm", 552457), ("kit capsule", 552457),
]

# Nhóm quản trị nội bộ (manual/category) theo từ khoá — thứ tự quan trọng
GROUP_RULES = [
    ("giao bù", "Mã nội bộ"), ("ahamove", "Mã nội bộ"), ("test", "Hàng B Grade / test / dummy"),
    ("quà tặng", "Quà tặng"), ("móc khoá", "Quà tặng"), ("dây đeo khẩu trang", "Nhóm SP Hỗ Trợ"),
    ("lô cuốn", "Nhóm Lô Cuốn Tóc"), ("lô uốn", "Nhóm Lô Cuốn Tóc"), ("hair roll", "Nhóm Lô Cuốn Tóc"),
    ("vệ sinh cọ", "Nhóm Vệ Sinh Cọ"), ("rửa cọ", "Nhóm Vệ Sinh Cọ"), ("giỏ lưới", "Nhóm Vệ Sinh Cọ"),
    ("dcvs", "Nhóm Vệ Sinh Cọ"), ("clean & dry", "Nhóm Vệ Sinh Cọ"), ("brush cleanser", "Nhóm Vệ Sinh Cọ"),
    ("sp304", "Nhóm FL603 và SP304"), ("f-l603", "Nhóm FL603 và SP304"), ("f-l604", "Nhóm FL603 và SP304"),
    ("khay", "Nhóm FL603 và SP304"), ("base kit", "Nhóm FL603 và SP304"),
    ("đuôi cá", "Nhóm Cọ Foundation"), ("foundation", "Nhóm Cọ Foundation"), ("tán nền", "Nhóm Cọ Foundation"),
    ("kem nền", "Nhóm Cọ Foundation"),
    ("mút", "Nhóm Mút"), ("puff", "Nhóm Mút"), ("bông trang điểm", "Nhóm Mút"), ("bông phấn", "Nhóm Mút"),
    ("kéo", "Nhóm Dao Cạo Nhíp Kéo"), ("dao cạo", "Nhóm Dao Cạo Nhíp Kéo"), ("nhíp", "Nhóm Dao Cạo Nhíp Kéo"),
    ("bấm móng", "Nhóm Dao Cạo Nhíp Kéo"), ("nail", "Nhóm Dao Cạo Nhíp Kéo"),
    ("kẹp mi", "Nhóm Kẹp Mi"), ("kẹp bấm mi", "Nhóm Kẹp Mi"), ("bấm mi", "Nhóm Kẹp Mi"),
    ("bông tắm", "Nhóm Phụ Kiện Nhà Tắm"), ("bồng tắm", "Nhóm Phụ Kiện Nhà Tắm"), ("tắm mây", "Nhóm Phụ Kiện Nhà Tắm"),
    ("găng tay", "Nhóm Phụ Kiện Nhà Tắm"), ("chà gót", "Nhóm Phụ Kiện Nhà Tắm"), ("lace bath", "Nhóm Phụ Kiện Nhà Tắm"),
    ("lược gội", "Nhóm Guasha"), ("massage", "Nhóm Guasha"),
    ("lược", "Nhóm Lược"),
    ("cọ môi", "Nhóm Cọ Môi"), ("cọ mội", "Nhóm Cọ Môi"), ("tán son", "Nhóm Cọ Môi"), ("ckd", "Nhóm Cọ Môi"),
    ("che khuyết điểm", "Nhóm Cọ Môi"),
    ("mắt", "Nhóm Cọ Mắt"), ("eye", "Nhóm Cọ Mắt"),
    ("cọ", "Nhóm Cọ"), ("brush", "Nhóm Cọ"),
    ("mi giả", "Nhóm Mi Giả"),
    ("máy rửa mặt", "Nhóm SP Hỗ Trợ"), ("pad rửa mặt", "Nhóm SP Hỗ Trợ"), ("nặn mụn", "Nhóm SP Hỗ Trợ"),
    ("kẹp tóc", "Nhóm SP Hỗ Trợ"), ("kẹp toc", "Nhóm SP Hỗ Trợ"), ("phu kiện tóc", "Nhóm SP Hỗ Trợ"),
    ("dây buộc", "Nhóm SP Hỗ Trợ"), ("dây cột", "Nhóm SP Hỗ Trợ"), ("băng dán", "Nhóm SP Hỗ Trợ"),
    ("lót giày", "Nhóm SP Hỗ Trợ"), ("gương", "Nhóm SP Hỗ Trợ"), ("túi đựng", "Nhóm SP Hỗ Trợ"),
    ("khăn giấy", "Vệ sinh & sát khuẩn"), ("bọt biển", "Làm sạch"), ("cotton pad", "Làm sạch"),
    ("tẩy trang", "Làm sạch"), ("rửa mặt", "Làm sạch"), ("face wash", "Làm sạch"), ("micellar", "Làm sạch"),
    ("mặt nạ", "Mặt nạ"), ("mask", "Mặt nạ"),
    ("dưỡng môi", "Dưỡng mi & môi"), ("lip", "Dưỡng mi & môi"),
    ("tint", "Trang điểm"), ("son ", "Trang điểm"), ("cushion", "Trang điểm"),
    ("pore clear", "Trị mụn"), ("dán mụn", "Trị mụn"), ("mụn", "Trị mụn"),
    ("toner", "Dưỡng da"), ("serum", "Dưỡng da"), ("gel", "Dưỡng da"), ("dưỡng", "Dưỡng da"),
    ("capsule", "Dưỡng da"), ("vegitoks", "Làm sạch"),
]

CODE_PREFIX_GROUP = [
    ("MP-", "Mặt nạ"), ("COM-199K", "Mặt nạ"), ("COM-99K", "Mặt nạ"), ("ILY-", "Dưỡng da"),
    ("LT-", "Trang điểm"), ("COM-BR", "Trang điểm"), ("LC-", "Dưỡng mi & môi"), ("LS-", "Dưỡng mi & môi"),
    ("NSP-", "Trị mụn"), ("CZ-", "Trị mụn"), ("SE-", "Dưỡng da"), ("CR-", "Dưỡng da"), ("TON-", "Dưỡng da"),
    ("CRM-", "Dưỡng da"), ("CSL-", "Làm sạch"), ("CLW-", "Làm sạch"), ("CL-", "Làm sạch"),
    ("COM-SMP", "Làm sạch"), ("COM-SIM", "Làm sạch"), ("MS-", "Nhóm SP Hỗ Trợ"), ("DS-", "Làm sạch"),
]

DEFAULT_WEIGHT = {
    "Nhóm Lô Cuốn Tóc": 20, "Nhóm Mút": 10, "Nhóm Cọ": 15, "Nhóm Cọ Foundation": 20, "Nhóm Cọ Môi": 10,
    "Nhóm Cọ Mắt": 30, "Nhóm Lược": 60, "Nhóm Guasha": 40, "Nhóm Kẹp Mi": 25, "Nhóm Dao Cạo Nhíp Kéo": 15,
    "Nhóm Phụ Kiện Nhà Tắm": 50, "Nhóm Vệ Sinh Cọ": 120, "Nhóm FL603 và SP304": 80, "Mặt nạ": 30,
    "Dưỡng da": 120, "Làm sạch": 150, "Trang điểm": 30, "Dưỡng mi & môi": 20, "Trị mụn": 15,
    "Nhóm SP Hỗ Trợ": 30, "Quà tặng": 20, "Vệ sinh & sát khuẩn": 40,
}


def rule_lookup(name, rules, default=None):
    n = " " + name.lower() + " "
    for kw, val in rules:
        if kw in n:
            return val
    return default


def round_price(x):
    """Làm tròn giá kiểu bán lẻ VN: ...900"""
    if x <= 0:
        return 0
    v = int(round(x / 1000.0)) * 1000 - 100
    return max(v, 900)


def build(sample_dir):
    J = lambda *p: load(os.path.join(sample_dir, *p))
    raw_products = J("nhanh", "products.jsonl")
    items = {}

    def add(pid, code, name, **kw):
        if pid is None:
            return
        d = items.setdefault(pid, {"id": pid, "code": None, "name": name.strip().strip('"').replace("\t", " ")})
        if code and not d["code"]:
            d["code"] = code
        for k, v in kw.items():
            if v is not None and d.get(k) in (None, 0, ""):
                d[k] = v
        return d

    combos = {}  # combo_id -> list of (component_id, qty)
    for r in raw_products:
        p = r["data"]
        add(p["id"], p.get("code"), p["name"], retail=p["prices"]["retail"], cost=p["prices"]["avgCost"] or None,
            imp=p["prices"]["import"] or None, type=p["type"], categoryId=p.get("categoryId"), brandId=p.get("brandId"),
            barcode=p.get("barcode"), weight=p["shipping"]["weight"] or None, createdAt=p["createdAt"],
            avatar=(p.get("images") or {}).get("avatar"))
        if p.get("combos"):
            combos[p["id"]] = [(c["id"], c["quantity"]) for c in p["combos"]]
            for c in p["combos"]:
                add(c["id"], c["code"], c["name"])
    for r in J("nhanh", "inventory.jsonl"):
        p = r["data"]
        add(p["productId"], None, p["name"], retail=p["prices"]["price"] or None, cost=p["prices"]["avgCost"] or None,
            imp=p["prices"]["import"] or None, barcode=p.get("barcode"))
    for r in J("nhanh", "combos.jsonl"):
        c = r["data"]
        add(c["id"], c["comboCode"], c["comboName"], type=9)
        combos.setdefault(c["id"], [(i["id"], i["qty"]) for i in c["comboItems"]])
        for i in c["comboItems"]:
            add(i["id"], i["code"], i["name"], retail=i["price"] or None)
    sample_order_codes = {}
    for r in J("nhanh", "orders.jsonl"):
        for x in r["data"]["products"]:
            add(x["id"], x.get("code"), x["name"], cost=x["avgCost"] or None, barcode=x.get("barcode"),
                weight=x["weight"] or None, avatar=x.get("image"))
            if x.get("combos"):
                combos.setdefault(x["id"], [(c["id"], c["quantity"]) for c in x["combos"]])
                items[x["id"]]["type"] = 9
                for c in x["combos"]:
                    add(c["id"], c["code"], c["name"])
            if x.get("code"):
                sample_order_codes[x["code"]] = sample_order_codes.get(x["code"], 0) + 1
    for r in J("nhanh", "inventory_bills.jsonl"):
        x = r["data"]["product"]
        add(x["id"], x.get("code"), x["name"], retail=x["prices"]["retail"] or None,
            cost=x["prices"]["avgCost"] or None, imp=x["prices"]["import"] or None, barcode=x.get("barcode"))
        if x.get("code"):
            sample_order_codes[x["code"]] = sample_order_codes.get(x["code"], 0) + 1

    # ---- suy ra trường thiếu ----
    known_ct = sorted((d["id"], d["createdAt"]) for d in items.values() if d.get("createdAt"))

    def interp_created(pid):
        lo = [k for k in known_ct if k[0] <= pid]
        hi = [k for k in known_ct if k[0] >= pid]
        if lo and hi:
            (a, ta), (b, tb) = lo[-1], hi[0]
            if b == a:
                return ta
            return int(ta + (tb - ta) * (pid - a) / (b - a))
        return (lo[-1][1] if lo else hi[0][1]) + R.randint(-86400 * 30, 86400 * 30)

    used_codes = {d["code"] for d in items.values() if d["code"]}
    bc_seq = 10000003000
    for d in items.values():
        name = d["name"]
        low = name.lower()
        d.setdefault("type", None)
        if d["type"] is None:
            d["type"] = 9 if d["id"] in combos else 1
        if d["id"] in combos and d["type"] != 9:
            d["type"] = 9
        if not d.get("createdAt"):
            d["createdAt"] = interp_created(d["id"])
        # nhóm nội bộ
        internal = d["type"] == 6 or low.startswith("test") or "giao bù" in low or low == "ahamove"
        d["is_internal"] = internal
        d["group"] = rule_lookup(name, GROUP_RULES, "Nhóm SP Hỗ Trợ")
        if internal:
            d["group"] = "Mã nội bộ" if ("giao bù" in low or low == "ahamove") else "Hàng B Grade / test / dummy"
        code = d["code"] or ""
        for pre, grp in CODE_PREFIX_GROUP:
            if code.startswith(pre) and d["group"] not in ("Quà tặng",):
                d["group"] = grp
                break
        is_tmr = code.startswith("TMR") or low.startswith("tmr") or " tmr " in f" {low} " or (
            not code and d["group"].startswith("Nhóm") and d["group"] not in ("Nhóm SP Hỗ Trợ",))
        if any(k in low for k, _ in BRAND_KEYWORDS):
            is_tmr = code.startswith("TMR")
        d["is_tmr"] = bool(is_tmr) and not internal
        # code
        if not d["code"] and not internal:
            base = "TMR" if d["is_tmr"] else "GEN"
            words = [w for w in re.split(r"[^A-Za-z0-9]+", strip_accents(name).upper()) if w and w != "TMR"]
            c1 = "".join(w[0] for w in words[:-1])[:4] or "X"
            c2 = words[-1][:4] if words else "X"
            cand = f"{base}-{c1}-{c2}"
            i = 2
            while cand in used_codes:
                cand = f"{base}-{c1}-{c2}{i}"
                i += 1
            d["code"] = cand
            used_codes.add(cand)
        # brand
        if not d.get("brandId"):
            b = None
            for kw, bid in BRAND_KEYWORDS:
                if kw in low:
                    b = bid
                    break
            if d["is_tmr"]:
                b = 71131
            # giữ một ít null như dữ liệu thật
            d["brandId"] = b if R.random() > 0.08 else None
        if not d.get("categoryId"):
            if d["is_tmr"]:
                c = 552447
            elif d["type"] == 9:
                c = 552452
            else:
                c = rule_lookup(name, NHANH_CAT_RULES)
            d["categoryId"] = c if R.random() > 0.10 else None
        if not d.get("weight"):
            d["weight"] = DEFAULT_WEIGHT.get(d["group"], 30)
        if not d.get("barcode") and not internal:
            bc_seq += R.randint(1, 7)
            d["barcode"] = str(bc_seq)

    # giá: thành phần trước, combo sau
    def ensure_price(d, depth=0):
        if d.get("retail") and d.get("cost"):
            return
        if d["type"] == 9 and d["id"] in combos and depth < 3:
            tot_r, tot_c = 0, 0
            for cid, q in combos[d["id"]]:
                c = items[cid]
                ensure_price(c, depth + 1)
                tot_r += (c.get("retail") or 0) * q
                tot_c += (c.get("cost") or 0) * q
            if not d.get("retail"):
                d["retail"] = round_price(tot_r * 0.9)
            if not d.get("cost"):
                d["cost"] = int(tot_c)
            return
        if not d.get("retail"):
            d["retail"] = 0 if d["is_internal"] else round_price((d.get("cost") or 9000) / (0.22 if d["is_tmr"] else 0.72))
        if not d.get("cost"):
            ratio = 0.22 if d["is_tmr"] else 0.72
            d["cost"] = int(d["retail"] * ratio * R.uniform(0.85, 1.15))

    for d in items.values():
        ensure_price(d)
    for d in items.values():
        if not d.get("imp"):
            d["imp"] = d["cost"]
        d["is_gift"] = (d["group"] == "Quà tặng") or ("quà tặng" in d["name"].lower())
        if d["retail"] == 0 and not d["is_internal"]:
            d["is_gift"] = True
        # sản phẩm chỉ bán ở kênh cũ (Tiki/Amazon) -> ngừng kinh doanh
        d["legacy"] = ("tiki" in d["name"].lower()) or ("amazon" in d["name"].lower()) or ("TIKI" in (d["code"] or ""))
        if not d.get("avatar"):
            slug = re.sub(r"[^A-Za-z0-9]+", "-", strip_accents(d["name"])).strip("-")[:60]
            d["avatar"] = f"https://cdn.example-shop.vn/products/{slug}.jpg"

    # combo phải có thành phần; loại combo rỗng
    for pid in list(combos):
        combos[pid] = [(c, q) for c, q in combos[pid] if c in items and c != pid and items[c]["type"] != 9]
        if not combos[pid]:
            del combos[pid]
            items[pid]["type"] = 1

    # ---- vòng đời & độ phổ biến ----
    START_TS = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())
    for d in items.values():
        d["launch_ts"] = max(d["createdAt"], START_TS - 86400 * 400)
        if d["is_internal"] or d["is_gift"] or d["legacy"]:
            d["sellable"] = False
        else:
            d["sellable"] = True
        # mỹ phẩm phân phối (không phải TMR) giảm dần & ngừng bán giữa 2024-2025
        if d["sellable"] and not d["is_tmr"]:
            d["discontinue_ts"] = int(datetime(2024, R.randint(3, 12), R.randint(1, 28), tzinfo=timezone.utc).timestamp()) + R.randint(0, 86400 * 200)
        elif d["sellable"] and R.random() < 0.08:
            d["discontinue_ts"] = int(datetime(2025, R.randint(1, 12), R.randint(1, 28), tzinfo=timezone.utc).timestamp())
        else:
            d["discontinue_ts"] = None
        base = R.paretovariate(1.3)
        boost = 1 + 2.5 * sample_order_codes.get(d["code"], 0)
        d["pop"] = (base * boost) * (1.0 if d["is_tmr"] else 0.25) * (0.7 if d["type"] == 9 else 1.0)
        # điểm chất lượng (ảnh hưởng số sao review)
        d["quality"] = min(0.98, max(0.55, R.gauss(0.86, 0.08)))
    for code in ("TMR-SCR-SLV", "TMR-SCR-BLK", "TMR-SWB-LFW", "TMR-HROL-LP40"):
        for d in items.values():
            if d["code"] == code:
                d["quality"] = 0.62  # sản phẩm có vấn đề chất lượng -> nhiều review xấu

    # ---- master khác lấy nguyên ----
    categories = [r["data"] for r in J("nhanh", "categories.jsonl")]
    brands = [r["data"] for r in J("nhanh", "brands.jsonl")]
    depots = [r["data"] for r in J("nhanh", "depots.jsonl")]
    groups = [r["data"] for r in J("manual", "category.jsonl")]
    return dict(items=items, combos=combos, categories=categories, brands=brands, depots=depots, groups=groups)


if __name__ == "__main__":
    import sys
    m = build(sys.argv[1])
    it = m["items"]
    print(len(it), "items", len(m["combos"]), "combos", sum(d["sellable"] for d in it.values()), "sellable",
          sum(d["is_tmr"] and d["sellable"] for d in it.values()), "tmr sellable")
    from collections import Counter
    print(Counter(d["group"] for d in it.values()))
    for d in sorted(it.values(), key=lambda d: -d["pop"])[:15]:
        print(d["code"], d["name"][:40], d["retail"], d["cost"], round(d["pop"], 2), d["type"])
