#!/usr/bin/env python3
"""Sinh dữ liệu giả lập cho tiểu luận "Data Lakehouse quản lý TMĐT đa kênh".

Cách chạy:
    python generate.py --sample <du_lieu_mau/jsonl> --out <dataset> [--start 2024-01-01 --end 2026-11-09 --scale 1.0]

Nguyên tắc:
- Master (sản phẩm, combo, danh mục, thương hiệu, kho) dùng lại từ dữ liệu mẫu.
- Giao dịch (đơn hàng, phiếu kho, tồn kho, hiệu quả sản phẩm Shopee, đánh giá) sinh 100% bằng mô phỏng;
  id đơn, mã vận đơn, shop_id, item_id Shopee, khách hàng... đều là giả.
- Giữ đúng "phong bì" landing (_source/_entity/_batch_id/_extracted_at/_request/data).
"""
import argparse, bisect, heapq, json, math, os, random, sys
from collections import defaultdict
from datetime import datetime, date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import catalog as CAT
from common import *

ap = argparse.ArgumentParser()
ap.add_argument("--sample", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--start", default="2024-01-01")
ap.add_argument("--end", default="2026-11-09")
ap.add_argument("--scale", type=float, default=1.0)
ap.add_argument("--seed", type=int, default=2026)
args = ap.parse_args()

R = random.Random(args.seed)
START = date.fromisoformat(args.start)
END = date.fromisoformat(args.end)
NDAYS = (END - START).days + 1
W = Writer(args.out, R)

HCM, HN, FBS, RAW = 109169, 181215, 236619, 168523
SALE_DEPOTS = [HCM, HN, FBS]
ALL_DEPOTS = [FBS, RAW, HN, HCM]  # thứ tự như dữ liệu mẫu inventory
FBS_START = date(2025, 3, 1)
TET = [date(2024, 2, 10), date(2025, 1, 29), date(2026, 2, 17), date(2027, 2, 6)]

# ======================= MASTER =======================
M = CAT.build(args.sample)
ITEMS, COMBOS = M["items"], M["combos"]
BY_CODE = {d["code"]: d for d in ITEMS.values() if d["code"]}
GIAO_BU = ITEMS.get(36218376)
DEPOT_INFO = {d["id"]: d for d in M["depots"]}
DEPOT_CITY = {HCM: 255, FBS: 255, RAW: 255, HN: 254}
WARDS, WARDS_BY_CITY = build_geo(R)
CITY_NAME = {c: n for c, n, _ in PROVINCES}
CITY_IDS = [c for c, _, _ in PROVINCES]
CITY_W = [w for _, _, w in PROVINCES]


def comp_of(pid):
    """Danh sách (component_id, qty) để trừ kho."""
    if pid in COMBOS:
        return COMBOS[pid]
    return [(pid, 1)]


# --- giá thay đổi theo thời gian (SCD) ---
PRICE_EVENTS = {}  # pid -> sorted list (date, retail)
for d in ITEMS.values():
    ev = [(date(2000, 1, 1), d["retail"])]
    if d["sellable"] and d["is_tmr"] and d["retail"] > 0:
        cur = d["retail"]
        n = R.choices([0, 1, 2], [0.45, 0.4, 0.15])[0]
        days = sorted(R.sample(range(60, max(62, NDAYS - 30)), n)) if n else []
        hist = []
        for k in days:
            hist.append((START + timedelta(days=k), None))
        # giá cuối = giá thật; giá trước đó thấp hơn 5-12% -> lùi ngược
        prices = [cur]
        for _ in hist:
            prices.insert(0, CAT.round_price(prices[0] / R.uniform(1.05, 1.12)))
        ev = [(date(2000, 1, 1), prices[0])] + [(hist[i][0], prices[i + 1]) for i in range(len(hist))]
    PRICE_EVENTS[d["id"]] = ev


def retail_at(pid, day):
    ev = PRICE_EVENTS[pid]
    p = ev[0][1]
    for dd, v in ev:
        if dd <= day:
            p = v
        else:
            break
    return p


for d in ITEMS.values():
    d["avgCost"] = float(d["cost"] or 0)
    d["lastImport"] = d["imp"] or d["cost"] or 0
    d["list_mult"] = R.uniform(1.25, 1.75)      # giá gạch Shopee
    d["tt_mult"] = R.uniform(1.04, 1.22)        # giá niêm yết TikTok trước giảm
    d["launch_day"] = max(START - timedelta(days=500), day_of(d["launch_ts"]))
    d["disc_day"] = day_of(d["discontinue_ts"]) if d["discontinue_ts"] else None
    d["walk"] = 0.0
    d["viral"] = []  # (start_day_idx, len, mult)
    if d["sellable"] and d["is_tmr"] and R.random() < 0.15:
        for _ in range(R.randint(1, 2)):
            d["viral"].append((R.randint(30, NDAYS - 20), R.randint(10, 35), R.uniform(2.0, 5.0)))
    d["tiktok"] = d["sellable"] and d["is_tmr"] and R.random() < 0.7
    d["fbs"] = d["sellable"] and d["is_tmr"] and d["type"] == 1 and d["pop"] > 3 and R.random() < 0.6
# combo dùng giá vốn = tổng thành phần
def combo_cost(pid):
    if pid in COMBOS:
        return sum(ITEMS[c]["avgCost"] * q for c, q in COMBOS[pid])
    return ITEMS[pid]["avgCost"]


# ======================= KÊNH / SHOP =======================
SHOPS = {
    "shopee_hcm": dict(channel=42, shop_id="100200301", start=START),
    "shopee_hn": dict(channel=42, shop_id="100200302", start=START),
    "tiktok_main": dict(channel=48, shop_id="7300000000000000101", start=date(2024, 3, 15)),
    "tiktok_2": dict(channel=48, shop_id="7300000000000000102", start=date(2026, 6, 1)),
    "admin": dict(channel=1, shop_id=None, start=START),
}


def tet_offset(day):
    best = None
    for t in TET:
        o = (day - t).days
        if -40 <= o <= 20:
            best = o
    return best


def season(day, ch):
    m, dd = day.month, day.day
    f = [1.05, 1.03, 1.0, 0.98, 0.97, 0.95, 1.02][day.weekday()]
    f *= {1: 1.05, 2: 0.9, 3: 1.05, 4: 0.95, 5: 0.96, 6: 0.97, 7: 0.92, 8: 0.97, 9: 1.0, 10: 1.05, 11: 1.18, 12: 1.12}[m]
    if dd == m:
        f *= {9: 3.6, 10: 3.2, 11: 5.0, 12: 4.2}.get(m, 2.8)
    elif dd == m - 1 or dd == m + 1:
        f *= 1.35
    if dd == 15:
        f *= 1.45
    if dd == 25:
        f *= 1.55
    if (m, dd) in ((3, 8), (10, 20), (2, 14), (11, 20)):
        f *= 1.4
    if m == 11 and day.weekday() == 4 and dd >= 22 and dd + 7 > 30:
        f *= 1.8  # Black Friday
    o = tet_offset(day)
    if o is not None:
        if -28 <= o <= -9:
            f *= 1.28
        elif -8 <= o <= -4:
            f *= 0.85
        elif -3 <= o <= 4:
            f *= 0.22
        elif 5 <= o <= 10:
            f *= 0.8
    if ch == "tiktok" and R.random() < 0.06:
        f *= R.uniform(1.5, 2.6)  # phiên live lớn
    return f


def base_rate(shop, day):
    t = (day - date(2024, 1, 1)).days / 365.25
    if shop == "shopee_hcm":
        return 85 * (1 + 0.17 * t)
    if shop == "shopee_hn":
        return 22 * (1 + 0.28 * t)
    if shop == "tiktok_main":
        x = (day - SHOPS[shop]["start"]).days
        if x < 0:
            return 0
        return 3 + 52 * min(1.0, x / 960.0) ** 1.4
    if shop == "tiktok_2":
        x = (day - SHOPS[shop]["start"]).days
        return 0 if x < 0 else 2 + 8 * min(1.0, x / 150)
    if shop == "admin":
        return 2.6 if day.weekday() < 5 else 0.6
    return 0


HOUR_W = [3.5, 1.6, 0.8, 0.4, 0.3, 0.5, 1.2, 2.5, 4.2, 5.5, 6.0, 5.8, 5.2, 4.8, 4.6, 4.7, 4.6, 4.5, 4.8, 5.6, 6.6, 7.6, 7.8, 5.9]
HOUR_W_ADMIN = [0, 0, 0, 0, 0, 0, 0, 1, 5, 9, 9, 8, 4, 6, 8, 8, 7, 5, 2, 1, 0.5, 0.3, 0, 0]


def rand_time(day, admin=False, mega=False):
    if mega and R.random() < 0.25:
        h = R.choice([0, 0, 0, 1, 2])
    else:
        h = R.choices(range(24), HOUR_W_ADMIN if admin else HOUR_W)[0]
    return datetime(day.year, day.month, day.day, h, R.randint(0, 59), R.randint(0, 59))


def is_mega(day):
    return day.day == day.month or day.day in (15, 25)


# ======================= KHÁCH HÀNG =======================
CUSTOMERS = []          # list dict
CUST_ORDERS = {"shopee": [], "tiktok": [], "admin": []}  # danh sách cid lặp theo số đơn (preferential)
REPEAT_P = {"shopee": 0.24, "tiktok": 0.16, "admin": 0.55}


def new_customer(group, day):
    cid = R.choices(CITY_IDS, CITY_W)[0]
    wid = R.choice(WARDS_BY_CITY[cid])
    c = dict(idx=len(CUSTOMERS), group=group, name=rand_name(R), phone=rand_phone(R), cityId=cid, wardId=wid,
             address=rand_address(R, cid), addr_id=R.randint(60000000, 160000000),
             shopee_uid=R.randint(10000000, 1600000000), first=day)
    c["location"] = f"{WARDS[wid][1]}, {('Thành phố ' if cid in (254, 255, 256, 257, 258, 259) else 'Tỉnh ')}{CITY_NAME[cid]}"
    if group == "admin":
        c["name"] = R.choice(["Shop ", "Cửa hàng ", "Đại lý ", "Spa ", "Salon ", "Công ty TNHH "]) + c["name"].split()[-1] + " " + R.choice(["Beauty", "Store", "Cosmetics", "Mart", "House"])
    CUSTOMERS.append(c)
    return c


def pick_customer(group, day):
    pool = CUST_ORDERS[group]
    if pool and R.random() < REPEAT_P[group]:
        c = CUSTOMERS[R.choice(pool)]
    else:
        c = new_customer(group, day)
    pool.append(c["idx"])
    return c


# ======================= TỒN KHO =======================
class Inv:
    def __init__(self):
        self.remain = defaultdict(float)     # (depot, pid)
        self.holding = defaultdict(float)
        self.shipping = defaultdict(float)
        self.damaged = defaultdict(float)
        self.transfering = defaultdict(float)  # tại kho đích: hàng đang về
        self.demand = defaultdict(float)     # EWMA nhu cầu/ngày (depot,pid) gồm cả nhu cầu bị mất
        self.today_dem = defaultdict(float)
        self.on_order = defaultdict(float)   # PO đang về RAW

    def avail(self, dep, pid):
        return self.remain[(dep, pid)] - self.holding[(dep, pid)]


INV = Inv()
EVENTS = defaultdict(list)   # day -> list of (sort_ts, kind, payload)
BILL_ROWS = defaultdict(list)  # day -> list of bill line dicts
SEQ = dict(order=300_000_000, oprod=210_000_000, bill=150_000_000, billline=420_000_000, req=5_200_000,
           handover=1_100_000, tt=576_100_000_000_000_000, review=90_000_000_000, model=900_000_000_000)


def nxt(k, lo=1, hi=1):
    SEQ[k] += R.randint(lo, hi)
    return SEQ[k]


def add_bill(day, tstamp, btype, mode, depot, lines, order_id=0, req_id=0, related=0, desc=None):
    """lines: list (pid, qty, productPrice, discount)"""
    bill_id = nxt("bill", 1, 40)
    for pid, qty, price, disc in lines:
        p = ITEMS[pid]
        r = {
            "id": nxt("billline", 1, 25), "billId": bill_id, "type": btype, "mode": mode, "date": day.isoformat(),
            "reference": {"orderId": order_id, "requirementBillId": req_id, "inventoryCheckId": 0, "warrantyBillId": 0},
            "depot": {"id": depot, "name": DEPOT_INFO[depot]["name"]},
            "relatedDepot": ({"id": related, "name": DEPOT_INFO[related]["name"]} if related else {"id": 0}),
            "supplier": {"id": 0},
            "product": {"id": pid, "code": p["code"], "barcode": p.get("barcode"), "name": p["name"],
                        "prices": {"retail": retail_at(pid, day), "import": int(p["lastImport"]), "avgCost": int(round(p["avgCost"]))}},
            "imei": {"id": 0}, "batch": {"id": 0}, "avgCost": int(round(p["avgCost"])), "productPrice": price,
            "quantity": qty, "vat": 0, "vatPercent": 0, "discount": disc, "points": 0, "usedPoints": 0,
            "usedPointAmount": 0, "createdAt": tstamp, "saleBonus": 0,
            "extendedWarranty": {"id": 0, "amount": 0, "month": 0}, "warrantyReasonId": 0,
        }
        if desc:
            r["description"] = desc
        BILL_ROWS[day].append(r)


def receive_purchase(day, pid, qty, unit_cost):
    p = ITEMS[pid]
    tot_q = sum(INV.remain[(dp, pid)] for dp in ALL_DEPOTS)
    tot_q = max(tot_q, 0)
    p["avgCost"] = (p["avgCost"] * tot_q + unit_cost * qty) / (tot_q + qty) if (tot_q + qty) > 0 else unit_cost
    p["lastImport"] = unit_cost
    INV.remain[(RAW, pid)] += qty
    INV.on_order[pid] = max(0, INV.on_order[pid] - qty)
    t = ts(datetime(day.year, day.month, day.day, R.randint(8, 16), R.randint(0, 59)))
    add_bill(day, t, 1, 10, RAW, [(pid, qty, int(unit_cost), 0)], desc="Nhập hàng từ xưởng/NCC")


def components_universe():
    s = set()
    for d in ITEMS.values():
        if d["sellable"]:
            for c, _ in comp_of(d["id"]):
                s.add(c)
    return sorted(s)


COMP_UNIVERSE = components_universe()

# ======================= ĐỘ PHỔ BIẾN THEO NGÀY =======================
SELLABLE = [d for d in ITEMS.values() if d["sellable"]]


def weight_on(d, day, di, shop):
    if day < d["launch_day"]:
        return 0.0
    if d["disc_day"] and day >= d["disc_day"]:
        return 0.0
    if shop.startswith("tiktok") and not d["tiktok"]:
        return 0.0
    if shop == "shopee_hn" and not d["is_tmr"]:
        return 0.0
    age = (day - d["launch_day"]).days
    w = d["pop"] * min(1.0, 0.15 + age / 50.0)
    if not d["is_tmr"]:
        w *= math.exp(-max(0, (day - START).days) / 140.0)
    w *= math.exp(d["walk"])
    for s0, ln, mult in d["viral"]:
        if s0 <= di < s0 + ln:
            w *= mult if shop.startswith("tiktok") else 1 + (mult - 1) * 0.35
    if d["type"] == 9:
        o = tet_offset(day)
        if (o is not None and -30 <= o <= -5) or (day.month, day.day) in ((3, 8), (10, 20), (2, 14)) or day.month == 12:
            w *= 2.2
    return w


# ======================= ĐƠN HÀNG =======================
ORDERS = []
STAFF = {HCM: [(3177470, "Trần Thu Hà"), (3228752, "Lê Minh Tâm"), (3301245, "Phạm Ngọc Hân")],
         HN: [(3190033, "Nguyễn Văn Hùng"), (3311420, "Đỗ Thị Hoa")], FBS: [(0, None)], RAW: [(3177470, "Trần Thu Hà")]}
SALES = [(2793499, "Võ Thanh Trúc"), (3242016, "Ngô Gia Bảo"), (3032752, "Huỳnh Mỹ Linh"), (3649200, "Đặng Quốc Huy")]
CARRIERS = {
    "spx": (29, "SPX Express"), "jt": (24, "JT Express"), "ghn": (5, "Giaohangnhanh"),
    "self": (12, "Tự Vận Chuyển"), "vtp": (8, "Viettel Post"), "aha": (30, "Ahamove"),
}
DAILY_INDEX = defaultdict(int)
HANDOVER = {}


def fee_rate(shop, day):
    t = (day - date(2024, 1, 1)).days / 365.25
    if shop.startswith("shopee"):
        return min(0.40, 0.17 + 0.065 * t + R.uniform(-0.02, 0.025))
    if shop.startswith("tiktok"):
        return min(0.09, 0.028 + 0.006 * t + R.uniform(-0.005, 0.008))
    return 0.0


def carrier_code(rnd, key):
    if key == "spx":
        return "SPXVN0" + "".join(rnd.choice("0123456789") for _ in range(10)) + rnd.choice("AB")
    if key == "jt":
        return "86" + "".join(rnd.choice("0123456789") for _ in range(10))
    if key == "ghn":
        return "G" + "".join(rnd.choice("ABCDEFGHJKLMNPQRSTUVWXY0123456789") for _ in range(7))
    if key == "vtp":
        return "VTP" + "".join(rnd.choice("0123456789") for _ in range(9))
    if key == "aha":
        return "AHA" + "".join(rnd.choice("0123456789") for _ in range(8))
    return None


def plan_timeline(o, t0, day):
    """Sinh dòng thời gian trạng thái: list (ts, status, extra)."""
    shop = o["shop"]
    tl = [(t0, 54)]
    if shop == "admin":
        tl.append((t0 + R.randint(60, 1800), 56))
    elif shop.startswith("tiktok") and R.random() < 0.15:
        tl.append((t0 + R.randint(60, 900), 57))
        tl.append((t0 + R.randint(900, 4200), 56))
    else:
        tl.append((t0 + R.randint(120, 3600), 56))
    # kết cục
    q = o["quality"]
    if shop.startswith("shopee"):
        pc, ps, pcc, pr = 0.075, 0.02, 0.004, 0.042
    elif shop.startswith("tiktok"):
        pc, ps, pcc, pr = 0.10, 0.025, 0.004, 0.06
    else:
        pc, ps, pcc, pr = 0.02, 0.0, 0.0, 0.01
    pr *= max(0.5, 1 + 4 * (0.86 - q))
    x = R.random()
    if x < pc:
        outcome = "cancel"
    elif x < pc + ps:
        outcome = "system"
    elif x < pc + ps + pcc:
        outcome = "carrier_cancel"
    elif x < pc + ps + pcc + pr:
        outcome = "return"
    else:
        outcome = "success"
    o["outcome"] = outcome
    if outcome == "cancel":
        tl.append((tl[-1][0] + R.randint(300, 3600 * 20), 63))
        return tl
    if outcome == "system":
        tl.append((tl[-1][0] + R.randint(3600, 3600 * 48), 64))
        return tl
    # đóng gói: ca làm 8h-17h, kho nghỉ Tết
    dt0 = from_ts(t0)
    pk = dt0 + timedelta(hours=R.uniform(1.5, 6))
    if pk.hour >= 17 or pk.hour < 8:
        nd = (pk + timedelta(days=1 if pk.hour >= 17 else 0)).date()
        pk = datetime(nd.year, nd.month, nd.day, 8, R.randint(0, 59)) + timedelta(hours=R.uniform(0, 4))
    o_ = tet_offset(pk.date())
    if o_ is not None and -3 <= o_ <= 4:
        nd = pk.date() + timedelta(days=5 - o_)
        pk = datetime(nd.year, nd.month, nd.day, 8, 0) + timedelta(hours=R.uniform(0, 8))
    if pk.weekday() == 6 and R.random() < 0.6:
        pk += timedelta(days=1)
    t_pack0 = ts(pk)
    tl.append((t_pack0, 42))
    t_pack = t_pack0 + R.randint(300, 3600 * 2)
    tl.append((t_pack, 43))
    o["packedAt"] = t_pack
    if outcome == "carrier_cancel":
        tl.append((t_pack + R.randint(7200, 3600 * 30), 58))
        return tl
    t_ship = t_pack + R.randint(1800, 3600 * 7)
    tl.append((t_ship, 59))
    o["shipAt"] = t_ship
    cust_city, dep_city = o["cust"]["cityId"], DEPOT_CITY[o["depot"]]
    if cust_city == dep_city:
        dd = R.uniform(0.6, 2.2)
    elif (cust_city in NORTH) == (dep_city in NORTH) and cust_city not in CENTRAL:
        dd = R.uniform(1.5, 3.5)
    else:
        dd = R.uniform(2.5, 5.5)
    t_deliv = t_ship + int(dd * 86400)
    dl = from_ts(t_deliv)
    if dl.hour < 8 or dl.hour > 20:
        dl = dl.replace(hour=R.randint(9, 19))
    od = tet_offset(dl.date())
    if od is not None and -4 <= od <= 5:
        dl += timedelta(days=6 - od)
    t_deliv = ts(dl)
    o["deliveryAt"] = t_deliv
    if outcome == "success":
        tl.append((t_deliv + (R.randint(0, 3600 * 30) if shop.startswith("shopee") else R.randint(0, 3600 * 6)), 60))
        return tl
    # hoàn hàng
    t = t_deliv
    if R.random() < 0.6:
        tl.append((t, 61))
        t += R.randint(3600 * 6, 3600 * 40)
    tl.append((t, 71))
    t += R.randint(86400 * 2, 86400 * 8)
    tl.append((t, 72))
    o["returnAt"] = t
    if R.random() < 0.7:
        t += R.randint(3600 * 10, 86400 * 6)
        tl.append((t, 74))
    return tl


def price_line(o, d, qty, day, mega):
    retail = retail_at(d["id"], day)
    shop = o["shop"]
    camp = 1.0
    if mega:
        camp = R.choice([0.9, 0.92, 0.95, 1.0])
    elif R.random() < 0.08:
        camp = R.choice([0.85, 0.9, 0.95])
    sale = CAT.round_price(retail * camp) if camp < 1 else retail
    line = dict(pid=d["id"], qty=qty)
    if shop.startswith("shopee"):
        orig = CAT.round_price(retail * d["list_mult"]) * qty
        total = sale * qty
        price = sale
        if qty >= 2 and R.random() < 0.35:
            total = int(round(total * 0.95, -2))
            price = round(total / qty, 2)
            if price == int(price):
                price = int(price)
        line.update(originalPrice=orig, price=price, discount=0, total=total, priceVatMode=1)
    elif shop.startswith("tiktok"):
        lp = CAT.round_price(retail * d["tt_mult"])
        disc = max(0, lp - sale)
        line.update(originalPrice=0, price=lp, discount=disc, total=(lp - disc) * qty, priceVatMode=1)
    else:
        if o["kind"] == "wholesale":
            wp = int(round(retail * R.choice([0.55, 0.6, 0.65]), -2))
            line.update(originalPrice=0, price=wp, discount=0, total=wp * qty, priceVatMode=2)
        else:
            line.update(originalPrice=0, price=retail, discount=0, total=retail * qty, priceVatMode=2)
    return line


def make_order(shop, day, di, cdf_cache):
    cfg = SHOPS[shop]
    mega = is_mega(day)
    t0 = ts(rand_time(day, admin=(shop == "admin"), mega=mega))
    group = "shopee" if shop.startswith("shopee") else ("tiktok" if shop.startswith("tiktok") else "admin")
    o = dict(id=nxt("order", 600, 1800), shop=shop, channel=cfg["channel"], shop_id=cfg["shop_id"], t0=t0, day=day,
             packedAt=0, shipAt=0, deliveryAt=0, returnAt=0, kind="retail")
    # kho xuất
    if shop == "shopee_hcm":
        o["depot"] = HCM
    elif shop == "shopee_hn":
        o["depot"] = HN
    elif shop.startswith("tiktok"):
        o["depot"] = HCM if R.random() < 0.92 else HN
    else:
        o["depot"] = HCM if R.random() < 0.75 else HN
        o["kind"] = R.choices(["wholesale", "giao_bu", "retail"], [0.35, 0.40, 0.25])[0]
    o["cust"] = pick_customer(group, day)
    # dòng hàng
    lines = []
    if o["kind"] == "giao_bu":
        gb = GIAO_BU if GIAO_BU and R.random() < 0.7 else ITEMS[36224379]
        lines.append(dict(pid=gb["id"], qty=1, originalPrice=0, price=1000, discount=1000, total=0, priceVatMode=2))
    else:
        keys, cdf = cdf_cache[shop if shop != "admin" else "shopee_hcm"]
        if not keys:
            return None
        nl = R.choices([1, 2, 3, 4], [0.84, 0.12, 0.03, 0.01])[0] if o["kind"] != "wholesale" else R.randint(2, 7)
        chosen = []
        tries = 0
        fbs_try = shop == "shopee_hcm" and day >= FBS_START and R.random() < min(0.35, 0.12 + (day - FBS_START).days / 2000)
        while len(chosen) < nl and tries < nl * 6:
            tries += 1
            if chosen and R.random() < 0.5:
                g = ITEMS[chosen[0]]["group"]
                same = [k for k in keys if ITEMS[k]["group"] == g and k not in chosen]
                pid = R.choice(same) if same else keys[min(len(keys) - 1, bisect.bisect(cdf, R.random() * cdf[-1]))]
            else:
                pid = keys[min(len(keys) - 1, bisect.bisect(cdf, R.random() * cdf[-1]))]
            if pid in chosen:
                continue
            # nhu cầu (kể cả bị mất do hết hàng) để ra quyết định bổ sung
            dep = o["depot"]
            if dep == FBS and not ITEMS[pid]["fbs"]:
                continue
            if fbs_try and not chosen and ITEMS[pid]["fbs"] and all(INV.avail(FBS, c) >= q for c, q in comp_of(pid)):
                dep = FBS
            for c, q in comp_of(pid):
                INV.today_dem[(dep, c)] += q
            if any(INV.avail(dep, c) < q for c, q in comp_of(pid)):
                if R.random() < 0.9 and shop != "admin":
                    continue  # sàn hiển thị hết hàng -> khách không đặt được
            if dep == FBS:
                o["depot"] = FBS
            chosen.append(pid)
        if not chosen:
            return None
        for pid in chosen:
            if o["kind"] == "wholesale":
                qty = R.choice([10, 12, 20, 24, 30, 50, 100])
            else:
                qty = R.choices([1, 2, 3, 4, 5], [0.88, 0.08, 0.025, 0.008, 0.007])[0]
            lines.append(price_line(o, ITEMS[pid], qty, day, mega))
        # quà tặng 0đ dịp campaign
        if mega and shop.startswith("shopee") and R.random() < 0.04:
            gifts = [d for d in ITEMS.values() if d["is_gift"] and d["type"] == 1 and d["code"]]
            if gifts:
                g = R.choice(gifts)
                lines.append(dict(pid=g["id"], qty=1, originalPrice=0, price=0, discount=0, total=0, priceVatMode=1))
    for l in lines:
        l["opid"] = nxt("oprod", 1, 3)
        l["avgCost"] = int(round(combo_cost(l["pid"])))
    o["lines"] = lines
    o["traffic"] = R.choice(["Khac", "Khac", "Khac", "SI", "VNSHOP", "Zalo", "Facebook"])
    o["desc"] = R.choice(["Gọi trước khi giao", "Giao giờ hành chính", "Hàng dễ vỡ, đóng gói kỹ", "Cho xem hàng"]) if R.random() < 0.1 else None
    o["ship_fee_noise"] = R.uniform(0, 8000)
    o["quality"] = min(ITEMS[l["pid"]]["quality"] for l in lines)
    # giữ hàng
    o["reserve"] = defaultdict(float)
    for l in lines:
        if ITEMS[l["pid"]]["is_internal"]:
            continue
        for c, q in comp_of(l["pid"]):
            o["reserve"][c] += q * l["qty"]
    for c, q in o["reserve"].items():
        INV.holding[(o["depot"], c)] += q
    o["timeline"] = plan_timeline(o, t0, day)
    # sự kiện kho
    for t, st in o["timeline"]:
        if st in (59, 63, 64, 58, 72):
            EVENTS[day_of(t)].append((t, st, o))
    if shop.startswith("shopee"):
        o["sn"] = shopee_sn(R, day)
        o["paid_online"] = R.random() < (0.5 + 0.08 * (day.year - 2024))
    elif shop.startswith("tiktok"):
        o["sn"] = str(nxt("tt", 10 ** 9, 9 * 10 ** 10))
        o["paid_online"] = R.random() < 0.45
    else:
        o["sn"] = None
        o["paid_online"] = o["kind"] == "wholesale"
    o["fee_rate"] = fee_rate(shop, day)
    if shop.startswith("shopee"):
        o["carrier"] = R.choices(["spx", "ghn", "jt"], [0.72, 0.16, 0.12])[0]
    elif shop.startswith("tiktok"):
        o["carrier"] = R.choices(["jt", "ghn", "spx"], [0.72, 0.18, 0.10])[0]
    else:
        o["carrier"] = R.choices(["spx", "self", "vtp", "aha"], [0.5, 0.25, 0.15, 0.1])[0]
    o["ccode"] = carrier_code(R, o["carrier"])
    o["staff_pack"] = R.choice(STAFF[o["depot"]])
    o["sale"] = R.choice(SALES) if (shop == "admin" or R.random() < 0.5) else (0, None)
    DAILY_INDEX[(day, o["depot"])] += 1
    o["orderIndex"] = DAILY_INDEX[(day, o["depot"])]
    o["note"] = None
    if shop == "admin":
        if o["kind"] == "giao_bu" and ORDERS:
            ref = R.choice(ORDERS[-3000:])
            o["note"] = f"Giao bù đơn {ref['sn'] or ref['id']}"
        elif R.random() < 0.25:
            o["note"] = f"- Copy từ đơn hàng {SEQ['order'] - R.randint(1000, 900000)}"
        elif o["kind"] == "wholesale":
            o["note"] = "Đã thanh toán"
    ORDERS.append(o)
    return o


def process_event(day, t, st, o):
    dep = o["depot"]
    if st == 59:
        # kiểm tra tồn; thiếu -> đơn chuyển "Hết hàng" (68)
        if any(INV.remain[(dep, c)] < q for c, q in o["reserve"].items()) and o["shop"] != "admin":
            for c, q in o["reserve"].items():
                INV.holding[(dep, c)] -= q
            o["timeline"] = [x for x in o["timeline"] if x[0] < t and x[1] not in (59, 60, 61, 71, 72, 74)]
            o["timeline"].append((t, 68))
            o["outcome"] = "soldout"
            o["shipAt"] = o["deliveryAt"] = o["returnAt"] = 0
            return
        mode = 19 if o["shop"].startswith("shopee") else 1
        bl = []
        for l in o["lines"]:
            p = ITEMS[l["pid"]]
            if p["is_internal"]:
                continue
            comps = comp_of(l["pid"])
            base = sum(retail_at(c, day) * q for c, q in comps) or 1
            for c, q in comps:
                share = retail_at(c, day) * q / base
                unit_price = int(round((l["price"] if isinstance(l["price"], (int, float)) else 0) * share / q))
                unit_disc = int(round(l["discount"] * share / q))
                bl.append((c, q * l["qty"], unit_price, unit_disc))
        for c, q in o["reserve"].items():
            INV.holding[(dep, c)] -= q
            INV.remain[(dep, c)] -= q
            INV.shipping[(dep, c)] += q
        if bl:
            add_bill(day, t, 2, mode, dep, bl, order_id=o["id"])
        # giao xong / hoàn -> giảm "đang giao"
        end_t = o["returnAt"] or o["deliveryAt"]
        EVENTS[day_of(end_t)].append((end_t, "delivered", o))
    elif st in (63, 64, 58):
        for c, q in o["reserve"].items():
            INV.holding[(dep, c)] -= q
    elif st == "delivered":
        for c, q in o["reserve"].items():
            INV.shipping[(dep, c)] -= q
    elif st == 72:
        if o.get("outcome") != "return":
            return
        bl = [(c, int(q), int(round(ITEMS[c]["avgCost"])), 0) for c, q in o["reserve"].items()]
        for c, q in o["reserve"].items():
            INV.remain[(dep, c)] += q
        if bl:
            add_bill(day, t, 1, 1, dep, bl, order_id=o["id"], desc="Nhập hàng hoàn")


# ---------- khởi tạo tồn kho ----------
def init_stock():
    d0 = START
    tot = defaultdict(float)
    for shop in ("shopee_hcm", "shopee_hn", "tiktok_main"):
        lam = base_rate(shop, d0 + timedelta(days=60)) * args.scale
        ws = {d["id"]: weight_on(d, d0 + timedelta(days=30), 30, shop) for d in SELLABLE}
        s = sum(ws.values()) or 1
        dep = HN if shop == "shopee_hn" else HCM
        for pid, w in ws.items():
            for c, q in comp_of(pid):
                tot[(dep, c)] += lam * w / s * q * 1.12
    for (dep, c), v in tot.items():
        INV.demand[(dep, c)] = v
        INV.remain[(dep, c)] = math.ceil(v * R.uniform(14, 30))
    for c in COMP_UNIVERSE:
        dsum = sum(INV.demand[(dp, c)] for dp in SALE_DEPOTS)
        INV.remain[(RAW, c)] = math.ceil(dsum * R.uniform(40, 80) / 10) * 10 + (50 if ITEMS[c]["is_tmr"] else 0)


def replenish(day):
    dow = day.weekday()
    o = tet_offset(day)
    closed = o is not None and -3 <= o <= 4
    # cập nhật EWMA nhu cầu
    keys = set(INV.demand) | set(INV.today_dem)
    for k in keys:
        INV.demand[k] = 0.93 * INV.demand[k] + 0.07 * INV.today_dem.get(k, 0)
    INV.today_dem.clear()
    if closed:
        return
    # chuyển kho RAW -> kho bán (T2-T7)
    if dow < 6:
        req = nxt("req", 1, 6)
        sends = defaultdict(list)
        for c in COMP_UNIVERSE:
            for dep in SALE_DEPOTS:
                if dep == FBS and (day < FBS_START or not ITEMS[c]["fbs"]):
                    continue
                dmd = INV.demand[(dep, c)]
                if dmd < 0.05:
                    continue
                pos = INV.avail(dep, c) + INV.transfering[(dep, c)]
                rop = dmd * (12 if dep == HN else 8) + 2
                if pos < rop:
                    q = math.ceil(dmd * (24 if dep == HN else 18) / 10) * 10
                    q = int(min(q, max(0, INV.remain[(RAW, c)] - INV.holding[(RAW, c)])))
                    if q >= 5:
                        sends[dep].append((c, q))
        for dep, lst in sends.items():
            t = ts(datetime(day.year, day.month, day.day, R.randint(9, 15), R.randint(0, 59)))
            lag = 3 if dep == HN else (1 if R.random() < 0.6 else 0)
            for c, q in lst:
                INV.remain[(RAW, c)] -= q
                INV.transfering[(dep, c)] += q
            add_bill(day, t, 2, 3, RAW, [(c, q, int(round(ITEMS[c]["avgCost"])), 0) for c, q in lst], req_id=req, related=dep)
            if lag == 0:
                handle_special(day, t + 5400, "arrive", (dep, lst, req))
            else:
                EVENTS[day + timedelta(days=lag)].append((t + lag * 86400 + 3600, "arrive", (dep, lst, req)))
    # đặt hàng nhập RAW (thứ Hai)
    if dow == 0:
        pre_tet = any(0 <= (t_ - day).days <= 75 for t_ in TET)
        for c in COMP_UNIVERSE:
            p = ITEMS[c]
            if not p["sellable"] and not any(c in [x for x, _ in COMBOS.get(k, [])] for k in COMBOS):
                continue
            if p["disc_day"] and day >= p["disc_day"] - timedelta(days=30):
                continue
            dsum = sum(INV.demand[(dp, c)] for dp in SALE_DEPOTS)
            if dsum < 0.03:
                continue
            pos = sum(INV.remain[(dp, c)] for dp in ALL_DEPOTS) + INV.on_order[c] + sum(INV.transfering[(dp, c)] for dp in SALE_DEPOTS)
            cover = 55 * (1.5 if pre_tet else 1.0)
            if pos < dsum * cover:
                q = math.ceil(dsum * 90 * (1.5 if pre_tet else 1.0) / 100) * 100
                lead = R.randint(18, 42) if p["is_tmr"] else R.randint(7, 20)
                arr = day + timedelta(days=lead)
                ot = tet_offset(arr)
                if ot is not None and -3 <= ot <= 4:
                    arr += timedelta(days=6)
                infl = 1 + 0.03 * (day - START).days / 365.25
                cost = int(round((p["imp"] or p["cost"] or 1000) * infl * R.uniform(0.93, 1.07)))
                INV.on_order[c] += q
                EVENTS[arr].append((0, "po", (c, q, cost)))
    # điều chỉnh/hủy hàng lỗi định kỳ (mode 10)
    if dow == 4 and R.random() < 0.7:
        dep = R.choice([HCM, HN, RAW])
        cand = [c for c in COMP_UNIVERSE if INV.remain[(dep, c)] > 20]
        if cand:
            lst = [(c, R.randint(1, 5), int(round(ITEMS[c]["avgCost"])), 0) for c in R.sample(cand, min(len(cand), R.randint(1, 4)))]
            for c, q, _, _ in lst:
                INV.remain[(dep, c)] -= q
            t = ts(datetime(day.year, day.month, day.day, 16, R.randint(0, 59)))
            add_bill(day, t, 2, 10, dep, lst, desc=R.choice(["Xuất hủy hàng lỗi", "Xuất mẫu chụp hình/KOL", "Điều chỉnh kiểm kho"]))
    if day.day == 28:
        # kiểm kho cuối tháng: chênh lệch nhỏ
        for dep in (HCM, HN):
            cand = [c for c in COMP_UNIVERSE if INV.remain[(dep, c)] > 30]
            for c in R.sample(cand, min(len(cand), 3)):
                q = R.randint(1, 4)
                t = ts(datetime(day.year, day.month, day.day, 17, R.randint(0, 59)))
                if R.random() < 0.5:
                    INV.remain[(dep, c)] += q
                    add_bill(day, t, 1, 10, dep, [(c, q, int(round(ITEMS[c]["avgCost"])), 0)], desc="Điều chỉnh kiểm kho (thừa)")
                else:
                    INV.remain[(dep, c)] -= q
                    add_bill(day, t, 2, 10, dep, [(c, q, int(round(ITEMS[c]["avgCost"])), 0)], desc="Điều chỉnh kiểm kho (thiếu)")


def handle_special(day, t, kind, payload):
    if kind == "arrive":
        dep, lst, req = payload
        for c, q in lst:
            INV.transfering[(dep, c)] -= q
            INV.remain[(dep, c)] += q
        add_bill(day, t, 1, 3, dep, [(c, q, int(round(ITEMS[c]["avgCost"])), 0) for c, q in lst], req_id=req, related=RAW)
    elif kind == "po":
        c, q, cost = payload
        receive_purchase(day, c, q, cost)


# ======================= SNAPSHOT TỒN KHO =======================
def inv_snapshot(day):
    rows = []
    for d in sorted(ITEMS.values(), key=lambda x: -x["id"]):
        if day_of(d["createdAt"]) > day:
            continue
        pid = d["id"]
        deps = []
        tot = dict(remain=0, shipping=0, damaged=0, holding=0, transfering=0, available=0)
        for dep in ALL_DEPOTS:
            if dep == FBS and day < FBS_START:
                continue
            if d["type"] == 1 and not d["is_internal"]:
                rm = int(INV.remain[(dep, pid)])
                hd = int(INV.holding[(dep, pid)])
                sh = int(INV.shipping[(dep, pid)])
                tr = int(INV.transfering[(dep, pid)])
            else:
                rm = hd = sh = tr = 0
            e = dict(id=dep, remain=rm, shipping=sh, damaged=0, holding=hd, transfering=tr, available=rm - hd, warranty={"remain": 0})
            deps.append(e)
            for k in tot:
                tot[k] += e[k]
        rows.append({"productId": pid, "barcode": d.get("barcode"), "name": d["name"],
                     "prices": {"price": retail_at(pid, day), "import": int(d["lastImport"]), "wholesale": 0,
                                "avgCost": int(round(combo_cost(pid)))},
                     "inventory": dict(tot, warranty={"remain": 0, "holding": 0}, depots=deps)})
    return rows


# ======================= VÒNG LẶP MÔ PHỎNG =======================
init_stock()
# phiếu "tồn đầu kỳ" để tồn kho = tổng nhập - tổng xuất (đối soát được)
for dep in ALL_DEPOTS:
    lst = [(c, int(INV.remain[(dep, c)]), int(round(ITEMS[c]["avgCost"])), 0) for c in COMP_UNIVERSE if INV.remain[(dep, c)] > 0]
    if lst:
        add_bill(START, ts(datetime(START.year, START.month, START.day, 0, 5)), 1, 10, dep, lst, desc="Tồn đầu kỳ")
print(f"Mô phỏng {START} -> {END} ({NDAYS} ngày)...", flush=True)
inv_rows_by_day = {}
for di, day in enumerate(daterange(START, END)):
    # random walk độ phổ biến (mỗi tuần)
    if day.weekday() == 0:
        for d in SELLABLE:
            d["walk"] = 0.85 * d["walk"] + R.gauss(0, 0.12)
    cdf_cache = {}
    for shop in ("shopee_hcm", "shopee_hn", "tiktok_main", "tiktok_2"):
        keys, cdf, acc = [], [], 0.0
        for d in SELLABLE:
            w = weight_on(d, day, di, shop)
            if w > 0:
                acc += w
                keys.append(d["id"])
                cdf.append(acc)
        cdf_cache[shop] = (keys, cdf)
    # sự kiện đầu ngày (hàng về)
    pending = EVENTS.pop(day, [])
    for t, k, p in sorted([e for e in pending if e[1] in ("arrive", "po")], key=lambda e: e[0]):
        handle_special(day, t, k, p)
    others = [e for e in pending if e[1] not in ("arrive", "po")]
    # đơn mới
    for shop in SHOPS:
        lam = base_rate(shop, day) * args.scale
        if lam <= 0:
            continue
        lam *= season(day, "tiktok" if shop.startswith("tiktok") else "x") * math.exp(R.gauss(0, 0.12))
        if shop == "admin":
            lam = base_rate(shop, day) * args.scale * (0.3 if tet_offset(day) is not None and -3 <= tet_offset(day) <= 4 else 1)
        n = 0
        # Poisson
        L = math.exp(-min(lam, 600))
        if lam < 30:
            k, p = 0, 1.0
            while True:
                p *= R.random()
                if p < L:
                    break
                k += 1
            n = k
        else:
            n = max(0, int(round(R.gauss(lam, math.sqrt(lam)))))
        for _ in range(n):
            make_order(shop, day, di, cdf_cache)
    # xử lý sự kiện trong ngày (kể cả sự kiện do đơn mới sinh ra)
    queue = others + EVENTS.pop(day, [])
    while queue:
        queue.sort(key=lambda e: e[0])
        for t, st, o in queue:
            if st in ("arrive", "po"):
                handle_special(day, t, st, o)
            else:
                process_event(day, t, st, o)
        queue = EVENTS.pop(day, [])
    replenish(day)
    run = datetime(day.year, day.month, day.day, 23, 5, R.randint(10, 30))
    batch = W.batch_id(run)
    rows = inv_snapshot(day)
    W.write("nhanh", "inventory", day.isoformat(), batch,
            [envelope("nhanh", "inventory", batch, run + timedelta(seconds=2 + i // 300), {"mode": "snapshot", "page": 1 + i // 20}, r)
             for i, r in enumerate(rows)])
    bl = BILL_ROWS.pop(day, [])
    if bl:
        run2 = run + timedelta(hours=R.randint(8, 9), minutes=R.randint(10, 30))
        b2 = W.batch_id(run2)
        W.write("nhanh", "inventory_bills", day.isoformat(), b2,
                [envelope("nhanh", "inventory_bills", b2, run2 + timedelta(seconds=2 + i // 200),
                          {"mode": "incremental", "day": day.isoformat(), "page": 1 + i // 100}, r) for i, r in enumerate(bl)])
    if di % 60 == 0:
        print(f"  {day} | đơn: {len(ORDERS):,} | KH: {len(CUSTOMERS):,}", flush=True)

print(f"Tổng đơn: {len(ORDERS):,}, khách: {len(CUSTOMERS):,}", flush=True)

# ======================= GHI ORDERS (nhiều phiên bản theo ngày cập nhật) =======================
STATUS_AFTER_PACK = {43, 58, 59, 60, 61, 68, 71, 72, 74}


def order_json(o, st, upd):
    shop = o["shop"]
    day = o["day"]
    packed = o["packedAt"] and upd >= o["packedAt"] and st != 68
    shipped = o["shipAt"] and upd >= o["shipAt"]
    deliv = o["deliveryAt"] if (o["deliveryAt"] and upd >= o["deliveryAt"] and st in (60, 61, 71, 72, 74)) else 0
    returned = st in (71, 72, 74)
    cancelled = st in (63, 64, 58, 68)
    total = sum(l["total"] for l in o["lines"])
    disc_amt = sum(l["discount"] * l["qty"] for l in o["lines"])
    weight = 0
    prods = []
    for l in o["lines"]:
        p = ITEMS[l["pid"]]
        w = int(p["weight"] or 0) * l["qty"]
        weight += w
        e = {"id": p["id"], "productId": p["id"], "orderProductId": l["opid"], "typeId": 0}
        if p["code"]:
            e["code"] = p["code"]
        if p.get("barcode"):
            e["barcode"] = p["barcode"]
        e.update({"name": p["name"], "weight": w, "quantity": l["qty"], "originalQuantity": l["qty"],
                  "originalPrice": l["originalPrice"], "displaySalePrice": l["price"], "displayVatAmount": 0,
                  "price": l["price"], "priceVatMode": l["priceVatMode"], "priceAfterVAT": l["price"] if l["total"] else 0,
                  "discount": l["discount"], "discountAmount": l["discount"],
                  "avgCost": l["avgCost"], "vat": 0, "vatPercent": 0,
                  "vatPercentSub": 0, "vatAmount": 0, "imeiId": 0, "imeiIds": [], "imeiCodes": [], "transactionFee": 0,
                  "paymentStatus": 0, "partialReturnStatus": 0, "usedPoints": 0, "usedPointAmount": 0})
        if not p["is_internal"]:
            e["image"] = p["avatar"]
        e["shippingWeight"] = str(w)
        e["totalAmount"] = l["total"]
        if p["id"] in COMBOS:
            e["combos"] = [{"id": c, "code": ITEMS[c]["code"], "name": ITEMS[c]["name"], "quantity": q} for c, q in COMBOS[p["id"]]]
        prods.append(e)
    fee = int(round(total * o["fee_rate"]))
    cust = o["cust"]
    info = {"id": o["id"], "type": 1, "depotId": o["depot"], "pickupDepotId": 0, "mode": 1 if shop == "admin" else 0,
            "exportType": 1, "originalOrderId": 0, "returnFromId": 0, "idReturn": 0,
            "createdById": o["sale"][0] if shop == "admin" else 0}
    if shop == "admin":
        info["createdByName"] = o["sale"][1]
    info["saleId"] = o["sale"][0]
    if o["sale"][0]:
        info["saleName"] = o["sale"][1]
    info.update({"technicalStaffId": 0, "packedById": o["staff_pack"][0] if packed else 0})
    if packed and o["staff_pack"][1]:
        info["packName"] = o["staff_pack"][1]
    info.update({"customerCareId": 0, "createdAt": o["t0"], "updatedAt": upd, "confirmedAt": 0,
                 "packedAt": o["packedAt"] if packed else 0})
    if o["note"]:
        info["privateDescription"] = o["note"]
    if shop == "admin" and o["desc"]:
        info["description"] = o["desc"]
    info.update({"orderIndex": o["orderIndex"], "status": st, "reason": 18 if st == 72 else 0})
    if shipped and o["depot"] != FBS:
        hk = (day_of(o["shipAt"]), o["depot"], o["carrier"])
        if hk not in HANDOVER:
            HANDOVER[hk] = nxt("handover", 1, 4)
        info["handoverIds"] = [HANDOVER[hk]]
    else:
        info["handoverIds"] = []
    info["tags"] = []
    if packed and o["ccode"]:
        info["trackingUrl"] = {"spx": "https://spx.vn/track?", "jt": "https://jtexpress.vn/vi/tracking?type=track&billcode=",
                               "ghn": "https://donhang.ghn.vn/?order_code=", "vtp": "https://viettelpost.com.vn/tra-cuu-hanh-trinh-don/?code=",
                               "aha": "https://ahamove.com/tracking/"}[o["carrier"]] + o["ccode"]
    else:
        info["trackingUrl"] = ""
    ch = {}
    if o["sn"]:
        ch["appOrderId"] = o["sn"]
    ch["saleChannel"] = o["channel"]
    if shop == "admin":
        ch["trafficSource"] = o["traffic"]
    ch.update({"fbAdsId": 0, "fbPsid": 0})
    if o["shop_id"]:
        ch["shopId"] = o["shop_id"]
    ch.update({"affiliate": {"value": 0, "bonus": 0, "bonusRate": 0}, "marketing": []})
    addr = {"id": cust["addr_id"] if shop == "admin" else 0, "name": cust["name"]}
    if shop == "admin":
        addr["mobile"] = cust["phone"]
    addr.update({"address": cust["address"], "cityId": cust["cityId"], "districtId": 0, "wardId": cust["wardId"],
                 "location": cust["location"]})
    cid, cname = CARRIERS[o["carrier"]] if packed or shop == "admin" else CARRIERS["self"]
    if not packed and shop.startswith("tiktok") and st in (56, 57):
        cid, cname = CARRIERS[o["carrier"]]
    car = {"id": cid, "name": cname,
           "service": ({"id": 186 if weight < 3000 else 188, "name": "Tiêu chuẩn" if weight < 3000 else "Hàng nặng", "type": 0,
                        "typeName": "Giao nhanh"} if shop == "admin" and o["carrier"] == "spx" else
                       {"id": 0, "type": 10 if shop.startswith("shopee") else (15 if shop.startswith("tiktok") else 0)})}
    if packed and o["ccode"]:
        car["carrierCode"] = o["ccode"]
    ship_fee = 0
    if shop == "admin" and o["carrier"] != "self":
        ship_fee = int(round(14000 + weight * 4 + o["ship_fee_noise"], -3))
    car.update({"checkGoods": 1 if shop == "admin" else 0, "isPartDelivery": 0, "isDeclaredFee": 1 if shop == "admin" else 0,
                "sendCarrierType": 1, "sendCarrierAt": o["shipAt"] if (shipped and shop == "admin") else 0,
                "deliveryAt": deliv, "declaredValue": 0, "declaredFee": 0, "weight": weight, "shipFee": ship_fee,
                "codFee": 0, "ecomFee": 0, "overWeightShipFee": 0, "returnFee": 0, "customerShipFee": 0,
                "carrierPaymentStatus": 2 if (st == 60 and upd - o["deliveryAt"] > 86400 * 7) else 0})
    if shop != "admin":
        car["codFee"] = fee if not returned else 0
        car["ecomFee"] = fee
    if returned:
        cod = 0
    elif shop == "admin":
        cod = total if o["kind"] == "retail" else 0
    else:
        cod = total
    if shop == "admin":
        bp = cod - ship_fee
    else:
        bp = 0 if returned else cod - fee
    pay = {"usedPoints": {"points": 0, "amount": 0}, "vat": {"amount": 0}, "discount": {"amount": disc_amt}}
    if shop == "admin" and o["kind"] == "giao_bu":
        pay["discount"]["percent"] = "100%"
    tr_amt = total if (shop == "admin" and o["kind"] == "wholesale") else 0
    pay.update({"deposit": {"amount": 0, "accountId": 0},
                "transfer": {"amount": tr_amt, "accountId": 764546 if shop == "admin" else 0},
                "credit": {"amount": 0, "accountId": 0}, "codAmount": cod, "businessPayment": bp, "businessPaymentStatus": 0})
    if tr_amt:
        pay["transfer"]["name"] = cust["name"]
    return {"info": info, "channel": ch, "shippingAddress": addr, "products": prods, "carrier": car, "payment": pay, "gifts": []}


def end_ts(day):
    return ts(datetime(day.year, day.month, day.day, 23, 59, 59))


print("Ghi orders...", flush=True)
orders_by_day = defaultdict(list)
END_TS = end_ts(END)
for o in ORDERS:
    tl = sorted([x for x in o["timeline"] if x[0] <= END_TS], key=lambda x: x[0])
    o["timeline_eff"] = tl
    # gom theo ngày
    last_by_day = {}
    for t, st in tl:
        last_by_day[day_of(t)] = (t, st)
    for d_, (t, st) in last_by_day.items():
        orders_by_day[d_].append((o, st, t))

for day in daterange(START, END):
    lst = orders_by_day.pop(day, [])
    if not lst:
        continue
    run = datetime(day.year, day.month, day.day, 23, 5, R.randint(5, 20))  # UTC = 06:05 sáng hôm sau giờ VN
    batch = W.batch_id(run)
    rows = []
    lst.sort(key=lambda x: x[2])
    for i, (o, st, t) in enumerate(lst):
        rows.append(envelope("nhanh", "orders", batch, run + timedelta(seconds=1 + i // 400),
                             {"mode": "incremental", "by": "updatedAt", "day": day.isoformat(), "page": 1 + i // 100},
                             order_json(o, st, t)))
    W.write("nhanh", "orders", day.isoformat(), batch, rows)
del orders_by_day

# ======================= MASTER SNAPSHOTS =======================
print("Ghi master snapshot...", flush=True)
snap_days = sorted({date(y, m, 1) for y in range(START.year, END.year + 1) for m in range(1, 13)
                    if START <= date(y, m, 1) <= END} | {END})


def product_json(d, day):
    pid = d["id"]
    out = {"id": pid, "parentId": -1}
    if d["code"]:
        out["code"] = d["code"]
    if d.get("barcode"):
        out["barcode"] = d["barcode"]
    st = 1
    if d["disc_day"] and day >= d["disc_day"]:
        st = 2
    out.update({"name": d["name"], "status": st, "priceVatMode": 2 if d["type"] == 9 else 0, "importPriceVatMode": 2 if d["type"] == 9 else 0})
    if d.get("categoryId"):
        out["categoryId"] = d["categoryId"]
    last_change = max([dd for dd, _ in PRICE_EVENTS[pid] if dd <= day] + [day_of(d["createdAt"])])
    out["prices"] = {"retail": retail_at(pid, day), "retailVat": 0, "import": int(d["lastImport"]), "importVat": 0, "old": 0,
                     "wholesale": 0, "avgCost": int(round(combo_cost(pid)))}
    out["images"] = {"avatar": d["avatar"], "others": []}
    out["warranty"] = []
    if d.get("brandId"):
        out["brandId"] = d["brandId"]
    out.update({"type": d["type"], "shipping": {"width": 0, "height": 0, "length": 0, "weight": int(d["weight"] or 0)},
                "units": {"list": []}})
    if pid in COMBOS:
        out["combos"] = [{"id": c, "quantity": q, "code": ITEMS[c]["code"], "name": ITEMS[c]["name"]} for c, q in COMBOS[pid]]
    out["inventory"] = {"remain": 0, "shipping": 0, "damaged": 0, "holding": 0, "transfering": 0, "available": 0,
                        "warranty": {"remain": 0, "holding": 0}, "depots": []}
    out.update({"attributes": [], "updatedAt": max(d["createdAt"], ts(datetime.combine(last_change, datetime.min.time())) + 36000),
                "createdAt": d["createdAt"], "tagIds": []})
    return out


for sd in snap_days:
    run = datetime(sd.year, sd.month, sd.day, 23, 5, R.randint(5, 20))
    prods = [d for d in sorted(ITEMS.values(), key=lambda x: -x["id"]) if day_of(d["createdAt"]) <= sd]
    for ent, rows in (
        ("products", [product_json(d, sd) for d in prods]),
        ("combos", [{"id": d["id"], "comboCode": d["code"], "comboName": d["name"], "avgCost": 0,
                     "comboItems": [{"id": c, "code": ITEMS[c]["code"], "name": ITEMS[c]["name"], "avgCost": 0,
                                     "price": retail_at(c, sd), "qty": q} for c, q in COMBOS[d["id"]]]}
                    for d in prods if d["id"] in COMBOS]),
        ("categories", M["categories"]),
        ("brands", M["brands"]),
        ("depots", [x for x in M["depots"] if not (x["id"] == FBS and sd < FBS_START)]),
    ):
        b = W.batch_id(run)
        W.write("nhanh", ent, sd.isoformat(), b,
                [envelope("nhanh", ent, b, run + timedelta(seconds=1), {"mode": "snapshot", "page": 1 + i // 100}, r)
                 for i, r in enumerate(rows)])
        run += timedelta(seconds=R.randint(1, 4))

# ======================= SHOPEE LISTINGS =======================
print("Shopee listings, performance, reviews...", flush=True)


def family_key(d):
    parts = (d["code"] or "").split("-")
    return "-".join(parts[:2]) if len(parts) >= 2 else d["code"]


LISTING_SUFFIX = {
    "Nhóm Cọ Foundation": "Tán Nền Mịn Không Vệt, Lông Mềm Không Rụng", "Nhóm Cọ": "Lông Mềm Mịn, Bám Phấn Tốt",
    "Nhóm Cọ Mắt": "Tán Phấn Mắt Chuyên Nghiệp", "Nhóm Cọ Môi": "Tán Son Mịn, Che Khuyết Điểm Đa Năng",
    "Nhóm Mút": "Mềm Mịn, Nở To Khi Ướt, Tán Kem Nền Tiệp Da", "Nhóm Lược": "Gỡ Rối Không Đau, Chải Tóc Bóng Mượt",
    "Nhóm Lô Cuốn Tóc": "Tạo Phồng Tự Nhiên, Không Cần Nhiệt", "Nhóm Dao Cạo Nhíp Kéo": "Thép Không Gỉ Sắc Bén An Toàn",
    "Nhóm Kẹp Mi": "Cong Mi Tự Nhiên Không Gãy Mi", "Nhóm Phụ Kiện Nhà Tắm": "Tạo Bọt Mịn, Làm Sạch Sâu",
    "Nhóm Vệ Sinh Cọ": "Làm Sạch Cọ Nhanh, Khô Thoáng", "Nhóm FL603 và SP304": "Inox 304 Cao Cấp Trộn Kem Nền",
}
LISTINGS = {}  # (shop, fam) -> listing dict
MODEL_OF = {}  # (shop, pid) -> (listing, model)
for shop in ("shopee_hcm", "shopee_hn"):
    fams = defaultdict(list)
    for d in SELLABLE:
        if shop == "shopee_hn" and not d["is_tmr"]:
            continue
        fams[family_key(d)].append(d)
    for fam, ds in fams.items():
        ds.sort(key=lambda x: -x["pop"])
        top = ds[0]
        base = top["name"]
        for pre in ("TMR ", "[Set 5] ", "Bộ ", "Set "):
            if base.startswith(pre):
                base = base[len(pre):]
        base = base.split(" - ")[0].split("(")[0].strip() or top["name"].replace("TMR ", "").strip()
        nm = base + (" TMR " if top["is_tmr"] else " ") + LISTING_SUFFIX.get(top["group"], "Chính Hãng")
        if shop == "shopee_hn":
            nm = nm.replace("TMR ", "TMR Chính Hãng ", 1)
        L = dict(shop=shop, fam=fam, item_id=R.randint(50_000_000_000, 59_999_999_999), name=nm[:120],
                 image=img_hash(R), created=min(x["launch_day"] for x in ds), models=[],
                 cr=R.uniform(0.025, 0.075), ctr=R.uniform(0.025, 0.07), browse=R.uniform(0.6, 1.6),
                 end=None if any(x["disc_day"] is None for x in ds) else max(x["disc_day"] for x in ds))
        for d in ds:
            mn = d["name"].split(" - ")[-1] if " - " in d["name"] else d["name"].replace("TMR ", "")[:40]
            mdl = dict(model_id=nxt("model", 1000, 99999), name=mn[:40], pid=d["id"], start=d["launch_day"], end=d["disc_day"])
            L["models"].append(mdl)
            MODEL_OF[(shop, d["id"])] = (L, mdl)
        LISTINGS[(shop, fam)] = L

# manual: category, product_category, shopee_listing_map
groups = M["groups"]
manual_days = sorted({START} | {date(y, m, 1) for y in range(START.year, END.year + 1) for m in (1, 4, 7, 10)
                                 if START < date(y, m, 1) <= END})
for md in manual_days:
    run = datetime(md.year, md.month, md.day, 2, R.randint(0, 59), R.randint(0, 59))
    gl = [g for g in groups if not (g["category_name"] == "Nhóm New" and md < date(2025, 7, 1))]
    b = f"manual-category-{run:%Y%m%dT%H%M%SZ}"
    W.write("manual", "category", md.isoformat(), b,
            [envelope("manual", "nhom", b, run, {"file": "danh_muc_san_pham.xlsx", "sheet": "nhom", "row": i + 2}, g)
             for i, g in enumerate(gl)])
    rows = []
    for d in sorted(ITEMS.values(), key=lambda x: x["code"] or "~"):
        if d["launch_day"] > md + timedelta(days=30):
            continue
        grp = d["group"]
        if md >= date(2025, 7, 1) and d["launch_day"] >= md - timedelta(days=120) and d["is_tmr"] and d["id"] % 2 == 0:
            grp = "Nhóm New"
        rec = {"sku_code": d["code"], "product_id": None if d["code"] else d["id"], "category_name": grp}
        rows.append(rec)
    b = f"manual-sku-{run:%Y%m%dT%H%M%SZ}"
    W.write("manual", "product_category", md.isoformat(), b,
            [envelope("manual", "sku", b, run, {"file": "danh_muc_san_pham.xlsx", "sheet": "sku", "row": i + 2}, r)
             for i, r in enumerate(rows)])
    rows = []
    for L in LISTINGS.values():
        if L["created"] > md + timedelta(days=30):
            continue
        for mdl in L["models"]:
            if mdl["start"] > md + timedelta(days=30):
                continue
            p = ITEMS[mdl["pid"]]
            rows.append({"shop_id": SHOPS[L["shop"]]["shop_id"], "item_id": L["item_id"], "item_name": L["name"],
                         "model_id": mdl["model_id"], "model_name": mdl["name"], "sku_code": p["code"], "product_id": p["id"]})
    b = f"manual-map-{run:%Y%m%dT%H%M%SZ}"
    W.write("manual", "shopee_listing_map", md.isoformat(), b,
            [envelope("manual", "shopee_listing_map", b, run, {"file": "shopee_listing_map.xlsx", "sheet": "map", "row": i + 2}, r)
             for i, r in enumerate(rows)])

# ---------- tổng hợp đơn Shopee theo listing/ngày ----------
# agg[(shop, item_id, day)] -> list of (o, line, mdl)
agg = defaultdict(list)
for o in ORDERS:
    if not o["shop"].startswith("shopee"):
        continue
    for l in o["lines"]:
        km = MODEL_OF.get((o["shop"], l["pid"]))
        if km:
            agg[(o["shop"], km[0]["item_id"], o["day"])].append((o, l, km[1]))


def status_at(o, t):
    s = 54
    for tt, st in o["timeline"]:
        if tt <= t:
            s = st
    return s


def perf_metrics(entries, crawl_t, day, L):
    placed_o, placed_u, placed_s, placed_b = set(), 0, 0, set()
    conf_o, conf_u, conf_s, conf_b = set(), 0, 0, set()
    paid_o, paid_u, paid_s, paid_b = set(), 0, 0, set()
    per_model = defaultdict(lambda: defaultdict(float))
    for o, l, mdl in entries:
        st = status_at(o, crawl_t)
        cuid = o["cust"]["idx"]
        placed_o.add(o["id"]); placed_u += l["qty"]; placed_s += l["total"]; placed_b.add(cuid)
        pm = per_model[mdl["model_id"]]
        pm["placed_units"] += l["qty"]; pm["placed_sales"] += l["total"]; pm["placed_buyers"] += 1
        if st not in (63, 64, 58, 68) and o["t0"] + 3 * 3600 <= crawl_t:
            conf_o.add(o["id"]); conf_u += l["qty"]; conf_s += l["total"]; conf_b.add(cuid)
            pm["confirmed_units"] += l["qty"]; pm["confirmed_sales"] += l["total"]; pm["confirmed_buyers"] += 1
        if o["paid_online"] and st != 64:
            paid_o.add(o["id"]); paid_u += l["qty"]; paid_s += l["total"]; paid_b.add(cuid)
            pm["paid_units"] += l["qty"]; pm["paid_sales"] += l["total"]; pm["paid_buyers"] += 1
    return (placed_o, placed_u, placed_s, placed_b, conf_o, conf_u, conf_s, conf_b, paid_o, paid_u, paid_s, paid_b, per_model)


def rr(a, b, n=4):
    return round(a / b, n) if b else 0


def perf_row(L, day, crawl_t, rnd):
    entries = agg.get((L["shop"], L["item_id"], day), [])
    (po, pu, ps, pb, co, cu, cs, cb, pdo, pdu, pds, pdb, per_model) = perf_metrics(entries, crawl_t, day, L)
    active = L["end"] is None or day < L["end"]
    sf = season(day, "x") ** 0.6
    nb = len(pb)
    uv = int(round(nb / max(0.01, L["cr"] * rnd.uniform(0.75, 1.3)))) if nb else 0
    uv += int(rnd.expovariate(1 / (L["browse"] * 3 * sf))) if active else 0
    uv = max(uv, nb)
    pv = int(round(uv * rnd.uniform(1.15, 1.8)))
    ctr = L["ctr"] * rnd.uniform(0.8, 1.2)
    clicks_u = int(round(uv * rnd.uniform(0.75, 0.95)))
    clicks = int(round(clicks_u * rnd.uniform(1.1, 1.4)))
    imp_u = int(round(clicks_u / ctr)) if clicks_u else (rnd.randint(0, 25) if active else 0)
    imp = int(round(imp_u * rnd.uniform(1.4, 1.9)))
    atc_b = min(uv, int(round(max(nb * rnd.uniform(1.4, 2.6), uv * rnd.uniform(0.04, 0.12))))) if uv else 0
    atc_u = int(round(atc_b * rnd.uniform(1.0, 1.35)))
    bounce = int(round(uv * rnd.uniform(0.04, 0.35)))
    search = int(round(clicks * rnd.uniform(0.3, 0.6)))
    likes = rnd.randint(0, 2) if uv > 20 and rnd.random() < 0.3 else 0
    label = "psd_product_status_normal" if active else "psd_label_deleted"
    if active and rnd.random() < 0.003:
        label = "psd_product_status_unlisted"
    models = None
    if len(L["models"]) > 1:
        models = []
        for mdl in L["models"]:
            if mdl["start"] > day:
                continue
            pm = per_model.get(mdl["model_id"], {})
            pu_, pb_ = int(pm.get("placed_units", 0)), int(pm.get("placed_buyers", 0))
            cb_, pdb_ = int(pm.get("confirmed_buyers", 0)), int(pm.get("paid_buyers", 0))
            atcb = pb_ + (rnd.randint(0, 3) if uv > 10 else 0)
            m_active = mdl["end"] is None or day < mdl["end"]
            models.append({"id": mdl["model_id"], "name": mdl["name"], "status": 1 if m_active else 0,
                           "display_tag_label": "psd_product_status_normal" if m_active else "psd_label_deleted",
                           "add_to_cart_units": atcb + (1 if atcb and rnd.random() < 0.3 else 0), "add_to_cart_buyers": atcb,
                           "placed_sales": pm.get("placed_sales", 0), "placed_units": pu_, "placed_buyers": pb_,
                           "paid_sales": pm.get("paid_sales", 0), "paid_units": int(pm.get("paid_units", 0)), "paid_buyers": pdb_,
                           "confirmed_sales": pm.get("confirmed_sales", 0), "confirmed_units": int(pm.get("confirmed_units", 0)),
                           "confirmed_buyers": cb_,
                           "paid_sales_per_buyers": round(pm.get("paid_sales", 0) / pdb_, 4) if pdb_ else 0,
                           "placed_to_paid_buyers_rate": rr(pdb_, pb_, 2), "placed_to_confirmed_buyers_rate": rr(cb_, pb_, 2),
                           "repeat_placed_order_rate": 0, "average_days_to_repeat_placed_order": 0, "repeat_paid_order_rate": 0,
                           "average_days_to_repeat_paid_order": 0, "repeat_confirmed_order_rate": 0,
                           "average_days_to_repeat_confirmed_order": 0})
    rep = rnd.uniform(0, 0.08) if nb >= 5 else 0
    data = {"id": L["item_id"], "name": L["name"], "image": L["image"], "status": 1 if active else 0,
            "display_tag_label": label, "unique_product_card_clicks": clicks_u, "product_card_clicks": clicks,
            "product_card_impressions": imp, "unique_product_card_impressions": imp_u, "ctr": rr(clicks_u, imp_u),
            "placed_order_conversion_rate": rr(len(po), clicks), "paid_order_conversion_rate": rr(len(pdo), clicks),
            "confirmed_order_conversion_rate": rr(len(co), clicks), "uv": uv, "pv": pv, "likes": likes,
            "bounce_visitors": bounce, "bounce_rate": rr(bounce, uv), "search_clicks": search,
            "add_to_cart_units": atc_u, "add_to_cart_buyers": atc_b,
            "placed_sales": ps, "placed_units": pu, "placed_buyers": nb, "placed_orders": len(po),
            "paid_sales": pds, "paid_units": pdu, "paid_buyers": len(pdb), "paid_orders": len(pdo),
            "confirmed_sales": cs, "confirmed_units": cu, "confirmed_buyers": len(cb), "confirmed_orders": len(co),
            "placed_sales_per_order": round(ps / len(po), 4) if po else 0,
            "confirmed_sales_per_order": round(cs / len(co), 4) if co else 0,
            "paid_sales_per_order": round(pds / len(pdo), 4) if pdo else 0,
            "placed_to_paid_buyers_rate": rr(len(pdb), nb), "placed_buyers_to_confirmed_buyers_rate": rr(len(cb), nb),
            "uv_to_add_to_cart_rate": rr(atc_b, uv), "uv_to_placed_buyers_rate": rr(nb, uv),
            "uv_to_paid_buyers_rate": rr(len(pdb), uv), "uv_to_confirmed_buyers_rate": rr(len(cb), uv),
            "repeat_placed_order_rate": round(rep, 4), "average_days_to_repeat_placed_order": round(rnd.uniform(8, 60), 1) if rep else 0,
            "repeat_paid_order_rate": round(rep * 0.8, 4), "average_days_to_repeat_paid_order": round(rnd.uniform(8, 60), 1) if rep else 0,
            "repeat_confirmed_order_rate": round(rep * 0.9, 4),
            "average_days_to_repeat_confirmed_order": round(rnd.uniform(8, 60), 1) if rep else 0,
            "models": models}
    return data


# crawl hằng tuần (Chủ nhật 06:2x giờ VN), mỗi lần 8 ngày gần nhất -> trùng 1 ngày giữa 2 lần crawl
first_sun = START + timedelta(days=(6 - START.weekday()) % 7)
if first_sun == START:
    first_sun += timedelta(days=7)
sun = first_sun
while sun <= END:
    for shop in ("shopee_hcm", "shopee_hn"):
        crawl_local = datetime(sun.year, sun.month, sun.day, 6, R.randint(15, 45), R.randint(0, 59))
        crawl_t = ts(crawl_local)
        crawl_utc = crawl_local - timedelta(hours=7)
        batch = W.batch_id(crawl_utc)
        for k in range(8, 0, -1):
            day = sun - timedelta(days=k)
            if day < START:
                continue
            rows = []
            ls = [L for L in LISTINGS.values() if L["shop"] == shop and L["created"] <= day and
                  (L["end"] is None or day < L["end"] + timedelta(days=60))]
            st_ = ts(datetime(day.year, day.month, day.day))
            for i, L in enumerate(ls):
                rnd = random.Random(hash((L["item_id"], day.toordinal(), sun.toordinal())) & 0xFFFFFFFF)
                rows.append(envelope("shopee", "product_performance", batch, crawl_utc + timedelta(seconds=5 + i // 10),
                                     {"mode": "scheduled", "shop_id": SHOPS[shop]["shop_id"], "date": day.isoformat(),
                                      "period": "day", "start_time": st_, "end_time": st_ + 86400, "page": 1 + i // 50},
                                     perf_row(L, day, crawl_t, rnd)))
            W.write("shopee", "product_performance", day.isoformat(), f"{batch}-{shop.split('_')[1]}-d{day:%Y%m%d}", rows)
    sun += timedelta(days=7)

# ======================= REVIEWS =======================
POS = ["Sản phẩm đúng mô tả, giao hàng nhanh", "Hàng đẹp, đóng gói cẩn thận", "Dùng rất ok, sẽ ủng hộ shop tiếp",
       "Chất lượng tốt so với giá tiền", "Mua lần 2 rồi vẫn ưng", "Cọ mềm, tán mịn lắm mọi người ơi", "Mút mềm, nở to khi làm ướt",
       "Shop tư vấn nhiệt tình, hàng y hình", "Giao nhanh hơn dự kiến, sản phẩm xinh", "Màu đẹp, cầm chắc tay",
       "Lược chải gỡ rối tốt, không đau da đầu", "Kéo sắc, nhỏ gọn tiện mang theo", "Đáng tiền, recommend nha"]
MID = ["Tạm ổn, đúng giá tiền", "Sản phẩm bình thường, không quá xuất sắc", "Giao hơi chậm nhưng hàng ok",
       "Cọ hơi cứng hơn mình nghĩ", "Màu ngoài hơi khác hình một chút", "Dùng được, chưa thấy gì đặc biệt",
       "Đóng gói sơ sài, may là hàng không sao"]
NEG = ["Hàng lỗi, dùng vài lần đã hỏng", "Không giống mô tả, chất lượng kém", "Giao sai màu, nhắn shop chưa thấy trả lời",
       "Lông cọ rụng nhiều, thất vọng", "Kéo bị cùn, cắt không được", "Bông tắm bung chỉ sau 1 tuần",
       "Hộp móp méo, sản phẩm trầy xước", "Giá sale mà còn mắc hơn ngày thường", "Dùng bị rát da, không nên mua",
       "Thiếu hàng trong đơn, liên hệ mãi chưa giải quyết"]
TAGS = ["Đúng với mô tả", "Chất lượng sản phẩm", "Lông cọ", "Độ bền", "Chất liệu", "Thiết kế", "Hiệu quả"]
TAG_VAL = {5: ["rất tốt", "ok", "tốt", "đúng", "mềm"], 4: ["tốt", "ổn", "ok"], 3: ["tạm", "bình thường", "ổn"],
           2: ["kém", "không giống", "cứng"], 1: ["tệ", "k bền", "kém", "không giống"]}
LOW_REASONS = [("Sản phẩm chất lượng kém", "low_rating_reason_product_quality"),
               ("Sản phẩm không như mô tả", "low_rating_reason_product_not_described"),
               ("Hàng lỗi hoặc bị hư hỏng", "low_rating_reason_product_defective"),
               ("Giao hàng chậm", "low_rating_reason_delivery_slow"),
               ("Thái độ người bán", "low_rating_reason_seller_attitude")]


def make_comment(star, rnd):
    if star == 5 and rnd.random() < 0.55 or star == 4 and rnd.random() < 0.6 or star == 3 and rnd.random() < 0.5:
        return ""
    body = rnd.choice(POS if star >= 4 else (MID if star == 3 else NEG))
    if rnd.random() < 0.35:
        tg = rnd.sample(TAGS, rnd.randint(1, 3))
        return "\n".join(f"{t}:{rnd.choice(TAG_VAL[star])}" for t in tg) + "\n\n" + body
    return body


reviews_by_week = defaultdict(list)
for o in ORDERS:
    if not o["shop"].startswith("shopee") or o.get("outcome") != "success":
        continue
    succ = [t for t, st in o["timeline"] if st == 60]
    if not succ:
        continue
    for l in o["lines"]:
        km = MODEL_OF.get((o["shop"], l["pid"]))
        if not km or R.random() > 0.27:
            continue
        L, mdl = km
        p = ITEMS[l["pid"]]
        rt = succ[0] + int(R.expovariate(1 / (86400 * 2.5)))
        if rt > END_TS:
            continue
        late = (o["deliveryAt"] - o["shipAt"]) > 4 * 86400
        q = p["quality"] - (0.08 if late else 0)
        x = R.random()
        if x < q * 0.95:
            star = 5
        elif x < q * 0.95 + 0.09:
            star = 4
        elif x < q * 0.95 + 0.13:
            star = 3
        elif x < q * 0.95 + 0.15:
            star = 2
        else:
            star = 1
        nimg = R.choices([0, 1, 2, 3, 5], [0.62, 0.2, 0.08, 0.05, 0.05])[0] if star != 3 else R.choice([0, 0, 1])
        low = []
        if star <= 2 and R.random() < 0.6:
            rs = R.sample(LOW_REASONS[:3] + ([LOW_REASONS[3]] if late else []), R.randint(1, 2))
            low = [{"category": "Sản phẩm", "low_rating_reasons": [{"tag_id": 0, "tag_name": a, "transify_key": b, "tag_name_tr": None} for a, b in rs],
                    "category_tr": None}]
        cuid = o["cust"]
        rev = {"comment_id": nxt("review", 1, 9000), "is_hidden": R.random() < 0.01, "rating_star": star,
               "comment": make_comment(star, R), "images": [f"[IMAGE_{i}]" for i in range(nimg)], "ctime": rt,
               "user_id": cuid["shopee_uid"], "user_name": "user_%06x" % ((cuid["idx"] * 2654435761 + 977) & 0xFFFFFF),
               "user_portrait": None, "order_id": 200_000_000_000_000 + o["id"], "order_sn": o["sn"], "product_id": L["item_id"],
               "model_id": mdl["model_id"], "product_cover": L["image"], "product_name": L["name"], "model_name": mdl["name"],
               "reply": None, "item_id": L["item_id"], "status": 2, "mtime": rt, "can_follow_up": None, "follow_up": None,
               "submit_time": rt, "low_rating_reasons": low}
        rd = day_of(rt)
        exp_day = rd + timedelta(days=7 - rd.weekday())  # thứ Hai tuần sau
        reviews_by_week[(o["shop"], exp_day)].append(rev)
        # ~3% review được sửa sau đó -> xuất hiện lại ở file tuần sau (mtime mới)
        if R.random() < 0.03 and star <= 3:
            r2 = dict(rev)
            r2["mtime"] = rt + R.randint(86400 * 3, 86400 * 20)
            if r2["mtime"] <= END_TS:
                r2["rating_star"] = min(5, star + R.randint(1, 3))
                r2["comment"] = (rev["comment"] + "\n\n[Cập nhật] Shop đã hỗ trợ đổi hàng, cảm ơn shop").strip()
                r2["low_rating_reasons"] = []
                md = day_of(r2["mtime"])
                reviews_by_week[(o["shop"], md + timedelta(days=7 - md.weekday()))].append(r2)

for (shop, exp_day), revs in sorted(reviews_by_week.items(), key=lambda x: x[0][1]):
    if exp_day > END:
        continue
    run = datetime(exp_day.year, exp_day.month, exp_day.day, 3, R.randint(0, 59), R.randint(0, 59))  # UTC ~ 10h VN
    b = f"shopee-reviews-{run:%Y%m%dT%H%M%SZ}-{R.getrandbits(24):06x}-d{exp_day:%Y%m%d}"
    revs.sort(key=lambda r: r["mtime"])
    W.write("shopee", "reviews", exp_day.isoformat(), b + "-" + shop.split("_")[1],
            [envelope("shopee", "reviews", b, run, {"mode": "manual_export", "shop_id": SHOPS[shop]["shop_id"],
                                                    "file": f"danh_gia_{shop.split('_')[1]}_{exp_day:%Y%m%d}.xlsx"}, r) for r in revs])

# ======================= THAM CHIẾU =======================
ref = os.path.join(args.out, "_reference")
os.makedirs(ref, exist_ok=True)
with open(os.path.join(ref, "locations.csv"), "w", encoding="utf-8-sig") as f:
    f.write("city_id,city_name,ward_id,ward_name\n")
    for wid, (cid, wn) in sorted(WARDS.items()):
        f.write(f"{cid},{CITY_NAME[cid]},{wid},{wn}\n")
with open(os.path.join(ref, "shops.csv"), "w", encoding="utf-8-sig") as f:
    f.write("shop_key,sale_channel,channel_name,shop_id,default_depot_id,start_date\n")
    names = {42: "Shopee", 48: "TikTok Shop", 1: "Admin (B2B / bán sỉ / giao bù / Zalo-FB)"}
    dd = {"shopee_hcm": HCM, "shopee_hn": HN, "tiktok_main": HCM, "tiktok_2": HCM, "admin": HCM}
    for k, v in SHOPS.items():
        f.write(f"{k},{v['channel']},{names[v['channel']]},{v['shop_id'] or ''},{dd[k]},{v['start']}\n")
with open(os.path.join(ref, "order_status.csv"), "w", encoding="utf-8-sig") as f:
    f.write("status,ten_trang_thai,nhom\n")
    for s, n, g in [(54, "Mới", "Đang xử lý"), (56, "Đã xác nhận", "Đang xử lý"), (57, "Chờ khách xác nhận", "Đang xử lý"),
                    (42, "Đang đóng gói", "Đang xử lý"), (43, "Đã đóng gói", "Đang xử lý"), (59, "Đang chuyển", "Đang giao"),
                    (60, "Thành công", "Thành công"), (61, "Giao thất bại", "Hoàn"), (71, "Đang chuyển hoàn", "Hoàn"),
                    (72, "Đã chuyển hoàn", "Hoàn"), (74, "Xác nhận hoàn (đã nhận lại hàng)", "Hoàn"),
                    (63, "Khách hủy", "Hủy"), (64, "Hệ thống hủy", "Hủy"), (58, "Hãng vận chuyển hủy", "Hủy"),
                    (68, "Hết hàng", "Hủy")]:
        f.write(f"{s},{n},{g}\n")
with open(os.path.join(ref, "inventory_bill_modes.csv"), "w", encoding="utf-8-sig") as f:
    f.write("type,mode,y_nghia\n")
    for t_, m_, n in [(2, 19, "Xuất bán đơn Shopee"), (2, 1, "Xuất bán đơn TikTok/Admin"), (1, 1, "Nhập hàng hoàn (có orderId)"),
                      (2, 3, "Xuất chuyển kho (từ Kho Hàng Thô)"), (1, 3, "Nhập chuyển kho (về kho bán)"),
                      (1, 10, "Nhập hàng từ xưởng/NCC hoặc điều chỉnh kiểm kho thừa"),
                      (2, 10, "Xuất hủy/xuất mẫu/điều chỉnh kiểm kho thiếu")]:
        f.write(f"{t_},{m_},{n}\n")

# thống kê
summary = {"range": [START.isoformat(), END.isoformat()], "seed": args.seed, "scale": args.scale,
           "orders_unique": len(ORDERS), "customers": len(CUSTOMERS),
           "tables": {f"{s}/{e}": {"rows": v[0], "files": v[1]} for (s, e), v in sorted(W.stats.items())}}
from collections import Counter
summary["orders_by_shop"] = dict(Counter(o["shop"] for o in ORDERS))
summary["final_status"] = dict(Counter(str(o["timeline_eff"][-1][1]) for o in ORDERS))
with open(os.path.join(args.out, "_reference", "summary.json"), "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=1)
print(json.dumps(summary, ensure_ascii=False, indent=1))
