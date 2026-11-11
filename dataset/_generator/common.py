"""Tiện ích dùng chung: thời gian (giờ VN), id, tên giả, địa lý, ghi file landing."""
import gzip, json, os, random, string
from datetime import datetime, date, timedelta, timezone

VN = timezone(timedelta(hours=7))
UTC = timezone.utc


def ts(dt_local):
    """datetime giờ VN (naive) -> unix seconds"""
    return int(dt_local.replace(tzinfo=VN).timestamp())


def from_ts(t):
    return datetime.fromtimestamp(t, VN).replace(tzinfo=None)


def day_of(t):
    return from_ts(t).date()


def iso_utc(dt_utc):
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def daterange(d0, d1):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


class Writer:
    """Ghi theo layout source=X/entity=Y/dt=YYYY-MM-DD/<batch>-part0001.jsonl.gz"""

    def __init__(self, root, rnd):
        self.root = root
        self.rnd = rnd
        self.stats = {}

    def batch_id(self, run_utc):
        return run_utc.strftime("%Y%m%dT%H%M%SZ") + "-" + "%08x" % self.rnd.getrandbits(32)

    def write(self, source, entity, dt, batch, rows):
        if not rows:
            return
        d = os.path.join(self.root, f"source={source}", f"entity={entity}", f"dt={dt}")
        os.makedirs(d, exist_ok=True)
        p = os.path.join(d, f"{batch}-part0001.jsonl.gz")
        with gzip.open(p, "wt", encoding="utf-8", compresslevel=6) as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")))
                f.write("\n")
        k = (source, entity)
        s = self.stats.setdefault(k, [0, 0])
        s[0] += len(rows)
        s[1] += 1


def envelope(source, entity, batch, extracted_utc, request, data):
    return {"_source": source, "_entity": entity, "_batch_id": batch,
            "_extracted_at": iso_utc(extracted_utc) if extracted_utc else None,
            "_request": request, "data": data}


# ---------------- tên giả ----------------
HO = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng", "Bùi", "Đỗ", "Hồ", "Ngô",
      "Dương", "Lý", "Đinh", "Trương", "Lâm", "Mai", "Tô", "Cao", "Tạ", "Lưu", "Châu"]
HO_W = [31, 11, 9, 7, 5, 5, 4, 4, 4, 3, 3, 3, 2, 2, 2, 1.5, 1.2, 1.2, 1, 1, .6, .6, .5, .5, .4]
DEM_NU = ["Thị", "Ngọc", "Thu", "Thanh", "Minh", "Phương", "Bảo", "Kim", "Mỹ", "Hồng", "Khánh", "Hoài", "Diệu", "Quỳnh", "Gia", "Tường"]
TEN_NU = ["Anh", "Linh", "Trang", "Hương", "Hà", "Ngân", "Thảo", "Vy", "Nhi", "My", "Huyền", "Trâm", "Yến", "Hằng",
          "Nhung", "Phương", "Quyên", "Uyên", "Tiên", "Châu", "Ngọc", "Dung", "Loan", "Mai", "Lan", "Hân", "Thư",
          "Duyên", "Giang", "Hạnh", "Khánh", "Oanh", "Tâm", "Trinh", "Vân", "Xuân", "Thùy", "Hiền", "Diễm", "Như"]
DEM_NAM = ["Văn", "Minh", "Quốc", "Đức", "Hoàng", "Thanh", "Gia", "Hữu", "Công", "Tuấn", "Anh", "Nhật"]
TEN_NAM = ["Huy", "Khang", "Bảo", "Phúc", "Nam", "Long", "Hùng", "Dũng", "Tuấn", "Khoa", "Tài", "Thịnh", "Đạt",
           "Hiếu", "Quân", "Trung", "Vinh", "Sơn", "Phát", "Lộc", "Tín", "Kiệt", "Duy", "Hải"]

# 34 tỉnh/thành sau sáp nhập 7/2025; cityId 254/255 khớp dữ liệu mẫu, còn lại tự gán
PROVINCES = [
    (254, "Hà Nội", 22), (255, "Hồ Chí Minh", 28), (256, "Hải Phòng", 4), (257, "Đà Nẵng", 3.5),
    (258, "Cần Thơ", 3), (259, "Huế", 1.2), (260, "Đồng Nai", 5), (261, "Bắc Ninh", 3.5),
    (262, "Hưng Yên", 2), (263, "Quảng Ninh", 1.6), (264, "Thanh Hóa", 2), (265, "Nghệ An", 2),
    (266, "Hà Tĩnh", .8), (267, "Quảng Trị", .7), (268, "Quảng Ngãi", 1), (269, "Gia Lai", 1.1),
    (270, "Đắk Lắk", 1.3), (271, "Khánh Hòa", 1.5), (272, "Lâm Đồng", 1.8), (273, "Tây Ninh", 1.8),
    (274, "Đồng Tháp", 1.2), (275, "Vĩnh Long", 1.2), (276, "An Giang", 1.3), (277, "Cà Mau", .8),
    (278, "Thái Nguyên", 1.1), (279, "Phú Thọ", 1.2), (280, "Ninh Bình", 1), (281, "Lào Cai", .5),
    (282, "Tuyên Quang", .4), (283, "Lạng Sơn", .4), (284, "Cao Bằng", .2), (285, "Sơn La", .4),
    (286, "Điện Biên", .2), (287, "Lai Châu", .15),
]
NORTH = {254, 256, 261, 262, 263, 264, 265, 266, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287}
CENTRAL = {257, 259, 267, 268, 269, 270, 271}

WARD_HCM = ["Sài Gòn", "Bến Thành", "Tân Định", "Bàn Cờ", "Xuân Hòa", "Chợ Lớn", "An Đông", "Bình Tây", "Phú Nhuận",
            "Đức Nhuận", "Gia Định", "Bình Thạnh", "Thạnh Mỹ Tây", "Gò Vấp", "Hạnh Thông", "An Nhơn", "Tân Sơn Nhất",
            "Tân Bình", "Bảy Hiền", "Tân Phú", "Phú Thọ Hòa", "Bình Tân", "An Lạc", "Thủ Đức", "Linh Xuân",
            "An Phú", "Bình Trưng", "Cát Lái", "Hiệp Bình", "Tân Chánh Hiệp", "Trung Mỹ Tây", "Tân Thới Hiệp",
            "Đông Hưng Thuận", "Phú Định", "Tân Hưng", "Tân Mỹ", "Dĩ An", "Thủ Dầu Một", "Vũng Tàu", "Bà Rịa"]
WARD_HN = ["Hoàn Kiếm", "Cửa Nam", "Ba Đình", "Ngọc Hà", "Giảng Võ", "Đống Đa", "Văn Miếu - Quốc Tử Giám", "Láng",
           "Ô Chợ Dừa", "Kim Liên", "Hai Bà Trưng", "Bạch Mai", "Vĩnh Tuy", "Tây Hồ", "Phú Thượng", "Cầu Giấy",
           "Nghĩa Đô", "Yên Hòa", "Thanh Xuân", "Khương Đình", "Phương Liệt", "Hoàng Mai", "Định Công", "Hoàng Liệt",
           "Long Biên", "Bồ Đề", "Việt Hưng", "Hà Đông", "Dương Nội", "Kiến Hưng", "Từ Liêm", "Xuân Phương",
           "Tây Mỗ", "Đại Mỗ", "Thượng Cát", "Đông Anh", "Gia Lâm", "Thanh Trì", "Sơn Tây", "Hoài Đức"]
WARD_GENERIC = ["An Hòa", "Tân Phú", "Hòa Bình", "Phước Long", "Thạnh Mỹ", "Tân An", "Bình Minh", "Long Hưng",
                "Phú Lợi", "Trung Tâm", "Đông Thành", "Tây Sơn", "Nam Định", "Bắc Sơn", "Hưng Lợi", "Tân Lập",
                "Quang Trung", "Lê Lợi", "Trần Phú", "Hùng Vương", "Thống Nhất", "Hòa Thuận", "Vĩnh Phúc",
                "Hiệp Thành", "Phú Hòa", "An Phước", "Tân Thành", "Đông Hòa"]
STREETS = ["Lê Lợi", "Nguyễn Trãi", "Trần Hưng Đạo", "Lý Thường Kiệt", "Hai Bà Trưng", "Nguyễn Huệ", "Lê Duẩn",
           "Phan Đình Phùng", "Võ Văn Tần", "Cách Mạng Tháng 8", "Nguyễn Văn Linh", "Điện Biên Phủ", "Hoàng Văn Thụ",
           "Lạc Long Quân", "Âu Cơ", "Nguyễn Thị Minh Khai", "Pasteur", "Lê Văn Sỹ", "Phạm Văn Đồng", "Quang Trung",
           "Nguyễn Văn Cừ", "Tô Hiến Thành", "Bạch Đằng", "Trường Chinh", "Cộng Hòa", "Xô Viết Nghệ Tĩnh",
           "Kim Mã", "Xã Đàn", "Giải Phóng", "Láng Hạ", "Nguyễn Chí Thanh", "Tôn Đức Thắng", "Lê Văn Việt",
           "Kha Vạn Cân", "Số 2", "Số 7", "Số 12", "D2", "N4", "Tân Lập"]


def build_geo(rnd):
    """Trả về list tỉnh và dict wardId -> (cityId, wardName)"""
    wards = {}
    by_city = {}
    wid = 20001
    for cid, cname, _w in PROVINCES:
        if cid == 255:
            names = WARD_HCM
        elif cid == 254:
            names = WARD_HN
        else:
            k = rnd.randint(8, 18)
            names = rnd.sample(WARD_GENERIC, k)
        lst = []
        for n in names:
            kind = "Phường" if (cid in (254, 255) or rnd.random() < 0.6) else "Xã"
            wards[wid] = (cid, f"{kind} {n}")
            lst.append(wid)
            wid += rnd.randint(1, 9)
        by_city[cid] = lst
    return wards, by_city


def rand_name(rnd):
    ho = rnd.choices(HO, HO_W)[0]
    if rnd.random() < 0.86:
        return f"{ho} {rnd.choice(DEM_NU)} {rnd.choice(TEN_NU)}"
    return f"{ho} {rnd.choice(DEM_NAM)} {rnd.choice(TEN_NAM)}"


def rand_phone(rnd):
    pre = rnd.choice(["090", "093", "089", "070", "079", "077", "076", "078", "091", "094", "088", "083", "084",
                      "085", "081", "082", "086", "096", "097", "098", "032", "033", "034", "035", "036", "037",
                      "038", "039", "056", "058", "059"])
    return pre + "".join(rnd.choice(string.digits) for _ in range(7))


def rand_address(rnd, cid):
    n = rnd.randint(1, 450)
    st = rnd.choice(STREETS)
    r = rnd.random()
    if cid == 254 and r < 0.5:
        return f"Số {n}, Ngõ {rnd.randint(2, 200)} Phố {st}"
    if r < 0.35:
        return f"{n}/{rnd.randint(1, 60)} Đường {st}"
    if r < 0.45:
        return f"Chung cư {rnd.choice(['Sunrise', 'Vinhomes', 'Masteri', 'Ehome', 'Hoàng Anh', 'Lotus', 'Celadon'])}, Block {rnd.choice('ABCDEFGH')}, Phòng {rnd.randint(1, 30)}{rnd.randint(1, 20):02d}"
    return f"{n} Đường {st}"


ALNUM = string.digits + string.ascii_uppercase


def shopee_sn(rnd, d):
    return d.strftime("%y%m%d") + "".join(rnd.choice(ALNUM) for _ in range(8))


def img_hash(rnd):
    return "vn-11134207-7r98o-" + "".join(rnd.choice(string.ascii_lowercase + string.digits) for _ in range(14))
