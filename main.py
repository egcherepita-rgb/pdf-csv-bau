import io
import os
import re
import csv
from collections import OrderedDict
from typing import List, Tuple, Dict

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

try:
    import openpyxl  # requires openpyxl in requirements.txt
except Exception:
    openpyxl = None


app = FastAPI(title="PDF → CSV (артикул / наименование / всего / категория)", version="3.8.0")

# -------------------------
# Static files (logo, etc.)
# -------------------------
STATIC_DIR = os.getenv("STATIC_DIR", "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# -------------------------
# Regex
# -------------------------
RX_SIZE = re.compile(r"\b\d{2,}[xх×]\d{2,}(?:[xх×]\d{1,})?\b", re.IGNORECASE)
RX_MM = re.compile(r"мм", re.IGNORECASE)
RX_WEIGHT = re.compile(r"\b\d+(?:[.,]\d+)?\s*кг\.?\b", re.IGNORECASE)

RX_MONEY_LINE = re.compile(r"^\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?\s*₽$")
RX_INT = re.compile(r"^\d+$")
RX_ANY_RUB = re.compile(r"₽")

# В одной строке: "... 450 ₽ 1 450 ₽"
RX_PRICE_QTY_SUM = re.compile(
    r"(?P<price>\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?)\s*₽\s+"
    r"(?P<qty>\d{1,4})\s+"
    r"(?P<sum>\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?)\s*₽",
    re.IGNORECASE,
)

# Размеры где угодно в строке: "30x10 мм", "600×300 мм" и т.п.
RX_DIMS_ANYWHERE = re.compile(
    r"\s*\d{1,4}[xх×]\d{1,4}(?:[xх×]\d{1,5})?\s*мм\.?\s*",
    re.IGNORECASE,
)


def normalize_space(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def normalize_key(name: str) -> str:
    s = normalize_space(name).lower()
    s = s.replace("×", "x").replace("х", "x")
    s = RX_DIMS_ANYWHERE.sub(" ", s)
    s = normalize_space(s)
    return s


def strip_dims_anywhere(name: str) -> str:
    name = normalize_space(name)
    name2 = RX_DIMS_ANYWHERE.sub(" ", name)
    return normalize_space(name2)


# -------------------------
# ID/Артикулы (Art1.xlsx)
# -------------------------
def load_article_map() -> Tuple[Dict[str, str], str, str]:
    """
    Возвращает:
      - map: normalized товар -> значение выбранной колонки (по умолчанию "Кастомный ID")
      - status: строка статуса
      - used_column: имя столбца, откуда взяли значение
    """
    if openpyxl is None:
        return {}, "openpyxl_not_installed", ""

    # Для БауЦентра по умолчанию Art1.xlsx
    path = os.getenv("ART_XLSX_PATH", "Art1.xlsx")
    if not os.path.exists(path):
        return {}, f"file_not_found:{path}", ""

    # Какой столбец брать как "Артикул" в итоговом CSV
    # (по умолчанию берём "Кастомный ID", но можно переопределить)
    desired_value_col = normalize_space(os.getenv("ART_VALUE_COLUMN", "Кастомный ID"))

    try:
        wb = openpyxl.load_workbook(path, data_only=True)
        ws = wb[wb.sheetnames[0]]
    except Exception as e:
        return {}, f"cannot_open:{e}", ""

    header = [normalize_space(ws.cell(1, c).value or "") for c in range(1, ws.max_column + 1)]

    # ищем колонки по названиям
    товар_col = None
    value_col = None

    for idx, h in enumerate(header, start=1):
        hl = h.lower()
        if hl == "товар":
            товар_col = idx
        if h == desired_value_col:
            value_col = idx

    # fallback-логика, если вдруг заголовок отличается/не найден:
    if товар_col is None:
        товар_col = 1  # как раньше

    if value_col is None:
        # пробуем "Артикул", если "Кастомный ID" не найден
        for idx, h in enumerate(header, start=1):
            if h.lower() == "артикул":
                value_col = idx
                desired_value_col = "Артикул"
                break

    if value_col is None:
        return {}, f"bad_header:no колонка '{desired_value_col}' (и fallback 'Артикул')", ""

    m: Dict[str, str] = {}
    for r in range(2, ws.max_row + 1):
        товар = ws.cell(r, товар_col).value
        val = ws.cell(r, value_col).value
        if not товар or val is None:
            continue

        товар_s = normalize_space(str(товар))
        val_s = normalize_space(str(val))
        if not товар_s or not val_s:
            continue

        m[normalize_key(товар_s)] = val_s

    return m, "ok", desired_value_col


ARTICLE_MAP, ARTICLE_MAP_STATUS, ARTICLE_VALUE_COLUMN = load_article_map()

CATEGORY_VALUE = 2

# -------------------------
# Счетчик конвертаций (простое файловое хранилище)
# -------------------------
from threading import Lock

COUNTER_FILE = os.getenv("COUNTER_FILE", "conversions.count")
_counter_lock = Lock()

def _read_counter() -> int:
    try:
        with open(COUNTER_FILE, "r", encoding="utf-8") as f:
            return int((f.read() or "0").strip())
    except Exception:
        return 0

def _write_counter(v: int) -> None:
    tmp = COUNTER_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(str(v))
    os.replace(tmp, COUNTER_FILE)

def increment_counter() -> int:
    with _counter_lock:
        v = _read_counter() + 1
        _write_counter(v)
        return v

def get_counter() -> int:
    return _read_counter()

# Видео-инструкция (положи рядом с main.py)
INSTRUCTION_VIDEO_PATH = os.getenv("INSTRUCTION_VIDEO_PATH", "instruction.mp4")


# -------------------------
# PDF parsing helpers
# -------------------------
def is_noise(line: str) -> bool:
    low = (line or "").strip().lower()
    if not low:
        return True
    if low.startswith("страница:"):
        return True
    if low.startswith("ваш проект"):
        return True
    if "проект создан" in low:
        return True
    if "развертка стены" in low:
        return True
    if "стоимость проекта" in low:
        return True
    return False


def is_totals_block(line: str) -> bool:
    low = (line or "").strip().lower()
    return (
        low.startswith("общий вес")
        or low.startswith("максимальный габарит заказа")
        or low.startswith("адрес:")
        or low.startswith("телефон:")
        or low.startswith("email")
    )


def money_to_number(line: str) -> int:
    s = normalize_space(line).replace("₽", "").strip()
    s = s.replace("\u00a0", " ")
    s = s.replace(" ", "")
    s = s.replace(",", ".")
    try:
        return int(float(s))
    except Exception:
        return -1


def is_project_total_only(line: str, prev_line: str = "") -> bool:
    if not RX_MONEY_LINE.fullmatch(normalize_space(line)):
        return False
    prev = normalize_space(prev_line).lower()
    if "стоимость проекта" in prev:
        return True
    v = money_to_number(line)
    return v >= 10000


def is_header_token(line: str) -> bool:
    low = normalize_space(line).lower().replace("–", "-").replace("—", "-")

    # может быть как отдельные токены, так и вся шапка таблицы одной строкой:
    if ("id" in low and "товар" in low and "кол-во" in low and "сумма" in low):
        return True

    return low in {"id", "фото", "товар", "габариты", "вес", "цена за шт", "кол-во", "сумма"}



def looks_like_dim_or_weight(line: str) -> bool:
    if RX_WEIGHT.search(line):
        return True
    if RX_SIZE.search(line) and RX_MM.search(line):
        return True
    return False


def looks_like_money_or_qty(line: str) -> bool:
    if RX_MONEY_LINE.fullmatch(line):
        return True
    if RX_INT.fullmatch(line):
        return True
    return False


def clean_name_from_buffer(buf: List[str]) -> str:
    filtered = []
    for ln in buf:
        if is_noise(ln) or is_header_token(ln) or is_totals_block(ln):
            continue
        filtered.append(ln)

    # убираем "хвосты" (габариты/вес/деньги/кол-во), которые могли попасть в буфер
    while filtered and (looks_like_dim_or_weight(filtered[-1]) or looks_like_money_or_qty(filtered[-1])):
        filtered.pop()

    name = normalize_space(" ".join(filtered))

    # убираем возможные заголовки
    name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()

    # В отчётах формата "ID Фото Товар ..." у каждой позиции часто первым идёт числовой ID (код).
    # Он НЕ является "наименованием", поэтому срезаем его из начала.
    name = re.sub(r"^(?:ID\s*)?\d{6,}\s+", "", name, flags=re.IGNORECASE).strip()

    # убираем габариты, если они где-то встроены в строку
    name = strip_dims_anywhere(name)
    return name


    while filtered and (looks_like_dim_or_weight(filtered[-1]) or looks_like_money_or_qty(filtered[-1])):
        filtered.pop()

    name = normalize_space(" ".join(filtered))
    name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()
    name = strip_dims_anywhere(name)
    return name


# -------------------------
# Main parser
# -------------------------
def parse_items(pdf_bytes: bytes) -> Tuple[List[Tuple[str, int]], Dict]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    ordered = OrderedDict()
    buf: List[str] = []

    stats = {
        "pages": 0,
        "items_found": 0,
        "anchors_inline": 0,
        "anchors_multiline": 0,
        "article_map_size": len(ARTICLE_MAP),
        "article_map_status": ARTICLE_MAP_STATUS,
        "article_value_column": ARTICLE_VALUE_COLUMN,
    }

    for page in doc:
        stats["pages"] += 1

        txt = page.get_text("text") or ""
        if "₽" not in txt:
            continue

        lines = [normalize_space(x) for x in txt.splitlines()]
        lines = [x for x in lines if x]
        if not lines:
            continue

        in_totals = False
        buf.clear()

        i = 0
        while i < len(lines):
            line = lines[i]
            prev = lines[i - 1] if i > 0 else ""

            if is_noise(line) or is_header_token(line):
                i += 1
                continue

            if is_project_total_only(line, prev_line=prev) or is_totals_block(line):
                in_totals = True
                buf.clear()
                i += 1
                continue

            if in_totals:
                i += 1
                continue

            # A) INLINE anchor
            m = RX_PRICE_QTY_SUM.search(line)
            if m:
                name = clean_name_from_buffer(buf)
                buf.clear()

                if name:
                    try:
                        qty = int(m.group("qty"))
                    except Exception:
                        qty = 0

                    if 1 <= qty <= 500:
                        ordered[name] = ordered.get(name, 0) + qty
                        stats["items_found"] += 1
                        stats["anchors_inline"] += 1

                i += 1
                continue

            # B) MULTILINE anchor
            if RX_MONEY_LINE.fullmatch(line):
                end = min(len(lines), i + 8)

                qty_idx = None
                for j in range(i + 1, end):
                    if RX_INT.fullmatch(lines[j]):
                        q = int(lines[j])
                        if 1 <= q <= 500:
                            qty_idx = j
                            break

                if qty_idx is None:
                    buf.append(line)
                    i += 1
                    continue

                sum_idx = None
                for j in range(qty_idx + 1, end):
                    if RX_MONEY_LINE.fullmatch(lines[j]) or RX_ANY_RUB.search(lines[j]):
                        sum_idx = j
                        break

                if sum_idx is None:
                    buf.append(line)
                    i += 1
                    continue

                name = clean_name_from_buffer(buf)
                buf.clear()

                if name:
                    qty = int(lines[qty_idx])
                    ordered[name] = ordered.get(name, 0) + qty
                    stats["items_found"] += 1
                    stats["anchors_multiline"] += 1

                i = sum_idx + 1
                continue

            buf.append(line)
            i += 1

    return list(ordered.items()), stats


# -------------------------
# CSV output (Excel-friendly)
# -------------------------
def make_csv_excel_friendly(rows: List[Tuple[str, int]]) -> bytes:
    out = io.StringIO()
    writer = csv.writer(
        out,
        delimiter=";",
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\r\n",
    )

    # ВНИМАНИЕ: колонка называется "Артикул", но значение будет из "Кастомный ID" (по умолчанию)
    writer.writerow(["Артикул", "Наименование", "Всего"])

    for name, qty in rows:
        custom_id = ARTICLE_MAP.get(normalize_key(name), "")
        writer.writerow([custom_id, name, qty])

    return out.getvalue().encode("utf-8-sig")  # UTF-8 BOM


# -------------------------
# UI (миниатюра + открыть полностью)
# -------------------------
# -------------------------
# UI (Logo + clean page)
# -------------------------
HOME_HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>bau.pdfcsv.ru — PDF → CSV</title>
  <style>
    :root{
      --bg1:#061225;
      --bg2:#0b2b57;
      --card:#ffffff;
      --text:#0b1f3a;
      --muted:#5f6f86;
      --stroke:#d7e0ef;
      --shadow: 0 18px 50px rgba(0,0,0,.22);

      /* Baucenter-like accents */
      --brand:#0b4aa2;     /* фирменный синий */
      --brand2:#083a83;    /* тёмный синий */
      --accent:#e41e2b;    /* фирменный красный */
      --ok:#1f9d55;
      --warn:#d97706;
    }
    *{ box-sizing:border-box; }
    html,body{ height:100%; }
    body{
      margin:0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      background:
        radial-gradient(900px 500px at 50% 16%, rgba(44,106,202,.55) 0%, rgba(9,33,72,.0) 60%),
        radial-gradient(1200px 700px at 50% 80%, rgba(10,39,86,.7) 0%, rgba(6,18,37,1) 55%),
        linear-gradient(180deg, var(--bg2), var(--bg1));
      color: var(--text);
      overflow-x:hidden;
    }
    /* subtle grid */
    body:before{
      content:"";
      position:fixed; inset:0;
      background-image:
        linear-gradient(to right, rgba(255,255,255,.04) 1px, transparent 1px),
        linear-gradient(to bottom, rgba(255,255,255,.04) 1px, transparent 1px);
      background-size: 64px 64px;
      mask-image: radial-gradient(700px 380px at 50% 22%, black 0%, transparent 70%);
      pointer-events:none;
      opacity:.65;
    }

    .wrap{
      min-height:100%;
      display:flex;
      align-items:center;
      justify-content:center;
      padding: 40px 16px 70px;
    }
    .stack{
      width:min(860px, 100%);
      display:flex;
      flex-direction:column;
      align-items:center;
      gap:14px;
    }

            .brand{
      width: min(860px, 100%);
      display:flex;
      align-items:center;
      justify-content:center;
      padding: 12px 14px;
      border-radius: 18px;
      background: linear-gradient(180deg, rgba(11,43,87,.92), rgba(6,18,37,.72));
      border: 1px solid rgba(255,255,255,.10);
      box-shadow: 0 18px 40px rgba(0,0,0,.28);
      backdrop-filter: blur(10px);
      position:relative;
      overflow:hidden;
    }
    .brand:before{
      content:"";
      position:absolute; inset:0;
      background:
        radial-gradient(520px 160px at 50% 0%, rgba(255,255,255,.10) 0%, rgba(255,255,255,0) 70%),
        linear-gradient(90deg, rgba(228,30,43,.0), rgba(228,30,43,.18), rgba(228,30,43,.0));
      opacity:.85;
      pointer-events:none;
      mix-blend-mode: screen;
    }
    .brand img{
      height: 60px;
      width: auto;
      max-width: 100%;
      object-fit: contain;
      display:block;
      /* чуть отделяем от фона, но без ощущения "вставки" */
      filter: drop-shadow(0 10px 18px rgba(0,0,0,.20));
      position:relative;
    }
        .brand img{
      max-width: 300px;
      max-height: 82px;
      object-fit: contain;
      filter: drop-shadow(0 10px 18px rgba(0,0,0,.28));
    }

    .card{
      width:min(860px,100%);
      background: var(--card);
      border-radius: 20px;
      box-shadow: var(--shadow);
      padding: 26px 26px 20px;
      position:relative;
    }
    .card:after{
      content:"";
      position:absolute; inset:0;
      border-radius: 20px;
      pointer-events:none;
      border:1px solid rgba(10,30,60,.08);
    }

    h1{
      margin:0;
      font-size: 30px;
      letter-spacing:.2px;
      text-align:center;
    }
    .sub{
      margin: 8px 0 18px;
      text-align:center;
      color: var(--muted);
      font-size: 14px;
      line-height:1.45;
    }

    .zone{
      border: 2px dashed var(--stroke);
      border-radius: 18px;
      padding: 18px;
      background: linear-gradient(180deg, #fbfcff, #f6f9ff);
      transition: .15s ease;
    }
    .zone.drag{
      border-color: rgba(228,30,43,.55);
      box-shadow: 0 0 0 6px rgba(228,30,43,.10);
    }
    .zone-inner{
      display:flex;
      gap:14px;
      align-items:center;
      justify-content:center;
      flex-wrap:wrap;
    }
    .icon{
      width: 44px;
      height: 44px;
      border-radius: 12px;
      display:flex;
      align-items:center;
      justify-content:center;
      background: rgba(11,74,162,.10);
      border: 1px solid rgba(228,30,43,.20);
    }
    .icon svg{ width:22px; height:22px; }
    .hint{
      text-align:center;
      color: var(--muted);
      font-size: 14px;
      margin:0;
      max-width: 520px;
    }
    .row{
      display:flex;
      gap:12px;
      justify-content:center;
      align-items:center;
      margin-top: 14px;
      flex-wrap:wrap;
    }

    .btn{
      appearance:none;
      border:0;
      border-radius: 12px;
      padding: 12px 16px;
      font-weight: 650;
      cursor:pointer;
      transition: .15s ease;
      user-select:none;
      display:inline-flex;
      align-items:center;
      justify-content:center;
      gap:10px;
      min-width: 190px;
      text-decoration:none;
    }
    .btn.primary{
      background: linear-gradient(180deg, var(--brand), var(--brand2));
      border: 1px solid rgba(255,255,255,.18);
      color: #fff;
      box-shadow: 0 12px 22px rgba(30,95,216,.28);
    }
    .btn.primary:hover{ transform: translateY(-1px); }
    .btn.secondary{
      background:#fff;
      border:1px solid var(--stroke);
      color: var(--text);
    }
    .btn.secondary:hover{ border-color: rgba(30,95,216,.35); }
    .btn:disabled{
      opacity:.55;
      cursor:not-allowed;
      transform:none !important;
      box-shadow:none !important;
    }

    .btn:focus{ outline:none; }
    .btn:focus-visible{
      box-shadow: 0 0 0 4px rgba(228,30,43,.18), 0 0 0 1px rgba(228,30,43,.55) inset;
    }

    .fileline{
      margin-top: 10px;
      text-align:center;
      color: var(--muted);
      font-size: 13px;
    }
    .filepill{
      display:inline-flex;
      align-items:center;
      gap:8px;
      padding: 6px 10px;
      border-radius: 999px;
      border:1px solid var(--stroke);
      background: #fff;
      max-width: 100%;
    }
    .filepill b{
      font-weight: 650;
      color: var(--text);
      white-space: nowrap;
      overflow:hidden;
      text-overflow: ellipsis;
      max-width: 520px;
    }

    .status{
      margin-top: 14px;
      font-size: 13px;
      text-align:center;
      min-height: 18px;
    }
    .status.ok{ color: var(--ok); }
    .status.err{ color: #b42318; }
    .status.warn{ color: var(--warn); }

    .footer{
      margin-top: 18px;
      display:flex;
      justify-content:space-between;
      gap:12px;
      color: var(--muted);
      font-size: 12px;
      flex-wrap:wrap;
    }
    .footer a{ color: var(--brand2); text-decoration:none; }
    .footer a:hover{ text-decoration:underline; }

    /* floating counter */
    .counter{
      position:fixed;
      right: 16px;
      bottom: 16px;
      background: linear-gradient(180deg, rgba(6,32,70,.82), rgba(4,22,50,.76));
      color:#fff;
      border: 1px solid rgba(255,255,255,.12);
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 12px;
      display:flex;
      align-items:center;
      gap:8px;
      backdrop-filter: blur(10px);
      box-shadow: 0 14px 28px rgba(0,0,0,.25);
    }
    .dot{
      width:8px;height:8px;border-radius:99px;
      background: var(--accent);
      box-shadow: 0 0 0 4px rgba(228,30,43,.18);
    }

    /* mobile */
    @media (max-width:560px){
      .brand{ width: 100%; padding: 8px 8px 0; }
      .card{ padding: 20px 16px 16px; border-radius: 18px; }
      h1{ font-size: 24px; }
      .btn{ min-width: 100%; }
      .filepill b{ max-width: 260px; }
      .footer{ justify-content:center; text-align:center; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="stack">
      <div class="brand">
        <img src="/static/logo.png" alt="Бауцентр" onerror="this.style.display='none'">
      </div>
      

      <div class="card">
        <h1>Конвертация PDF → CSV</h1>
        <p class="sub">
          Загрузите PDF (отчёт/корзина из 3D конфигуратора) — получите файл CSV.<br>
          Можно просто перетащить файл в область ниже.
        </p>

        <div id="zone" class="zone">
          <div class="zone-inner">
            <div class="icon" aria-hidden="true">
              <svg viewBox="0 0 24 24" fill="none">
                <path d="M12 3v10m0-10l-4 4m4-4l4 4" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
                <path d="M4 14v5a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-5" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
              </svg>
            </div>
            <p class="hint">
              Перетащите PDF сюда или выберите файл на устройстве.
            </p>
          </div>

          <div class="row">
            <input id="file" type="file" accept="application/pdf" hidden />
            <button id="pick" class="btn secondary" type="button">Выбрать PDF</button>
            <button id="go" class="btn primary" type="button" disabled>
              <span id="goText">Скачать CSV</span>
              <span id="spinner" style="display:none">⏳</span>
            </button>
          </div>

          <div class="fileline" id="fileline" style="display:none">
            <span class="filepill">
              ✅ <span>Файл:</span> <b id="fname"></b>
            </span>
          </div>
        </div>

        <div id="status" class="status"></div>

        <div class="footer" style="justify-content:center"><div><a href="/health" target="_blank" rel="noopener">.</a></div></div>
      </div>
    </div>
  </div>

  <div class="counter"><span class="dot"></span><span id="count">Конвертаций: …</span></div>

  <script>
    const zone = document.getElementById('zone');
    const fileInput = document.getElementById('file');
    const pickBtn = document.getElementById('pick');
    const goBtn = document.getElementById('go');
    const goText = document.getElementById('goText');
    const spinner = document.getElementById('spinner');
    const statusEl = document.getElementById('status');
    const fileline = document.getElementById('fileline');
    const fname = document.getElementById('fname');

    let selectedFile = null;

    function setStatus(msg, cls){
      statusEl.className = 'status ' + (cls || '');
      statusEl.textContent = msg || '';
    }

    function setBusy(b){
      goBtn.disabled = b || !selectedFile;
      spinner.style.display = b ? 'inline' : 'none';
      goText.textContent = b ? 'Конвертирую…' : 'Скачать CSV';
      pickBtn.disabled = b;
      zone.style.pointerEvents = b ? 'none' : 'auto';
      zone.style.opacity = b ? '.85' : '1';
    }

    function setFile(f){
      selectedFile = f;
      if (f){
        fname.textContent = f.name;
        fileline.style.display = 'block';
        goBtn.disabled = false;
        setStatus('', '');
      }else{
        fileline.style.display = 'none';
        goBtn.disabled = true;
      }
    }

    pickBtn.addEventListener('click', () => fileInput.click());

    fileInput.addEventListener('change', () => {
      const f = fileInput.files && fileInput.files[0];
      if (f) setFile(f);
    });

    ['dragenter','dragover'].forEach(evt => {
      zone.addEventListener(evt, (e) => {
        e.preventDefault(); e.stopPropagation();
        zone.classList.add('drag');
      });
    });
    ['dragleave','drop'].forEach(evt => {
      zone.addEventListener(evt, (e) => {
        e.preventDefault(); e.stopPropagation();
        zone.classList.remove('drag');
      });
    });
    zone.addEventListener('drop', (e) => {
      const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
      if (f) setFile(f);
    });

    goBtn.addEventListener('click', async () => {
      if (!selectedFile) return;
      setBusy(true);
      setStatus('', '');

      try{
        const fd = new FormData();
        fd.append('file', selectedFile, selectedFile.name);

        const res = await fetch('/extract', { method:'POST', body: fd });
        if (!res.ok){
          let msg = 'Ошибка конвертации';
          try{
            const j = await res.json();
            msg = (j && j.detail) ? j.detail : msg;
          }catch(_){}
          setStatus(msg, 'err');
          setBusy(false);
          return;
        }

        const blob = await res.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;

        // имя из Content-Disposition если есть
        const cd = res.headers.get('Content-Disposition') || '';
        const m = /filename="([^"]+)"/i.exec(cd);
        a.download = m ? m[1] : 'items.csv';

        document.body.appendChild(a);
        a.click();
        a.remove();
        window.URL.revokeObjectURL(url);

        setStatus('Готово! CSV скачан.', 'ok');
        await refreshCounter();
      }catch(e){
        setStatus('Не удалось выполнить запрос. Проверьте интернет/сервер.', 'err');
      }finally{
        setBusy(false);
      }
    });

    async function refreshCounter(){
      try{
        const r = await fetch('/stats');
        const j = await r.json();
        if (j && typeof j.total === 'number'){
          document.getElementById('count').textContent = 'Конвертаций: ' + j.total;
        }
      }catch(_){}
    }
    refreshCounter();
  </script>
</body>
</html>
"""




@app.get("/stats")
def stats():
    return {"conversions": get_counter()}


@app.get("/health")
def health():
    return {
        "status": "ok",
        "article_map_size": len(ARTICLE_MAP),
        "article_map_status": ARTICLE_MAP_STATUS,
        "article_value_column": ARTICLE_VALUE_COLUMN,
        "category_value": CATEGORY_VALUE,
        "instruction_video_exists": os.path.exists(INSTRUCTION_VIDEO_PATH),
        "conversions": get_counter(),
        "art_xlsx_path": os.getenv("ART_XLSX_PATH", "Art1.xlsx"),
    }


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def home():
    return HOME_HTML


@app.get("/instruction.mp4")
def instruction_video():
    if not os.path.exists(INSTRUCTION_VIDEO_PATH):
        raise HTTPException(status_code=404, detail="instruction.mp4 not found")
    return FileResponse(INSTRUCTION_VIDEO_PATH, media_type="video/mp4")


@app.post("/extract")
async def extract(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Загрузите PDF файл (.pdf).")

    pdf_bytes = await file.read()

    try:
        rows, stats = parse_items(pdf_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось распарсить PDF: {e}")

    if not rows:
        raise HTTPException(
            status_code=422,
            detail=(
                "Не удалось найти позиции. Поддерживаемые якоря:\n"
                "1) в одной строке: price ₽ qty sum ₽\n"
                "2) в разных строках: price ₽ -> qty -> sum ₽\n"
                f"debug={stats}"
            ),
        )

    csv_bytes = make_csv_excel_friendly(rows)
    increment_counter()
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="items.csv"'},
    )

