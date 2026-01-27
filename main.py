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
  <title>PDF → CSV</title>
  <style>
    :root{
      --bg:#071426;
      --card:#ffffff;
      --text:#13274b;
      --muted:#6c7a90;
      --dash:#d0d9e8;
      --btn:#1e5fd8;
      --btn2:#184fb4;
    }
    *{ box-sizing:border-box; }
    body{
      margin:0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      background: radial-gradient(1200px 600px at 50% 20%, #1d4a8a 0%, var(--bg) 60%);
      color: var(--text);
    }
    .wrap{
      min-height:100vh;
      display:flex;
      flex-direction:column;
      align-items:center;
      justify-content:center;
      gap:22px;
      padding:32px;
    }
    .logo{
      width:260px;
      max-width:70vw;
      height:auto;
      filter: drop-shadow(0 14px 28px rgba(0,0,0,.28));
    }
    .card{
      width:min(760px, 100%);
      background: var(--card);
      border-radius: 22px;
      padding: 36px;
      box-shadow: 0 25px 70px rgba(0,0,0,.45);
      text-align:center;
    }
    h1{
      margin:0 0 18px;
      font-size: 34px;
      letter-spacing:.2px;
      color: var(--text);
    }
    .drop{
      margin: 18px 0 26px;
      border: 2px dashed var(--dash);
      border-radius: 18px;
      padding: 34px 22px;
      background: #fbfcff;
    }
    .drop p{
      margin:0 0 16px;
      color: var(--muted);
      font-size: 16px;
      line-height:1.35;
    }
    .pick-btn{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      gap:10px;
      background: var(--btn);
      color: white;
      padding: 14px 26px;
      border-radius: 12px;
      font-weight: 800;
      cursor: pointer;
      user-select:none;
      box-shadow: 0 10px 25px rgba(30,95,216,.25);
    }
    .pick-btn:hover{ background: var(--btn2); }
    input[type=file]{ display:none; }

    .main-btn{
      width:100%;
      border: none;
      background: linear-gradient(180deg, #2b74ff, #1c56c7);
      color: white;
      padding: 18px;
      border-radius: 14px;
      font-size: 20px;
      font-weight: 900;
      cursor: pointer;
      box-shadow: 0 16px 35px rgba(30,95,216,.28);
    }
    .main-btn:disabled{
      opacity: .55;
      cursor: not-allowed;
      box-shadow:none;
    }
    .status{
      margin-top: 14px;
      min-height: 22px;
      font-size: 14px;
      color: var(--muted);
      white-space: pre-wrap;
    }
    .status.ok{ color: #108a44; }
    .status.err{ color: #c2283a; }

    .counter{
      position: fixed;
      right: 18px;
      bottom: 16px;
      color: #b8c6df;
      font-size: 13px;
      opacity: .95;
      border: 1px solid rgba(255,255,255,.12);
      border-radius: 999px;
      padding: 6px 10px;
      background: rgba(0,0,0,.18);
      backdrop-filter: blur(6px);
    }

    @media (max-width: 520px){
      .card{ padding: 26px; }
      h1{ font-size: 28px; }
      .drop{ padding: 28px 18px; }
      .main-btn{ font-size: 18px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <img class="logo" src="/static/logo.png" alt="Бауцентр" />

    <div class="card" id="dropzone">
      <h1>Конвертация PDF → CSV</h1>

      <div class="drop">
        <p>Перетащите файл PDF сюда или выберите на устройстве</p>
        <label class="pick-btn">
          Выбрать PDF файл
          <input id="pdf" type="file" accept="application/pdf,.pdf" />
        </label>
      </div>

      <button id="btn" class="main-btn" disabled>Скачать CSV</button>
      <div id="status" class="status"></div>
    </div>
  </div>

  <div class="counter" id="counter">Конвертаций: …</div>

  <script>
    const input = document.getElementById('pdf');
    const btn = document.getElementById('btn');
    const statusEl = document.getElementById('status');
    const counterEl = document.getElementById('counter');
    const dropzone = document.getElementById('dropzone');

    function ok(msg){ statusEl.className='status ok'; statusEl.textContent=msg; }
    function err(msg){ statusEl.className='status err'; statusEl.textContent=msg; }
    function neutral(msg){ statusEl.className='status'; statusEl.textContent=msg||''; }

    async function loadCounter(){
      try {
        const r = await fetch('/stats');
        if (!r.ok) return;
        const j = await r.json();
        if (typeof j.conversions === 'number') {
          counterEl.textContent = 'Конвертаций: ' + String(j.conversions);
        }
      } catch(e) {}
    }
    loadCounter();

    function setFile(file){
      if (!file) return;
      const dt = new DataTransfer();
      dt.items.add(file);
      input.files = dt.files;
      btn.disabled = false;
      neutral('Выбран файл: ' + file.name);
    }

    input.addEventListener('change', () => {
      const f = input.files && input.files[0];
      btn.disabled = !f;
      neutral(f ? ('Выбран файл: ' + f.name) : '');
    });

    // Drag & drop
    ['dragenter','dragover'].forEach(ev => {
      dropzone.addEventListener(ev, (e) => {
        e.preventDefault();
        e.stopPropagation();
        dropzone.style.transform = 'translateY(-1px)';
      });
    });
    ['dragleave','drop'].forEach(ev => {
      dropzone.addEventListener(ev, (e) => {
        e.preventDefault();
        e.stopPropagation();
        dropzone.style.transform = 'translateY(0)';
      });
    });
    dropzone.addEventListener('drop', (e) => {
      const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
      if (f) setFile(f);
    });

    btn.addEventListener('click', async () => {
      const f = input.files && input.files[0];
      if (!f) return;

      btn.disabled = true;
      neutral('Обработка…');

      try {
        const fd = new FormData();
        fd.append('file', f);

        const resp = await fetch('/extract', { method: 'POST', body: fd });
        if (!resp.ok) {
          let text = await resp.text();
          try { const j = JSON.parse(text); if (j.detail) text = String(j.detail); } catch(e) {}
          throw new Error(text || ('HTTP ' + resp.status));
        }

        const blob = await resp.blob();
        const base = (f.name || 'items.pdf').replace(/\.pdf$/i, '');
        const filename = base + '.csv';

        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);

        ok('Готово! CSV скачан: ' + filename);
        loadCounter();
      } catch(e) {
        err('Ошибка: ' + String(e.message || e));
      } finally {
        btn.disabled = !(input.files && input.files[0]);
      }
    });
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
