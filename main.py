import io
import os
import re
import csv
from collections import OrderedDict
from typing import List, Tuple, Dict

from urllib.parse import quote
import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

try:
    import openpyxl
except Exception:
    openpyxl = None

# -------------------------
# App
# -------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# Data / Art1.xlsx map
# -------------------------
ARTICLE_MAP: Dict[str, str] = {}
ARTICLE_MAP_STATUS = "empty"
ARTICLE_VALUE_COLUMN = "A"  # default

def normalize_space(s: str) -> str:
    if s is None:
        return ""
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_key(s: str) -> str:
    s = normalize_space(s).lower()
    s = s.replace("ё", "е")
    s = re.sub(r"[–—−]", "-", s)
    s = re.sub(r"[“”«»\"]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def try_load_article_map() -> None:
    """
    Считываем Art1.xlsx, ожидаем:
      - колонка A: Артикул
      - колонка B: Наименование
    """
    global ARTICLE_MAP, ARTICLE_MAP_STATUS, ARTICLE_VALUE_COLUMN

    ARTICLE_MAP = {}
    ARTICLE_MAP_STATUS = "empty"
    ARTICLE_VALUE_COLUMN = "A"

    # Пытаемся найти Art1.xlsx рядом с main.py, в текущей папке или в /root
    candidates = [
        os.path.join(os.path.dirname(__file__), "Art1.xlsx"),
        os.path.join(os.getcwd(), "Art1.xlsx"),
        "/root/Art1.xlsx",
        "/root/app/Art1.xlsx",
        "/home/app/Art1.xlsx",
    ]

    xlsx_path = None
    for c in candidates:
        if os.path.exists(c):
            xlsx_path = c
            break

    if xlsx_path is None:
        ARTICLE_MAP_STATUS = "Art1.xlsx not found"
        return

    if openpyxl is None:
        ARTICLE_MAP_STATUS = "openpyxl not installed"
        return

    try:
        wb = openpyxl.load_workbook(xlsx_path, data_only=True)
        ws = wb.active

        # Попробуем автоматически определить колонку с артикулом (A/C/D...) и наименованием (B/...)
        # Но по умолчанию считаем A=Артикул, B=Наименование
        # Если в первой строке есть заголовки, попробуем найти по ним.
        headers = []
        for cell in ws[1]:
            headers.append(str(cell.value).strip().lower() if cell.value is not None else "")

        # поиск по заголовкам
        name_col = None
        art_col = None

        for idx, h in enumerate(headers, start=1):
            if h in ("наименование", "товар", "название", "наим."):
                name_col = idx
            if h in ("артикул", "код", "id", "sku"):
                art_col = idx

        if name_col is None:
            name_col = 2  # B
        if art_col is None:
            art_col = 1  # A

        # сохранить букву для инфо
        ARTICLE_VALUE_COLUMN = openpyxl.utils.get_column_letter(art_col)

        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row:
                continue

            art = row[art_col - 1] if art_col - 1 < len(row) else None
            name = row[name_col - 1] if name_col - 1 < len(row) else None
            if name is None:
                continue

            name_key = normalize_key(str(name))
            if not name_key:
                continue

            art_str = "" if art is None else str(art).strip()
            ARTICLE_MAP[name_key] = art_str

        ARTICLE_MAP_STATUS = f"loaded: {len(ARTICLE_MAP)} rows from {os.path.basename(xlsx_path)}"
    except Exception as e:
        ARTICLE_MAP_STATUS = f"failed to load Art1.xlsx: {e}"

try_load_article_map()

# -------------------------
# Parsing helpers
# -------------------------
RX_NOISE = re.compile(
    r"^(?:"
    r"проект|итого|сумма|итог|всего|итоговая|скидка|доставка|монтаж|"
    r"страница|лист|дата|номер|тел|e-?mail|адрес|менеджер|клиент|"
    r"промет|praktik|home|гардеробн|система|"
    r"кол-?во|количество|цена|стоимость|"
    r")\b",
    re.IGNORECASE,
)

RX_HEADER_TOKEN = re.compile(
    r"^(?:наименование|товар|цена|кол-?во|количество|сумма|итого)\b",
    re.IGNORECASE,
)

RX_TOTALS_BLOCK = re.compile(
    r"^(?:"
    r"итого(?:\s+по\s+проекту)?|всего|итоговая\s+стоимость|сумма\s+по\s+проекту|"
    r"в\s+том\s+числе|ндс|без\s+ндс|налог|итого\s+со\s+скидкой|"
    r")\b",
    re.IGNORECASE,
)

RX_PROJECT_TOTAL_ONLY = re.compile(
    r"^(?:итого(?:\s+по\s+проекту)?)$",
    re.IGNORECASE,
)

# Размеры/маркеры, которые часто встречаются отдельной строкой (шум)
RX_DIMS = re.compile(r"\b\d{2,}[xх×]\d{2,}(?:[xх×]\d{1,})?\b", re.IGNORECASE)
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

# После цены иногда PyMuPDF отдает "кол-во + сумма" одной строкой: "8 600 ₽"
RX_QTY_SUM_LINE = re.compile(
    r"^(?P<qty>\d{1,4})\s+(?P<sum>\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?)\s*₽$",
    re.IGNORECASE,
)

# Размеры где угодно в строке: "30x10 мм", "600×300 мм" и т.п.
RX_DIM_ANYWHERE = re.compile(
    r"\b\d{2,}\s*[xх×]\s*\d{2,}(?:\s*[xх×]\s*\d{1,})?\b",
    re.IGNORECASE,
)

def is_noise(line: str) -> bool:
    if not line:
        return True
    if RX_NOISE.match(line):
        return True
    # отдельные строки типа "600x400", "мм", "2.5 кг" часто попадают как мусор
    if RX_DIMS.search(line) and len(line) <= 20:
        return True
    if RX_MM.search(line) and len(line) <= 12:
        return True
    if RX_WEIGHT.search(line) and len(line) <= 12:
        return True
    return False

def is_header_token(line: str) -> bool:
    return bool(RX_HEADER_TOKEN.match(line or ""))

def is_totals_block(line: str) -> bool:
    return bool(RX_TOTALS_BLOCK.match(line or ""))

def is_project_total_only(line: str, prev_line: str = "") -> bool:
    # когда в таблице встречается "Итого" и ниже идут суммы — считаем это стоп-блоком
    if RX_PROJECT_TOTAL_ONLY.match(line or ""):
        return True
    # иногда "Итого" идет в предыдущей строке
    if RX_PROJECT_TOTAL_ONLY.match(prev_line or "") and RX_ANY_RUB.search(line or ""):
        return True
    return False

def clean_name_from_buffer(buf: List[str]) -> str:
    """
    Собираем наименование из буфера до якоря (цена/кол-во/сумма),
    удаляя хвостовые числовые/денежные куски, если они попали.
    """
    if not buf:
        return ""

    parts: List[str] = []
    for x in buf:
        x = normalize_space(x)
        if not x:
            continue
        # выкидываем чистые деньги или цифры
        if RX_MONEY_LINE.fullmatch(x):
            continue
        if RX_INT.fullmatch(x):
            continue
        # выкидываем элементы заголовка
        if is_header_token(x):
            continue
        # иногда отдельной строкой встречается "₽"
        if x.strip() == "₽":
            continue
        parts.append(x)

    name = normalize_space(" ".join(parts))

    # удаляем возможный хвост с ценой/суммой
    name = re.sub(r"\s+\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?\s*₽\s*$", "", name).strip()

    return name

# -------------------------
# PDF parsing
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

                # 1) Частый кейс: "кол-во + сумма" идут одной строкой (например: "8 600 ₽")
                qty_idx = None
                sum_idx = None
                for j in range(i + 1, end):
                    m_qs = RX_QTY_SUM_LINE.fullmatch(lines[j])
                    if m_qs:
                        try:
                            q = int(m_qs.group("qty"))
                        except Exception:
                            q = 0
                        if 1 <= q <= 500:
                            qty_idx = j
                            sum_idx = j
                            break

                # 2) Fallback: кол-во отдельной строкой, сумма отдельной строкой
                if qty_idx is None:
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
                    if sum_idx == qty_idx:
                        # qty+sum были в одной строке
                        m_qs = RX_QTY_SUM_LINE.fullmatch(lines[qty_idx])
                        try:
                            qty = int(m_qs.group("qty")) if m_qs else 0
                        except Exception:
                            qty = 0
                    else:
                        qty = int(lines[qty_idx])

                    if 1 <= qty <= 500:
                        ordered[name] = ordered.get(name, 0) + qty
                        stats["items_found"] += 1
                        stats["anchors_multiline"] += 1

                i = sum_idx + 1
                continue

            buf.append(line)
            i += 1

    return list(ordered.items()), stats


# -------------------------
# Excel output (.xlsx)
# -------------------------
def make_xlsx(rows: List[Tuple[str, int]]) -> bytes:
    """
    Формат:
      A: Артикул  (берем из ARTICLE_MAP по наименованию)
      B: ШТ       (количество)
      C: Площадь  (пусто)
    """
    if openpyxl is None:
        raise RuntimeError("openpyxl не установлен. Добавьте openpyxl в requirements.txt")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "items"

    ws.append(["Артикул", "ШТ", "Площадь"])

    for name, qty in rows:
        article_raw = ARTICLE_MAP.get(normalize_key(name), "")
        # Если артикул состоит только из цифр и не слишком длинный — пишем числом,
        # иначе оставляем текст (чтобы не терять точность/нули).
        article_val = article_raw
        if isinstance(article_raw, str):
            a = article_raw.strip()
            if a.isdigit() and len(a) <= 15:
                try:
                    article_val = int(a)
                except Exception:
                    article_val = a
            else:
                article_val = a
        ws.append([article_val, qty, ""])

    # Чуть удобнее читать (не обязательно)
    try:
        widths = [18, 8, 12]
        for col_idx, w in enumerate(widths, start=1):
            col_letter = openpyxl.utils.get_column_letter(col_idx)
            ws.column_dimensions[col_letter].width = w
    except Exception:
        pass

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()


# -------------------------
# CSV output (;)
# -------------------------
def make_csv(rows: List[Tuple[str, int]]) -> bytes:
    """
    CSV; separator=; in Windows-friendly format.
    Columns:
      Артикул;ШТ;Площадь
    """
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";", lineterminator="\n")

    writer.writerow(["Артикул", "ШТ", "Площадь"])
    for name, qty in rows:
        article = ARTICLE_MAP.get(normalize_key(name), "")
        writer.writerow([article, qty, ""])

    return output.getvalue().encode("utf-8-sig")


# -------------------------
# UI (single-file HTML)
# -------------------------
HTML = r"""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>PDF → CSV / XLSX</title>
  <style>
    :root{
      --bg1:#0f172a;
      --bg2:#111827;
      --card: rgba(255,255,255,.08);
      --card2: rgba(255,255,255,.04);
      --text: rgba(255,255,255,.92);
      --muted: rgba(255,255,255,.7);
      --accent: #22c55e;
      --accent2:#16a34a;
      --danger:#ef4444;
      --border: rgba(255,255,255,.15);
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      color:var(--text);
      min-height:100vh;
      background:
        radial-gradient(1200px 800px at 20% 10%, rgba(34,197,94,.22), transparent 60%),
        radial-gradient(900px 700px at 85% 30%, rgba(56,189,248,.18), transparent 55%),
        linear-gradient(145deg, var(--bg1), var(--bg2));
      display:flex;
      align-items:center;
      justify-content:center;
      padding:24px;
    }
    .wrap{
      width:min(980px, 100%);
    }
    .topbar{
      display:flex;
      align-items:flex-start;
      justify-content:space-between;
      gap:16px;
      margin-bottom:18px;
    }
    .brand{
      display:flex;
      align-items:center;
      gap:14px;
    }
    .brand h1{
      margin:0;
      font-size:22px;
      letter-spacing:.2px;
    }
    .brand p{
      margin:4px 0 0 0;
      color:var(--muted);
      font-size:13px;
    }
    .logo{
      width:54px;
      height:54px;
      border-radius:14px;
      background: rgba(255,255,255,.10);
      display:flex;
      align-items:center;
      justify-content:center;
      border: 1px solid var(--border);
      overflow:hidden;
    }
    .logo img{
      max-width:100%;
      max-height:100%;
      display:block;
    }
    .card{
      background: linear-gradient(180deg, var(--card), var(--card2));
      border: 1px solid var(--border);
      border-radius:18px;
      padding:18px;
      box-shadow: 0 16px 40px rgba(0,0,0,.35);
    }
    .grid{
      display:grid;
      grid-template-columns: 1.25fr .75fr;
      gap:16px;
    }
    @media (max-width: 860px){
      .grid{grid-template-columns:1fr}
      .topbar{flex-direction:column; align-items:flex-start}
    }
    .drop{
      border: 2px dashed rgba(255,255,255,.22);
      border-radius:16px;
      padding:18px;
      min-height:180px;
      display:flex;
      flex-direction:column;
      justify-content:center;
      align-items:center;
      gap:10px;
      text-align:center;
      transition: .15s ease;
    }
    .drop.drag{
      border-color: rgba(34,197,94,.8);
      background: rgba(34,197,94,.08);
      transform: translateY(-1px);
    }
    .drop input{display:none}
    .btn{
      cursor:pointer;
      border: 0;
      background: linear-gradient(180deg, var(--accent), var(--accent2));
      color:white;
      font-weight:700;
      padding:10px 14px;
      border-radius:12px;
      transition:.15s ease;
      font-size:14px;
    }
    .btn:hover{filter:brightness(1.03)}
    .btn:active{transform: translateY(1px)}
    .btn2{
      background: transparent;
      border: 1px solid var(--border);
      color:var(--text);
      font-weight:600;
      padding:10px 12px;
      border-radius:12px;
      cursor:pointer;
    }
    .row{
      display:flex;
      gap:10px;
      flex-wrap:wrap;
      justify-content:center;
    }
    .muted{color:var(--muted); font-size:13px}
    .side h3{
      margin:0 0 10px 0;
      font-size:16px;
      letter-spacing:.15px;
    }
    .side ul{
      margin:0;
      padding-left:18px;
      color:var(--muted);
      font-size:13px;
      line-height:1.55;
    }
    .stat{
      margin-top:14px;
      font-size:13px;
      color:var(--muted);
      border-top:1px solid var(--border);
      padding-top:12px;
      display:none;
      white-space:pre-wrap;
    }
    .err{
      margin-top:12px;
      color: #fecaca;
      background: rgba(239,68,68,.12);
      border: 1px solid rgba(239,68,68,.35);
      padding:10px 12px;
      border-radius:12px;
      display:none;
      font-size:13px;
      white-space:pre-wrap;
    }
    .footer{
      margin-top:14px;
      text-align:center;
      color:rgba(255,255,255,.55);
      font-size:12px;
    }
    a{color:inherit}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="brand">
        <div class="logo">
          <img src="/static/logo.png" alt="logo" onerror="this.style.display='none'"/>
        </div>
        <div>
          <h1>PDF → CSV / XLSX</h1>
          <p>Загрузите PDF (из 3D конфигуратора) и скачайте CSV или XLSX</p>
        </div>
      </div>

      <div class="row">
        <button class="btn2" id="reloadMapBtn" title="Перезагрузить Art1.xlsx">Перезагрузить справочник</button>
      </div>
    </div>

    <div class="card grid">
      <div>
        <div class="drop" id="drop">
          <div style="font-size:15px; font-weight:700;">Перетащите PDF сюда</div>
          <div class="muted">или выберите файл вручную</div>
          <div class="row" style="margin-top:8px">
            <label class="btn">
              Выбрать PDF
              <input type="file" id="file" accept="application/pdf" />
            </label>
            <button class="btn2" id="clearBtn">Очистить</button>
          </div>
          <div class="muted" id="fileName" style="margin-top:6px"></div>
        </div>

        <div class="row" style="margin-top:14px">
          <button class="btn" id="csvBtn" disabled>Скачать CSV</button>
          <button class="btn" id="xlsxBtn" disabled>Скачать XLSX</button>
        </div>

        <div class="err" id="err"></div>
        <div class="stat" id="stat"></div>
      </div>

      <div class="side">
        <h3>Как это работает</h3>
        <ul>
          <li>Достаём текст из PDF</li>
          <li>Ищем позиции по якорям «цена → кол-во → сумма»</li>
          <li>Сопоставляем наименование → артикул по Art1.xlsx</li>
          <li>Формируем CSV (; ) или XLSX</li>
        </ul>
        <div class="footer">
          <div>Разделитель CSV: <b>;</b> (удобно для Excel)</div>
          <div style="margin-top:6px;">Сервис: <a href="https://pdfcsv.ru/" target="_blank" rel="noreferrer">pdfcsv.ru</a></div>
        </div>
      </div>
    </div>
  </div>

<script>
  const fileInput = document.getElementById('file');
  const drop = document.getElementById('drop');
  const fileName = document.getElementById('fileName');
  const csvBtn = document.getElementById('csvBtn');
  const xlsxBtn = document.getElementById('xlsxBtn');
  const clearBtn = document.getElementById('clearBtn');
  const errBox = document.getElementById('err');
  const statBox = document.getElementById('stat');
  const reloadMapBtn = document.getElementById('reloadMapBtn');

  let currentFile = null;

  function setError(msg){
    if(!msg){
      errBox.style.display='none';
      errBox.textContent='';
      return;
    }
    errBox.style.display='block';
    errBox.textContent=msg;
  }
  function setStat(msg){
    if(!msg){
      statBox.style.display='none';
      statBox.textContent='';
      return;
    }
    statBox.style.display='block';
    statBox.textContent=msg;
  }

  function setFile(f){
    currentFile = f;
    if(f){
      fileName.textContent = f.name;
      csvBtn.disabled = false;
      xlsxBtn.disabled = false;
    } else {
      fileName.textContent = '';
      csvBtn.disabled = true;
      xlsxBtn.disabled = true;
      setError('');
      setStat('');
    }
  }

  fileInput.addEventListener('change', (e)=>{
    const f = e.target.files && e.target.files[0];
    setFile(f || null);
  });

  clearBtn.addEventListener('click', ()=>{
    fileInput.value='';
    setFile(null);
  });

  function prevent(e){ e.preventDefault(); e.stopPropagation(); }

  ['dragenter','dragover'].forEach(ev=>{
    drop.addEventListener(ev, (e)=>{
      prevent(e);
      drop.classList.add('drag');
    });
  });
  ['dragleave','drop'].forEach(ev=>{
    drop.addEventListener(ev, (e)=>{
      prevent(e);
      drop.classList.remove('drag');
    });
  });
  drop.addEventListener('drop', (e)=>{
    const f = e.dataTransfer.files && e.dataTransfer.files[0];
    if(f){
      fileInput.value='';
      setFile(f);
    }
  });

  async function uploadAndDownload(endpoint){
    if(!currentFile) return;
    setError('');
    setStat('Обработка...');

    const fd = new FormData();
    fd.append('file', currentFile);

    try{
      const res = await fetch(endpoint, { method:'POST', body: fd });
      const stat = res.headers.get('X-Parse-Stats');
      if(stat){
        setStat(decodeURIComponent(stat));
      } else {
        setStat('');
      }
      if(!res.ok){
        const text = await res.text();
        setError(text || ('Ошибка: ' + res.status));
        return;
      }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);

      const a = document.createElement('a');
      a.href = url;
      const cd = res.headers.get('Content-Disposition') || '';
      let fn = '';
      const m = /filename\\*=UTF-8''([^;]+)/.exec(cd);
      if(m) fn = decodeURIComponent(m[1]);
      a.download = fn || (endpoint.includes('csv') ? 'result.csv' : 'result.xlsx');
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch(err){
      setError('Ошибка сети: ' + err);
      setStat('');
    }
  }

  csvBtn.addEventListener('click', ()=>uploadAndDownload('/api/convert/csv'));
  xlsxBtn.addEventListener('click', ()=>uploadAndDownload('/api/convert/xlsx'));

  reloadMapBtn.addEventListener('click', async ()=>{
    setError('');
    setStat('Перезагрузка справочника...');
    try{
      const res = await fetch('/api/reload-map', { method:'POST' });
      const txt = await res.text();
      if(!res.ok){
        setError(txt || ('Ошибка: ' + res.status));
        setStat('');
        return;
      }
      setStat(txt);
    } catch(err){
      setError('Ошибка сети: ' + err);
      setStat('');
    }
  });
</script>
</body>
</html>
"""


# -------------------------
# Static (logos)
# -------------------------
from fastapi.staticfiles import StaticFiles

static_dir = os.path.join(os.path.dirname(__file__), "static")
if not os.path.exists(static_dir):
    # на всякий случай — создадим пустую папку, чтобы mount не падал
    os.makedirs(static_dir, exist_ok=True)

app.mount("/static", StaticFiles(directory=static_dir), name="static")


# -------------------------
# Routes
# -------------------------
@app.get("/", response_class=HTMLResponse)
def index():
    return HTML

@app.post("/api/reload-map")
def reload_map():
    try_load_article_map()
    return f"{ARTICLE_MAP_STATUS}. value column: {ARTICLE_VALUE_COLUMN}"

@app.post("/api/convert/csv")
async def convert_csv(file: UploadFile = File(...)):
    pdf_bytes = await file.read()
    rows, stats = parse_items(pdf_bytes)

    data = make_csv(rows)

    stats_str = (
        f"pages={stats.get('pages')}\\n"
        f"items_found={stats.get('items_found')}\\n"
        f"anchors_inline={stats.get('anchors_inline')}\\n"
        f"anchors_multiline={stats.get('anchors_multiline')}\\n"
        f"article_map_status={stats.get('article_map_status')}\\n"
        f"article_map_size={stats.get('article_map_size')}\\n"
        f"article_value_column={stats.get('article_value_column')}\\n"
    )

    headers = {
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote('result.csv')}",
        "X-Parse-Stats": quote(stats_str),
    }
    return StreamingResponse(io.BytesIO(data), media_type="text/csv", headers=headers)

@app.post("/api/convert/xlsx")
async def convert_xlsx(file: UploadFile = File(...)):
    pdf_bytes = await file.read()
    rows, stats = parse_items(pdf_bytes)

    data = make_xlsx(rows)

    stats_str = (
        f"pages={stats.get('pages')}\\n"
        f"items_found={stats.get('items_found')}\\n"
        f"anchors_inline={stats.get('anchors_inline')}\\n"
        f"anchors_multiline={stats.get('anchors_multiline')}\\n"
        f"article_map_status={stats.get('article_map_status')}\\n"
        f"article_map_size={stats.get('article_map_size')}\\n"
        f"article_value_column={stats.get('article_value_column')}\\n"
    )

    headers = {
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote('result.xlsx')}",
        "X-Parse-Stats": quote(stats_str),
    }
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
