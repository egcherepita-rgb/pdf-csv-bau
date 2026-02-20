import io
import os
import re
import csv
from collections import OrderedDict
from typing import List, Tuple, Dict, Optional
from urllib.parse import quote

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

try:
    import openpyxl
    from openpyxl.utils import get_column_letter
except Exception:
    openpyxl = None


app = FastAPI(title="PDF → CSV/XLSX", version="5.0.0")

# -------------------------
# Static
# -------------------------
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")


# -------------------------
# Regex (close to main(2).py)
# -------------------------
RX_SIZE = re.compile(r"\b\d{2,}[xх×]\d{2,}(?:[xх×]\d{1,})?\b", re.IGNORECASE)
RX_MM = re.compile(r"мм", re.IGNORECASE)
RX_WEIGHT = re.compile(r"\b\d+(?:[.,]\d+)?\s*кг\.?\b", re.IGNORECASE)

RX_MONEY_LINE = re.compile(r"^\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?\s*₽$")
RX_INT = re.compile(r"^\d+$")
RX_ANY_RUB = re.compile(r"₽")

# INLINE: "... 390.00 ₽ 2 780 ₽"
RX_PRICE_QTY_SUM = re.compile(
    r"(?P<price>\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?)\s*₽\s+"
    r"(?P<qty>\d{1,4})\s+"
    r"(?P<sum>\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?)\s*₽",
    re.IGNORECASE,
)

# remove ONLY gabarits with "мм" (keeps 60х40 / 90х40!)
RX_DIMS_MM_ANYWHERE = re.compile(
    r"\s*\d{1,4}[xх×]\d{1,4}(?:[xх×]\d{1,5})?\s*мм\.?\s*",
    re.IGNORECASE,
)

# qty+sum in one line: "8 600 ₽"
RX_QTY_SUM_LINE = re.compile(
    r"^(?P<qty>\d{1,4})\s+(?P<sum>\d+(?:[ \u00a0]\d{3})*(?:[.,]\d+)?)\s*₽$",
    re.IGNORECASE,
)

# headers/totals (light)
RX_HEADER_TOKEN = re.compile(
    r"^(?:фото|товар|габариты|вес|цена\s*за\s*шт|кол-?во|количество|сумма|итого)\b",
    re.IGNORECASE,
)


def normalize_space(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def normalize_key(name: str) -> str:
    """
    IMPORTANT: keep 60х40 / 90х40 in key.
    Remove only "...мм..." gabarits like "607x14x405 мм".
    """
    s = normalize_space(name).lower()
    s = s.replace("×", "x").replace("х", "x")
    s = RX_DIMS_MM_ANYWHERE.sub(" ", s)
    s = normalize_space(s)
    return s


def strip_dims_mm_anywhere(name: str) -> str:
    return normalize_space(RX_DIMS_MM_ANYWHERE.sub(" ", normalize_space(name)))


# -------------------------
# PDF helpers (from main(2).py spirit)
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
    # Lone money line after "Стоимость проекта:" OR huge money line (project total) — skip it as an item anchor
    if not RX_MONEY_LINE.fullmatch(normalize_space(line)):
        return False
    prev = normalize_space(prev_line).lower()
    if "стоимость проекта" in prev:
        return True
    v = money_to_number(line)
    return v >= 10000


def is_header_token(line: str) -> bool:
    low = normalize_space(line).lower().replace("–", "-").replace("—", "-")
    return low in {"фото", "товар", "габариты", "вес", "цена за шт", "кол-во", "сумма", "итого"}


def looks_like_dim_or_weight(line: str) -> bool:
    if RX_WEIGHT.search(line):
        return True
    if RX_SIZE.search(line) and RX_MM.search(line):
        return True
    return False


def looks_like_money_or_qty(line: str) -> bool:
    if RX_MONEY_LINE.fullmatch(normalize_space(line)):
        return True
    if RX_INT.fullmatch(normalize_space(line)):
        return True
    return False


def clean_name_from_buffer(buf: List[str]) -> str:
    filtered: List[str] = []
    for ln in buf:
        if is_noise(ln) or is_header_token(ln) or is_totals_block(ln):
            continue
        filtered.append(ln)

    while filtered and (looks_like_dim_or_weight(filtered[-1]) or looks_like_money_or_qty(filtered[-1])):
        filtered.pop()

    name = normalize_space(" ".join(filtered))
    name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()

    # remove only gabarits with "мм" (keeps 60х40 / 90х40)
    name = strip_dims_mm_anywhere(name)
    return name

# -------------------------
# Art1.xlsx map (Товар / Артикул)
# -------------------------
def load_article_map() -> Tuple[Dict[str, str], str]:
    if openpyxl is None:
        return {}, "openpyxl_not_installed"

    path = os.getenv("ART_XLSX_PATH", "Art1.xlsx")
    if not os.path.exists(path):
        return {}, f"file_not_found:{path}"

    try:
        wb = openpyxl.load_workbook(path, data_only=True)
        ws = wb[wb.sheetnames[0]]
    except Exception as e:
        return {}, f"cannot_open:{e}"

    header = [normalize_space(ws.cell(1, c).value or "") for c in range(1, ws.max_column + 1)]
    товар_col = 1
    арт_col = 2
    for idx, h in enumerate(header, start=1):
        if h.lower() == "товар":
            товар_col = idx
        if h.lower() == "артикул":
            арт_col = idx

    m: Dict[str, str] = {}
    for r in range(2, ws.max_row + 1):
        товар = ws.cell(r, товар_col).value
        арт = ws.cell(r, арт_col).value
        if not товар or арт is None:
            continue

        товар_s = normalize_space(str(товар))
        арт_s = normalize_space(str(арт))

        if not товар_s:
            continue
        # если в справочнике реально 0 — оставим как пусто (чтобы не писать 0 в файл)
        if арт_s in {"", "0"}:
            continue

        m[normalize_key(товар_s)] = арт_s

    return m, "ok"


ARTICLE_MAP, ARTICLE_MAP_STATUS = load_article_map()


def map_article(name: str) -> str:
    return ARTICLE_MAP.get(normalize_key(name), "")


# -------------------------
# Parser (adds embedded-anchor + qty+sum line)
# -------------------------
def parse_items(pdf_bytes: bytes) -> Tuple[List[Tuple[str, int]], Dict]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    total_pages = doc.page_count

    ordered: "OrderedDict[str, int]" = OrderedDict()
    buf: List[str] = []

    stats = {
        "pages": 0,
        "total_pages": total_pages,
        "processed_pages": 0,
        "items_found": 0,
        "anchors_inline": 0,
        "anchors_multiline": 0,
        "anchors_embedded": 0,
        "anchors_qtysum": 0,
        "article_map_status": ARTICLE_MAP_STATUS,
        "article_map_size": len(ARTICLE_MAP),
    }

    for page in doc:
        stats["pages"] += 1
        stats["processed_pages"] += 1

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

            # A) INLINE anchor: "... 390.00 ₽ 2 780 ₽"
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

            # B1) qty+sum in one line right after any price-containing line
            # Example: after "… 75.00 ₽" sometimes comes "8 600 ₽"
            if RX_ANY_RUB.search(line):
                if i + 1 < len(lines):
                    mqs = RX_QTY_SUM_LINE.fullmatch(lines[i + 1])
                    if mqs:
                        try:
                            qty = int(mqs.group("qty"))
                        except Exception:
                            qty = 0
                        if 1 <= qty <= 500:
                            name = clean_name_from_buffer(buf + [line])
                            buf.clear()
                            if name:
                                ordered[name] = ordered.get(name, 0) + qty
                                stats["items_found"] += 1
                                stats["anchors_qtysum"] += 1
                            i += 2
                            continue

            # C) EMBEDDED price anchor:
            # line contains ₽ (even if not "money-only"), next lines are qty and sum
            if RX_ANY_RUB.search(line):
                if i + 2 < len(lines) and RX_INT.fullmatch(lines[i + 1]) and RX_MONEY_LINE.fullmatch(lines[i + 2]):
                    try:
                        qty = int(lines[i + 1])
                    except Exception:
                        qty = 0

                    if 1 <= qty <= 500:
                        name = clean_name_from_buffer(buf + [line])
                        buf.clear()

                        if name:
                            ordered[name] = ordered.get(name, 0) + qty
                            stats["items_found"] += 1
                            stats["anchors_embedded"] += 1

                    i += 3
                    continue

            # B) MULTILINE anchor (money-only line as price line)
            if RX_MONEY_LINE.fullmatch(line):
                end = min(len(lines), i + 10)

                # allow qty+sum line
                qty_idx: Optional[int] = None
                sum_idx: Optional[int] = None
                qty_val: Optional[int] = None

                for j in range(i + 1, end):
                    m_qs = RX_QTY_SUM_LINE.fullmatch(lines[j])
                    if m_qs:
                        try:
                            q = int(m_qs.group("qty"))
                        except Exception:
                            q = 0
                        if 1 <= q <= 500:
                            qty_idx, sum_idx, qty_val = j, j, q
                            break

                if qty_idx is None:
                    for j in range(i + 1, end):
                        if RX_INT.fullmatch(lines[j]):
                            q = int(lines[j])
                            if 1 <= q <= 500:
                                qty_idx, qty_val = j, q
                                break

                    if qty_idx is None:
                        buf.append(line)
                        i += 1
                        continue

                    for j in range(qty_idx + 1, end):
                        if RX_MONEY_LINE.fullmatch(lines[j]):
                            sum_idx = j
                            break

                    if sum_idx is None:
                        buf.append(line)
                        i += 1
                        continue

                name = clean_name_from_buffer(buf)
                buf.clear()

                if name and qty_val:
                    ordered[name] = ordered.get(name, 0) + qty_val
                    stats["items_found"] += 1
                    stats["anchors_multiline"] += 1

                i = (sum_idx + 1) if sum_idx is not None else (qty_idx + 1)
                continue

            buf.append(line)
            i += 1

    return list(ordered.items()), stats


# -------------------------
# Output
# -------------------------
def make_xlsx(rows: List[Tuple[str, int]]) -> bytes:
    if openpyxl is None:
        raise RuntimeError("openpyxl не установлен. Добавьте openpyxl в requirements.txt")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "items"

    ws.append(["Артикул", "ШТ", "Площадь"])
    for name, qty in rows:
        ws.append([map_article(name), qty, ""])

    try:
        widths = [18, 8, 12]
        for col_idx, w in enumerate(widths, start=1):
            ws.column_dimensions[get_column_letter(col_idx)].width = w
    except Exception:
        pass

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()


def make_csv(rows: List[Tuple[str, int]]) -> bytes:
    out = io.StringIO()
    writer = csv.writer(out, delimiter=";", lineterminator="\n")
    writer.writerow(["Артикул", "ШТ", "Площадь"])
    for name, qty in rows:
        writer.writerow([map_article(name), qty, ""])
    return out.getvalue().encode("utf-8-sig")


# -------------------------
# UI (simple)
# -------------------------
INDEX_HTML = r"""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>PDF → CSV / XLSX</title>
  <style>
    :root{
      --bg1:#0f172a; --bg2:#111827; --card: rgba(255,255,255,.08);
      --card2: rgba(255,255,255,.04); --text: rgba(255,255,255,.92);
      --muted: rgba(255,255,255,.7); --accent: #22c55e; --accent2:#16a34a;
      --border: rgba(255,255,255,.15);
    }
    *{box-sizing:border-box}
    body{
      margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      color:var(--text); min-height:100vh;
      background:
        radial-gradient(1200px 800px at 20% 10%, rgba(34,197,94,.22), transparent 60%),
        radial-gradient(900px 700px at 85% 30%, rgba(56,189,248,.18), transparent 55%),
        linear-gradient(145deg, var(--bg1), var(--bg2));
      display:flex; align-items:center; justify-content:center; padding:24px;
    }
    .wrap{width:min(980px, 100%);}
    .topbar{display:flex; align-items:flex-start; justify-content:space-between; gap:16px; margin-bottom:18px;}
    .brand{display:flex; align-items:center; gap:14px;}
    .brand h1{margin:0; font-size:22px;}
    .brand p{margin:4px 0 0 0; color:var(--muted); font-size:13px;}
    .logo{
      width:54px; height:54px; border-radius:14px; background: rgba(255,255,255,.10);
      display:flex; align-items:center; justify-content:center;
      border: 1px solid var(--border); overflow:hidden;
    }
    .logo img{max-width:100%; max-height:100%; display:block;}
    .card{
      background: linear-gradient(180deg, var(--card), var(--card2));
      border: 1px solid var(--border);
      border-radius:18px; padding:18px;
      box-shadow: 0 16px 40px rgba(0,0,0,.35);
    }
    .grid{display:grid; grid-template-columns: 1.25fr .75fr; gap:16px;}
    @media (max-width: 860px){ .grid{grid-template-columns:1fr} .topbar{flex-direction:column; align-items:flex-start} }
    .drop{
      border: 2px dashed rgba(255,255,255,.22);
      border-radius:16px; padding:18px; min-height:180px;
      display:flex; flex-direction:column; justify-content:center; align-items:center;
      gap:10px; text-align:center;
      transition: .15s ease;
    }
    .drop.drag{border-color: rgba(34,197,94,.8); background: rgba(34,197,94,.08); transform: translateY(-1px);}
    .drop input{display:none}
    .btn{
      cursor:pointer; border: 0;
      background: linear-gradient(180deg, var(--accent), var(--accent2));
      color:white; font-weight:700;
      padding:10px 14px; border-radius:12px;
      transition:.15s ease; font-size:14px;
    }
    .btn2{
      background: transparent; border: 1px solid var(--border);
      color:var(--text); font-weight:600;
      padding:10px 12px; border-radius:12px; cursor:pointer;
    }
    .row{display:flex; gap:10px; flex-wrap:wrap; justify-content:center;}
    .muted{color:var(--muted); font-size:13px}
    .side h3{margin:0 0 10px 0; font-size:16px;}
    .side ul{margin:0; padding-left:18px; color:var(--muted); font-size:13px; line-height:1.55;}
    .stat{margin-top:14px; font-size:13px; color:var(--muted); border-top:1px solid var(--border); padding-top:12px; display:none; white-space:pre-wrap;}
    .err{margin-top:12px; color: #fecaca; background: rgba(239,68,68,.12); border: 1px solid rgba(239,68,68,.35); padding:10px 12px; border-radius:12px; display:none; font-size:13px; white-space:pre-wrap;}
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
          <p>Загрузите PDF и скачайте CSV или XLSX</p>
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
          <li>Сопоставляем наименование → Артикул по Art1.xlsx</li>
          <li>Формируем CSV (; ) или XLSX</li>
        </ul>
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
    if(!msg){ errBox.style.display='none'; errBox.textContent=''; return; }
    errBox.style.display='block'; errBox.textContent=msg;
  }
  function setStat(msg){
    if(!msg){ statBox.style.display='none'; statBox.textContent=''; return; }
    statBox.style.display='block'; statBox.textContent=msg;
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
    drop.addEventListener(ev, (e)=>{ prevent(e); drop.classList.add('drag'); });
  });
  ['dragleave','drop'].forEach(ev=>{
    drop.addEventListener(ev, (e)=>{ prevent(e); drop.classList.remove('drag'); });
  });
  drop.addEventListener('drop', (e)=>{
    const f = e.dataTransfer.files && e.dataTransfer.files[0];
    if(f){ fileInput.value=''; setFile(f); }
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
      if(stat) setStat(decodeURIComponent(stat)); else setStat('');
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
      if(!res.ok){ setError(txt || ('Ошибка: ' + res.status)); setStat(''); return; }
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


@app.get("/", response_class=HTMLResponse)
def index():
    return INDEX_HTML


@app.post("/api/reload-map")
def reload_map():
    global ARTICLE_MAP, ARTICLE_MAP_STATUS
    ARTICLE_MAP, ARTICLE_MAP_STATUS = load_article_map()
    return f"{ARTICLE_MAP_STATUS}; rows={len(ARTICLE_MAP)}"


@app.post("/api/convert/csv")
async def convert_csv(file: UploadFile = File(...)):
    if file.content_type not in ("application/pdf", "application/octet-stream"):
        raise HTTPException(status_code=400, detail="Нужен PDF")

    pdf_bytes = await file.read()
    rows, stats = parse_items(pdf_bytes)
    data = make_csv(rows)

    stats_str = (
        f"pages={stats.get('pages')}\n"
        f"items_found={stats.get('items_found')}\n"
        f"anchors_inline={stats.get('anchors_inline')}\n"
        f"anchors_multiline={stats.get('anchors_multiline')}\n"
        f"anchors_embedded={stats.get('anchors_embedded')}\n"
        f"anchors_qtysum={stats.get('anchors_qtysum')}\n"
        f"article_map_status={stats.get('article_map_status')}\n"
        f"article_map_size={stats.get('article_map_size')}\n"
    )

    headers = {
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote('result.csv')}",
        "X-Parse-Stats": quote(stats_str),
    }
    return Response(content=data, media_type="text/csv", headers=headers)


@app.post("/api/convert/xlsx")
async def convert_xlsx(file: UploadFile = File(...)):
    if file.content_type not in ("application/pdf", "application/octet-stream"):
        raise HTTPException(status_code=400, detail="Нужен PDF")

    pdf_bytes = await file.read()
    rows, stats = parse_items(pdf_bytes)
    data = make_xlsx(rows)

    stats_str = (
        f"pages={stats.get('pages')}\n"
        f"items_found={stats.get('items_found')}\n"
        f"anchors_inline={stats.get('anchors_inline')}\n"
        f"anchors_multiline={stats.get('anchors_multiline')}\n"
        f"anchors_embedded={stats.get('anchors_embedded')}\n"
        f"anchors_qtysum={stats.get('anchors_qtysum')}\n"
        f"article_map_status={stats.get('article_map_status')}\n"
        f"article_map_size={stats.get('article_map_size')}\n"
    )

    headers = {
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote('result.xlsx')}",
        "X-Parse-Stats": quote(stats_str),
    }
    return Response(
        content=data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
