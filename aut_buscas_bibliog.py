"""
Levantamento bibliográfico unificado (SciELO, OpenAlex, Crossref, BDTD) → JSON único
Versão com TESAURO + FORMAS DE BUSCA + TEMPORIZADOR + CHECKPOINTS + RESUME + NDJSON streaming.

Requisitos:
  pip install requests beautifulsoup4
"""

import os
import re
import json
import time
import argparse
import datetime as dt
import random
import unicodedata
import signal
import html  
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import urljoin, urlparse, quote

import requests
from bs4 import BeautifulSoup

# ============ Configuração global ============
USER_AGENT = os.getenv("QS_USER_AGENT", "quadro_sintese/1.6 (+https://example.org)")
TIMEOUT = 30
REQUEST_DELAY = 1.2  # atraso padrão entre requisições

OUTPUT_JSON = "resultado_busca_multi10.json"
OUTPUT_NDJSON = "resultado_busca_multi10.jsonl"

# Limites padrão (ajustáveis via CLI)
SCIELO_MAX_PAGES = 2
SCIELO_ENRICH_MAX = 6
SCIELO_ENRICH_TIMEOUT = 5.0

OPENALEX_PER_PAGE = 25
OPENALEX_MAX_PAGES = 3

CROSSREF_ROWS = 40
CROSSREF_MAX_PAGES = 3

BDTD_LIMIT_PER_PAGE = 20
BDTD_MAX_PAGES = 2
BDTD_ENRICH_MAX = 10
BDTD_ENRICH_TIMEOUT = 6.0

# Endpoints BDTD
BDTD_HOST = "https://bdtd.ibict.br"
BDTD_API_BASE = f"{BDTD_HOST}/vufind/api/v1/search"

# ============ DESCRITORES (bases) ============
DESCRITORES_BASE_1 = [
    "Parque Nacional da Serra da Capivara",
    "Serra da Capivara",
    "PNSC",
    "São Raimundo Nonato Piauí",
    "FUMDHAM",
    "ICMBio Parque Nacional Serra da Capivara",
    "IPHAN Serra da Capivara",
    "ICOMOS Serra da Capivara",
    "UNESCO World Heritage Serra da Capivara",
    "Decreto 83.548/1979 Serra da Capivara",
    "SNUC unidade de conservação Parque Nacional",
    "plano de manejo Parque Nacional Serra da Capivara",
    "licenciamento ambiental patrimônio arqueológico",
    "zona de amortecimento PNSC",
    "tombamento arqueológico IPHAN Piauí",
    "paisagem cultural Serra da Capivara",
    "gestão compartilhada IPHAN ICMBio FUMDHAM",
    "conselho consultivo PNSC",
    "regularização fundiária Parque Nacional da Serra da Capivara",
    "arte rupestre Piauí",
    "registro rupestre Nordeste",
    "ecoturismo Serra da Capivara",
    "turismo sustentável PNSC",
    "conflitos socioambientais PNSC",
]

DESCRITORES_BASE_2 = [
    "Parque Nacional da Serra da Capivara",
    "Unidades de Conservação",
    "Áreas de Proteção Integral",
    "Planos de Manejo",
    "Sobreposição de unidades de conservação",
    "Patrimônio arqueológico",
    "Sítios arqueológicos",
    "Sítios pré-históricos",
    "Patrimônio cultural (material e imaterial)",
    "Patrimônio mundial (UNESCO)",
    "Arqueologia do Nordeste",
    "Instrumentos legais de proteção ambiental",
    "Sobreposição de normas legais",
    "Proteção legal do patrimônio arqueológico",
    "Legislação arqueológica no Brasil",
    "Tombamento de sítios arqueológicos",
    "IPHAN (Instituto do Patrimônio Histórico e Artístico Nacional)",
    "Política Nacional do Meio Ambiente",
    "Leis de proteção ambiental e cultural",
    "Conflitos de competência ambiental",
    "Gestão participativa",
    "Políticas públicas de preservação",
    "Conflitos de gestão em áreas protegidas",
    "Fiscalização ambiental e cultural",
    "Conservação de sítios arqueológicos",
    "Desenvolvimento sustentável e conservação",
    "Modelos de governança em parques nacionais",
    "Turismo arqueológico",
    "Comunidades locais e preservação",
    "Desenvolvimento local sustentável",
    "Educação patrimonial",
    "Participação social na gestão de parques",
    "Estudos de caso no PNSC",
    "Análise de políticas públicas (policy analysis)",
    "Pesquisa de campo em arqueologia e conservação",
    "Análise comparativa de instrumentos legais",
    "Conflitos entre legislação federal e estadual",
    "Falta de objetividade nas normas de proteção",
    "Insegurança jurídica em unidades de conservação",
    "Harmonização de instrumentos legais",
    "Propostas de revisão legal e institucional"
]

# Dicionário simples de variantes (default). Será mesclado com o tesauro e/ou --variants-file
VARIANT_MAP_DEFAULT = {
    "zona de amortecimento": ["buffer zone", "zona tampão"],
    "plano de manejo": ["management plan", "plano de gestão"],
    "tombamento": ["heritage listing", "registro do patrimônio", "listing"],
    "SNUC": ["Sistema Nacional de Unidades de Conservação", "Brazilian National System of Protected Areas"],
    "patrimônio mundial": ["world heritage", "patrimônio da humanidade"],
    "patrimônio arqueológico": ["archaeological heritage"],
    "arte rupestre": ["rock art"],
    "sítio arqueológico": ["archaeological site"],
    "paisagem cultural": ["cultural landscape"],
}

# ============ Utilidades comuns ============
DOI_RE = re.compile(r'\b10\.\d{4,9}/[^\s"<>]+', re.I)

STOP_REQUESTED = False
def _signal_handler(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    print(f"\n[AVISO] Sinal {signum} recebido. Finalizando com checkpoint...")
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

def normalize_title(t: str) -> str:
    t = re.sub(r"\s+", " ", (t or "")).strip().lower()
    t = re.sub(r"[^\w\sáéíóúàâêôãõç-]", "", t, flags=re.I)
    return t

def strip_accents(s: str) -> str:
    if not s: return s
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def clean_text(a: Optional[str]) -> str:
    """
    Limpa texto:
      1) html.unescape para converter entidades (&lt;, &gt;, &amp;, &#179; etc.)
      2) remove tags reais <...>
      3) normaliza espaços
    """
    if not a:
        return ""
    a = html.unescape(a)                    
    a = re.sub(r"<[^>]+>", " ", a)
    a = re.sub(r"\s+", " ", a).strip()
    return a

def sleep_with_jitter(base: float):
    time.sleep(base + random.uniform(0, base * 0.3))

def http_get(url: str, headers: dict, timeout: int, retries: int = 2,
             backoff: float = 1.6, debug: bool = False,
             params: Optional[dict] = None) -> Optional[requests.Response]:
    last_err = None
    for attempt in range(1, retries + 1):
        if STOP_REQUESTED: return None
        try:
            if debug:
                print(f"  [GET] {url} (try {attempt}/{retries}) params={params or {}}")
            r = requests.get(url, headers=headers, timeout=timeout, params=params)
            if r.status_code == 200:
                return r
            if debug:
                print(f"  [GET] status={r.status_code} body={r.text[:200]!r}")
            if r.status_code in (403, 429, 500, 502, 503, 504):
                sleep_with_jitter(backoff ** attempt)
                continue
            return None
        except requests.RequestException as e:
            last_err = e
            if debug:
                print(f"  [GET] erro: {e}")
            sleep_with_jitter(backoff ** attempt)
    if debug and last_err:
        print(f"  [GET] falhou após {retries} tentativas: {last_err}")
    return None

def pick_doi_from(*vals: Any) -> str:
    def scan(v):
        if v is None:
            return None
        if isinstance(v, dict):
            for k in v.values():
                r = scan(k)
                if r: return r
        elif isinstance(v, list):
            for k in v:
                r = scan(k)
                if r: return r
        else:
            m = DOI_RE.search(str(v))
            if m: return m.group(0)
        return None
    for val in vals:
        r = scan(val)
        if r: return r.strip().rstrip('.,;')
    return ""

def pick_year_from(*vals: Any) -> Optional[int]:
    for v in vals:
        if v is None: continue
        if isinstance(v, int) and 1800 <= v <= 2100:
            return v
        if isinstance(v, (list, tuple)):
            for it in v:
                y = pick_year_from(it)
                if y: return y
        if isinstance(v, dict):
            for it in v.values():
                y = pick_year_from(it)
                if y: return y
        s = str(v)
        m = re.search(r'\b(18|19|20)\d{2}\b', s)
        if m: return int(m.group())
    return None

def normalize_authors(raw) -> str:
    names: List[str] = []
    if raw is None: return ""
    if isinstance(raw, str): return clean_text(raw)
    if isinstance(raw, list):
        for it in raw:
            if isinstance(it, dict):
                nm = it.get("name") or it.get("label") or it.get("display_name") or ""
                if nm: names.append(str(nm))
            else:
                if str(it).strip(): names.append(str(it))
        return "; ".join([clean_text(n) for n in names if n])
    if isinstance(raw, dict):
        def pull(obj):
            if obj is None: return
            if isinstance(obj, dict):
                for k in ("name", "display_name"):
                    if obj.get(k): names.append(str(obj[k]))
                for k in obj.keys():
                    if k not in ("name", "display_name"):
                        val = obj.get(k)
                        if isinstance(val, str) and val.strip():
                            names.append(val)
            elif isinstance(obj, list):
                for x in obj: pull(x)
            elif isinstance(obj, str):
                if obj.strip(): names.append(obj)
        pull(raw)
        return "; ".join([clean_text(n) for n in names if n])
    return clean_text(str(raw))

# ===== Tesauro & Variantes =====
def load_thesaurus_json(path: Optional[str]) -> Optional[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def build_variant_map_from_thesaurus(th: Dict[str, Any]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    if not th or "terms" not in th:
        return out
    for term in th["terms"]:
        pref_pt = ((term.get("pref") or {}).get("pt") or "").strip()
        if not pref_pt:
            continue
        variants: List[str] = []
        pref_en = (term.get("pref") or {}).get("en") or ""
        if pref_en:
            variants.append(pref_en.strip())
        for lang in ("pt", "en"):
            alts = (term.get("alt") or {}).get(lang) or []
            for a in alts:
                a = (a or "").strip()
                if a:
                    variants.append(a)
        seen, clean = set(), []
        for v in variants:
            if v not in seen:
                seen.add(v); clean.append(v)
        out[pref_pt] = clean
    return out

def extract_controlled_terms_pt(th: Dict[str, Any]) -> List[str]:
    if not th or "terms" not in th:
        return []
    tps = []
    for term in th["terms"]:
        pref_pt = ((term.get("pref") or {}).get("pt") or "").strip()
        if pref_pt:
            tps.append(pref_pt)
    seen, out = set(), []
    for it in tps:
        if it not in seen:
            seen.add(it); out.append(it)
    return out

def load_variant_map(path: Optional[str], base: Optional[Dict[str, List[str]]] = None) -> Dict[str, List[str]]:
    base_map = dict(VARIANT_MAP_DEFAULT)
    if base:
        for k, v in base.items():
            if not isinstance(v, list):
                continue
            base_map.setdefault(k, [])
            for it in v:
                if it not in base_map[k]:
                    base_map[k].append(it)
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                ext = json.load(f)
            for k, v in (ext or {}).items():
                if not isinstance(v, list):
                    continue
                base_map.setdefault(k, [])
                for it in v:
                    if it not in base_map[k]:
                        base_map[k].append(it)
        except Exception:
            pass
    return base_map

def generate_variants(descritor: str, variant_map: Dict[str, List[str]], expand_variants: bool) -> List[str]:
    out = []
    base = descritor.strip()
    if not base: return out
    out.append(base)
    sa = strip_accents(base)
    if sa != base:
        out.append(sa)
    if expand_variants:
        low = base.lower()
        for key, alts in variant_map.items():
            if key.lower() in low or low in key.lower():
                for a in alts:
                    if a not in out:
                        out.append(a)
        if base in variant_map:
            for a in variant_map[base]:
                if a not in out:
                    out.append(a)
    seen, out2 = set(), []
    for it in out:
        if it not in seen:
            seen.add(it)
            out2.append(it)
    return out2

def build_descritores(custom_file: Optional[str] = None) -> List[str]:
    if custom_file and os.path.exists(custom_file):
        with open(custom_file, "r", encoding="utf-8") as f:
            items = [l.strip() for l in f if l.strip()]
    else:
        items = DESCRITORES_BASE_1 + DESCRITORES_BASE_2
    seen, out = set(), []
    for it in items:
        if it not in seen:
            seen.add(it)
            out.append(it)
    return out

# ============ Registro unificado & dedupe/merge ============
def make_record(descritor_base: Union[str, List[str]],
                consulta: Union[str, List[str]],
                fonte: str, tipo: str, ano: Optional[int],
                titulo: str, autores: str, resumo: str, doi: str, link: str,
                hit_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    rec = {
        "descritor": descritor_base,
        "consulta": consulta,
        "fonte": fonte,
        "fontes": [fonte],
        "tipo": tipo or "",
        "ano": int(ano) if (isinstance(ano, int) or (isinstance(ano, str) and str(ano).isdigit())) else "",
        "titulo": titulo or "",
        "autores": autores or "",
        "resumo": resumo or "",
        "doi": (doi or "").lower(),
        "link": link or "",
        "hit_context": [hit_context] if hit_context else []
    }
    return rec

def prefer_type(t1: str, t2: str) -> str:
    ranking = {"thesis": 3, "dissertation": 3, "journal-article": 2, "article": 2,
               "book-chapter": 2, "proceedings-article": 2, "thesis/dissertation": 1, "": 0}
    s1, s2 = ranking.get((t1 or "").lower(), 0), ranking.get((t2 or "").lower(), 0)
    return t1 if s1 >= s2 else t2

def merge_records(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    for key in ("descritor", "consulta"):
        av = a.get(key); bv = b.get(key)
        if not isinstance(av, list): av = [av] if av else []
        if not isinstance(bv, list): bv = [bv] if bv else []
        merged = []
        seen = set()
        for it in av + bv:
            if it and it not in seen:
                seen.add(it); merged.append(it)
        a[key] = merged
    af = a.get("fontes") or []
    bf = b.get("fontes") or [b.get("fonte")] if b.get("fonte") else []
    a["fontes"] = list(dict.fromkeys([x for x in af + bf if x]))
    if not a.get("fonte") and b.get("fonte"):
        a["fonte"] = b["fonte"]
    a["tipo"] = prefer_type(a.get("tipo", ""), b.get("tipo", ""))
    if not a.get("ano") and b.get("ano"): a["ano"] = b["ano"]
    if len(b.get("autores","")) > len(a.get("autores","")): a["autores"] = b["autores"]
    if len(b.get("resumo","")) > len(a.get("resumo","")): a["resumo"] = b["resumo"]
    def is_pdf(u: str) -> bool: return bool(u and u.lower().endswith(".pdf"))
    if not a.get("link") or (not is_pdf(a.get("link","")) and is_pdf(b.get("link",""))):
        if b.get("link"): a["link"] = b["link"]
    if not a.get("doi") and b.get("doi"): a["doi"] = b["doi"]
    a["hit_context"] = (a.get("hit_context") or []) + (b.get("hit_context") or [])
    return a

def dedupe_key(r: Dict[str, Any]) -> str:
    k = (r.get("doi") or "").lower().strip()
    if not k:
        k = f"{normalize_title(r.get('titulo',''))}|{r.get('ano','')}"
    return k

def dedupe(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for r in records:
        key = dedupe_key(r)
        if key in merged:
            merged[key] = merge_records(merged[key], r)
        else:
            merged[key] = dict(r)
    return list(merged.values())

# ============ Checkpoint/Streaming ============
class CheckpointManager:
    def __init__(self, out_json: str, out_ndjson: Optional[str], checkpoint_seconds: int,
                 checkpoint_records: int, resume: bool):
        self.out_json = out_json
        self.out_ndjson = out_ndjson
        self.checkpoint_seconds = checkpoint_seconds
        self.checkpoint_records = checkpoint_records
        self.last_flush = time.time()
        self.records_since = 0
        self.buffer: List[Dict[str, Any]] = []
        self.seen = set()
        if resume:
            self._preseed_seen()
        self.ndjson_fh = None
        if self.out_ndjson:
            self.ndjson_fh = open(self.out_ndjson, "a", encoding="utf-8")

    def _preseed_seen(self):
        if os.path.exists(self.out_json):
            try:
                data = json.load(open(self.out_json, "r", encoding="utf-8"))
                for r in data if isinstance(data, list) else []:
                    self.seen.add(dedupe_key(r))
            except Exception:
                pass
        if self.out_ndjson and os.path.exists(self.out_ndjson):
            try:
                with open(self.out_ndjson, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line: continue
                        try:
                            r = json.loads(line)
                            self.seen.add(dedupe_key(r))
                        except Exception:
                            continue
            except Exception:
                pass

    def add(self, rec: Dict[str, Any]):
        key = dedupe_key(rec)
        if key in self.seen:
            return False
        self.seen.add(key)
        self.buffer.append(rec)
        self.records_since += 1
        if self.ndjson_fh:
            self.ndjson_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self.ndjson_fh.flush()
        now = time.time()
        if (self.checkpoint_records and self.records_since >= self.checkpoint_records) or \
           (self.checkpoint_seconds and now - self.last_flush >= self.checkpoint_seconds):
            self.flush_snapshot()
        return True

    def flush_snapshot(self):
        if not self.buffer:
            self.last_flush = time.time()
            self.records_since = 0
            return
        existing = []
        if os.path.exists(self.out_json):
            try:
                existing = json.load(open(self.out_json, "r", encoding="utf-8"))
                if not isinstance(existing, list): existing = []
            except Exception:
                existing = []
        agg = existing + self.buffer
        final = dedupe(agg)
        tmp = self.out_json + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(final, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.out_json)
        print(f"[checkpoint] snapshot salvo → {self.out_json} (total atual: {len(final)} registros)")
        self.buffer = []
        self.last_flush = time.time()
        self.records_since = 0

    def finalize(self):
        self.flush_snapshot()
        if self.ndjson_fh:
            self.ndjson_fh.close()

# ============ SciELO ============
def scielo_search(descritor_base: str, consulta: str, year_min: int, year_max: int,
                  max_pages: int, enrich_max: int, enrich_timeout: float,
                  delay: float, exact_phrase: bool, deadline_ts: Optional[float],
                  debug: bool=False) -> Iterable[Dict[str, Any]]:
    headers = {"User-Agent": USER_AGENT}
    q = f"\"{consulta}\"" if exact_phrase else consulta
    base = "https://search.scielo.org/?q={q}&lang=pt&count=50&from=1&output=site&format=summary&fb=&page={p}"
    enriched = 0
    def time_up(): return deadline_ts is not None and time.time() >= deadline_ts
    def enrich_article(url: str) -> Tuple[str, str, str]:
        out_doi, out_abs, out_tipo = "", "", ""
        r = http_get(url, headers=headers, timeout=TIMEOUT, retries=1, backoff=1.4, debug=debug)
        if not r: return out_doi, out_abs, out_tipo
        soup = BeautifulSoup(r.text, "html.parser")
        for name in ("citation_doi", "dc.identifier", "DC.identifier", "doi"):
            m = soup.find("meta", attrs={"name": re.compile(name, re.I)})
            if m and m.get("content"):
                out_doi = pick_doi_from(m["content"])
                if out_doi: break
        if not out_doi:
            out_doi = pick_doi_from(soup.get_text(" ", strip=True))
        def grab(sel: str) -> str:
            node = soup.select_one(sel)
            return clean_text(node.get_text(" ", strip=True)) if node else ""
        for sel in ["section#abstract p", "div#abstract p", "div.abstract p", "div.resumo p", "div#resumo p",
                    "div#content .abstract p"]:
            out_abs = grab(sel)
            if out_abs: break
        if not out_abs:
            node = soup.find(class_=re.compile("abstract|resumo", re.I))
            if node: out_abs = clean_text(node.get_text(" ", strip=True))
        txt = soup.get_text(" ", strip=True).lower()
        if not out_tipo:
            if any(k in txt for k in ["thesis", "tese", "doutor"]):
                out_tipo = "thesis"
            elif any(k in txt for k in ["dissertação", "dissertacao", "mestrado"]):
                out_tipo = "dissertation"
            else:
                out_tipo = "journal-article"
        return out_doi, out_abs, out_tipo

    for page in range(1, max_pages + 1):
        if STOP_REQUESTED or time_up(): break
        url = base.format(q=quote(q), p=page)
        r = http_get(url, headers=headers, timeout=TIMEOUT, retries=2, backoff=1.5, debug=debug)
        if not r: break
        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.find_all("div", class_="item")
        if not items: break

        for it in items:
            if STOP_REQUESTED or time_up(): break
            title_tag = it.find("strong", class_="title")
            titulo = title_tag.get_text(strip=True) if title_tag else ""
            a_parent = title_tag.find_parent("a") if title_tag else None
            link = a_parent["href"] if (a_parent and a_parent.has_attr("href")) else ""

            authors = it.find("div", class_="line authors")
            autores = authors.get_text(" ", strip=True) if authors else ""

            source = it.find("div", class_="line source")
            ano = None
            if source:
                m = re.search(r"\b(19|20)\d{2}\b", source.get_text())
                if m: ano = int(m.group())

            resumo = ""
            for ab in it.find_all("div", class_="abstract"):
                cand = clean_text(ab.get_text(strip=True))
                if cand:
                    resumo = cand
                    break

            doi = pick_doi_from(titulo, autores)
            tipo = "journal-article"

            if ano and (ano < year_min or ano > year_max):
                continue

            hit_ctx = {"fonte": "SciELO", "endpoint": "search.scielo.org", "query": q, "page": page}

            if (not doi or not resumo) and enrich_max > 0 and link and not time_up():
                d2, a2, t2 = enrich_article(link)
                if d2: doi = d2
                if a2 and not resumo: resumo = a2
                if t2: tipo = t2
                enrich_max -= 1

            yield make_record(descritor_base, q, "SciELO", tipo, ano, titulo, autores, resumo, doi, link, hit_ctx)
        sleep_with_jitter(delay)

# ============ OpenAlex ============
def openalex_search(descritor_base: str, consulta: str, year_min: int, year_max: int,
                    per_page: int, max_pages: int, delay: float,
                    title_search: bool, deadline_ts: Optional[float], debug: bool=False) -> Iterable[Dict[str, Any]]:
    headers = {"User-Agent": USER_AGENT}
    cursor = "*"
    pages = 0
    def time_up(): return deadline_ts is not None and time.time() >= deadline_ts
    filt = f"from_publication_date:{year_min}-01-01,to_publication_date:{year_max}-12-31,language:pt|en|es"
    while pages < max_pages and cursor and not STOP_REQUESTED and not time_up():
        if title_search:
            url = (f"https://api.openalex.org/works"
                   f"?filter={quote(filt)},title.search:{quote(consulta)}"
                   f"&per_page={per_page}&cursor={quote(cursor)}")
            qmode = "title.search"
        else:
            url = (f"https://api.openalex.org/works"
                   f"?search={quote(consulta)}&filter={quote(filt)}"
                   f"&per_page={per_page}&cursor={quote(cursor)}")
            qmode = "search"
        r = http_get(url, headers=headers, timeout=TIMEOUT, retries=2, backoff=1.4, debug=debug)
        if not r: break
        data = r.json()
        results = data.get("results", [])
        if not results: break
        for w in results:
            if STOP_REQUESTED or time_up(): break
            titulo = w.get("display_name") or ""
            ano = w.get("publication_year")
            tipo = (w.get("type") or "").lower() or "article"
            doi = (w.get("doi") or "").replace("https://doi.org/", "").lower()
            auths = []
            for au in (w.get("authorships") or []):
                nm = (au.get("author") or {}).get("display_name")
                if nm: auths.append(nm)
            autores = "; ".join(auths)
            link = ""
            pl = w.get("primary_location") or {}
            if pl.get("landing_page_url"):
                link = pl["landing_page_url"]
            elif w.get("open_access") and (w["open_access"] or {}).get("oa_url"):
                link = w["open_access"]["oa_url"]
            resumo = ""
            inv = w.get("abstract_inverted_index")
            if inv:
                words = []
                for word, idxs in inv.items():
                    for _ in idxs:
                        words.append((_, word))
                words.sort(key=lambda x: x[0])
                resumo = clean_text(" ".join([w for _, w in words]))
            hit_ctx = {"fonte": "OpenAlex", "endpoint": "api.openalex.org/works",
                       "mode": qmode, "query": consulta, "cursor": cursor}
            yield make_record(descritor_base, consulta, "OpenAlex", tipo, ano, titulo, autores, resumo, doi, link, hit_ctx)
        cursor = (data.get("meta") or {}).get("next_cursor")
        pages += 1
        sleep_with_jitter(delay)

# ============ Crossref ============
def crossref_search(descritor_base: str, consulta: str, year_min: int, year_max: int,
                    rows: int, max_pages: int, delay: float, mailto: Optional[str],
                    title_search: bool, deadline_ts: Optional[float], debug: bool=False) -> Iterable[Dict[str, Any]]:
    headers = {"User-Agent": f"{USER_AGENT} mailto:{mailto}" if mailto else USER_AGENT}
    filters = f"from-pub-date:{year_min}-01-01,until-pub-date:{year_max}-12-31"
    base_params = {"rows": rows, "filter": filters, "select": "title,author,issued,abstract,DOI,type,URL",
                   "sort": "relevance", "order": "desc"}
    query_field = "query.title" if title_search else "query"
    def time_up(): return deadline_ts is not None and time.time() >= deadline_ts
    for page in range(max_pages):
        if STOP_REQUESTED or time_up(): break
        params = dict(base_params)
        params["offset"] = page * rows
        params[query_field] = consulta
        r = http_get("https://api.crossref.org/works", headers=headers, timeout=TIMEOUT,
                     retries=2, backoff=1.3, debug=debug, params=params)
        if not r: break
        items = (r.json().get("message") or {}).get("items", [])
        if not items: break
        for it in items:
            if STOP_REQUESTED or time_up(): break
            titulo = " ".join(it.get("title") or []).strip()
            ano = None
            issued = it.get("issued", {}).get("date-parts")
            if issued and isinstance(issued, list) and issued[0]:
                y = issued[0][0]
                if isinstance(y, int): ano = y
            autores_list = []
            for a in it.get("author", []) or []:
                given = a.get("given") or ""
                family = a.get("family") or ""
                nm = (given + " " + family).strip()
                if nm: autores_list.append(nm)
            autores = "; ".join(autores_list)
            resumo = clean_text(it.get("abstract"))
            doi = (it.get("DOI") or "").lower()
            link = it.get("URL") or ""
            tipo = (it.get("type") or "").lower()
            hit_ctx = {"fonte": "Crossref", "endpoint": "api.crossref.org/works",
                       "query_field": query_field, "query": consulta, "offset": page * rows}
            yield make_record(descritor_base, consulta, "Crossref", tipo, ano, titulo, autores, resumo, doi, link, hit_ctx)
        sleep_with_jitter(delay)

# ============ BDTD (API + HTML) ============
BLOCKLIST_DOMAINS = {"brasil.gov.br", "www.brasil.gov.br"}
DEPRIORITY_DOMAINS = {"gov.br", "www.gov.br"}
REPO_HINTS_HOST = ("handle.net", "repositorio", "ri.", "bdm.", "bdtd.", "oasisbr", "dspace",
                   "uf", "unb", "usp", "unesp", "ufpi", "ufpe", "ufrj", "ufmg", "ufc", "ufpb", "ufrn")
REPO_HINTS_PATH = ("/bitstream/", "/handle/", "/jspui/", "/download", "objectId=", "filename=", "rest/bitstreams")
GOOD_TEXT_HINTS = ("texto completo", "download", "arquivo", "pdf", "full text", "ver arquivo",
                   "acessar", "acesso ao texto", "ver documento")

HDR_PATTERNS = {"pt": re.compile(r"\b(resumo)\b", re.I),
                "es": re.compile(r"\b(resumen)\b", re.I),
                "en": re.compile(r"\b(abstract)\b", re.I),
                "desc": re.compile(r"descri(?:ç|c)ão|description", re.I)}
NEG_LABEL = re.compile(r"\b(orientador(?:a)?s?|advisor|adviser|co-?orientador(?:a)?s?|palavras?-chave|keywords|descriptores)\b", re.I)

def score_link(href: str, text: str, base: str) -> int:
    if not href or href.strip() in ("#", "javascript:void(0)", "javascript:;"):
        return -999
    url = urljoin(base, href)
    p = urlparse(url)
    host = (p.netloc or "").lower()
    path = (p.path or "").lower()
    txt = (text or "").strip().lower()
    score = 0
    if path.endswith((".pdf", ".doc", ".docx", ".odt")): score += 120
    if any(h in path for h in REPO_HINTS_PATH): score += 80
    if any(h in txt for h in GOOD_TEXT_HINTS): score += 60
    if any(h in host for h in REPO_HINTS_HOST): score += 25
    if host in BLOCKLIST_DOMAINS: score -= 120
    if host in DEPRIORITY_DOMAINS: score -= 40
    if "bdtd.ibict.br" in host and not path.endswith((".pdf",".doc",".docx",".odt")): score -= 15
    return score

def select_best_fulltext_link(soup: BeautifulSoup, record_url: str) -> Optional[str]:
    best_url, best_score = None, -999
    for a in soup.select("div.record-tabs a, a[href]"):
        href = a.get("href")
        text = a.get_text(" ", strip=True)
        url = urljoin(record_url, href) if href else ""
        sc = score_link(href or "", text, record_url)
        if sc > best_score:
            best_score, best_url = sc, url
    if best_url and best_score >= 10:
        return best_url
    return None

def extract_best_abstract(soup: BeautifulSoup) -> str:
    def clean(s: str) -> str: return clean_text(s)
    candidates = {"pt":"", "es":"", "en":"", "desc":""}
    for meta_name in ("DC.description", "dc.description", "description", "citation_abstract"):
        for m in soup.find_all("meta", attrs={"name": re.compile(meta_name, re.I)}):
            val = clean(m.get("content", ""))
            if val and not candidates["desc"]: candidates["desc"] = val
    for sel, key in [(".resumo, #resumo, [id*=resumo i]", "pt"),
                     (".abstract, #abstract, [id*=abstract i]", "en"),
                     (".descripcion, .descricao, #descricao, [id*=descri i]", "desc")]:
        for el in soup.select(sel):
            txt = clean(el.get_text(" ", strip=True))
            if txt and not NEG_LABEL.search(txt) and not candidates[key]:
                candidates[key] = txt
    def _text_after_cell_label(lbl: BeautifulSoup) -> str:
        tr = lbl.find_parent("tr")
        if not tr: return ""
        td = tr.find("td")
        if td: return clean(td.get_text(" ", strip=True))
        got = False
        for c in tr.find_all(["td", "dd", "th"]):
            if c is lbl: got = True; continue
            if got and c.name in ("td", "dd"):
                return clean(c.get_text(" ", strip=True))
        return ""
    for hdr in soup.find_all(["th", "dt"]):
        label = clean(hdr.get_text(" ", strip=True))
        if not label or NEG_LABEL.search(label): continue
        for key, pat in HDR_PATTERNS.items():
            if pat.search(label):
                body = _text_after_cell_label(hdr)
                if body and not NEG_LABEL.search(body) and not candidates[key]:
                    candidates[key] = body
                break
    def _text_from_label_block(lbl: BeautifulSoup) -> str:
        parent = lbl.parent
        chunks: List[str] = []
        for s in parent.find_next_siblings(limit=4):
            if s.find(["label","th","dt"]): break
            t = clean(s.get_text(" ", strip=True))
            if t: chunks.append(t)
        if chunks: return " ".join(chunks)
        nxt = lbl.find_next(["p","div","span"])
        if nxt and parent in nxt.parents:
            return clean(nxt.get_text(" ", strip=True))
        row = parent.find_parent(class_=re.compile(r"\brow\b", re.I))
        if row:
            cols = row.find_all(class_=re.compile(r"\bcol(-xs|-sm|-md|-lg)?-\d+\b", re.I))
            for c in cols:
                if lbl in c.descendants:
                    idx = cols.index(c)
                    if idx+1 < len(cols):
                        return clean(cols[idx+1].get_text(" ", strip=True))
        return ""
    for lbl in soup.find_all(["label", "strong", "b", "span"]):
        label = clean(lbl.get_text(" ", strip=True))
        if not label or NEG_LABEL.search(label): continue
        for key, pat in HDR_PATTERNS.items():
            if pat.search(label):
                body = _text_from_label_block(lbl)
                if body and not NEG_LABEL.search(body) and not candidates[key]:
                    candidates[key] = body
                break
    def _collect_following_paragraphs(start_el) -> str:
        chunks: List[str] = []
        for sib in start_el.find_all_next():
            if sib == start_el: continue
            if sib.name and sib.name.lower() in ("h1","h2","h3","h4","h5"): break
            if sib.name in ("dt","th","label"): break
            if sib.name in ("p","div","span","li","td","dd"):
                t = clean(sib.get_text(" ", strip=True))
                if t: chunks.append(t)
            if sum(len(x) for x in chunks) >= 6000: break
            if sib.get("class") and any(("tab" in c or "ficha" in c) for c in sib.get("class")): break
        return " ".join(chunks).strip()
    for key, pat in HDR_PATTERNS.items():
        for h in soup.find_all(re.compile(r"h[2-5]", re.I)):
            tx = clean(h.get_text(" ", strip=True))
            if pat.search(tx) and not NEG_LABEL.search(tx):
                txt = _collect_following_paragraphs(h)
                if txt and not NEG_LABEL.search(txt) and not candidates[key]:
                    candidates[key] = txt
    for key in ("pt","es","en","desc"):
        if candidates.get(key): return candidates[key]
    return ""

def bdtd_enrich(record_url: str, timeout_sec: float, debug: bool=False) -> Dict[str, Any]:
    start = time.time()
    headers_html = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    headers_json = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    out = {"resumo": "", "autores": "", "ano": None, "doi": "", "link_pdf": "", "tipo": ""}

    if time.time() - start < timeout_sec * 0.6:
        for style in ("Json", "JSON"):
            u = f"{record_url}/Export?style={style}"
            r = http_get(u, headers=headers_json, timeout=TIMEOUT, retries=1, backoff=1.3, debug=debug)
            if r:
                try:
                    data = r.json()
                except Exception:
                    data = None
                if data:
                    res = data.get("abstract") or data.get("description") or ""
                    au = data.get("authors") or data.get("creator") or data.get("author")
                    yr = pick_year_from(data.get("publishDate"), data.get("date"), data.get("issued"), data.get("year"))
                    doi = pick_doi_from(data.get("doi"), data.get("identifier"), data.get("id"))
                    if res: out["resumo"] = clean_text(res)
                    if au: out["autores"] = normalize_authors(au)
                    if yr: out["ano"] = yr
                    if doi: out["doi"] = doi

    if time.time() - start < timeout_sec:
        r = http_get(record_url, headers=headers_html, timeout=TIMEOUT, retries=1, backoff=1.2, debug=debug)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            if not out["autores"]:
                metas = soup.find_all("meta", attrs={"name": re.compile(r"dc\.creator", re.I)})
                if metas:
                    out["autores"] = "; ".join([m.get("content","") for m in metas if m.get("content")])
                if not out["autores"]:
                    for lbl in soup.find_all(["dt","th","label","strong","b"]):
                        if re.search(r"\b(autor|autores|author|creator)\b", lbl.get_text(" ", strip=True), re.I):
                            dd = lbl.find_next(["dd","td","p","div","span"])
                            if dd: out["autores"] = clean_text(dd.get_text(" ", strip=True)); break
            if not out["resumo"]:
                out["resumo"] = extract_best_abstract(soup)
            if not out["ano"]:
                m = soup.find("meta", attrs={"name": re.compile(r"(dc\.date|citation_publication_date)", re.I)})
                if m and m.get("content"):
                    out["ano"] = pick_year_from(m.get("content"))
                if not out["ano"]:
                    for lbl in soup.find_all(["dt","th","label","strong","b"]):
                        if re.search(r"(data|ano|issued|date)", lbl.get_text(" ", strip=True), re.I):
                            dd = lbl.find_next(["dd","td","p","div","span"])
                            if dd:
                                out["ano"] = pick_year_from(dd.get_text(" ", strip=True))
                                if out["ano"]: break
            if not out["doi"]:
                metas = soup.find_all("meta", attrs={"name": re.compile(r"dc\.identifier", re.I)})
                for m in metas:
                    doi = pick_doi_from(m.get("content",""))
                    if doi: out["doi"] = doi; break
                if not out["doi"]:
                    out["doi"] = pick_doi_from(soup.get_text(" ", strip=True))
            if not out["tipo"]:
                txt = soup.get_text(" ", strip=True).lower()
                if any(k in txt for k in ["tese","doctoral","phd","doutor"]):
                    out["tipo"] = "thesis"
                elif any(k in txt for k in ["dissertação","dissertacao","mestrado","master"]):
                    out["tipo"] = "dissertation"
                else:
                    out["tipo"] = "thesis/dissertation"
            best = select_best_fulltext_link(soup, record_url)
            if best: out["link_pdf"] = best
    if out["resumo"] and NEG_LABEL.search(out["resumo"]) and len(out["resumo"]) < 120:
        out["resumo"] = ""
    return out

def bdtd_api_search(descritor_base: str, consulta: str, year_min: int, year_max: int,
                    limit_per_page: int, max_pages: int,
                    enrich_max: int, enrich_timeout: float,
                    delay: float, exact_phrase: bool, deadline_ts: Optional[float], debug: bool=False) -> Iterable[Dict[str, Any]]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    look = f"\"{consulta}\"" if exact_phrase else consulta
    q = f'{look} AND publishDate:[{year_min} TO {year_max}]'
    enriched = 0
    def time_up(): return deadline_ts is not None and time.time() >= deadline_ts
    for page in range(1, max_pages + 1):
        if STOP_REQUESTED or time_up(): break
        params = {"lookfor": q, "type": "AllFields", "limit": limit_per_page, "page": page}
        r = http_get(BDTD_API_BASE, headers=headers, timeout=TIMEOUT, retries=2, backoff=1.4, debug=debug, params=params)
        if not r: break
        try:
            data = r.json()
        except Exception:
            break
        recs = None
        for path in ("records", "result.records", "items"):
            cur = data
            for key in path.split("."):
                if isinstance(cur, dict): cur = cur.get(key)
            if isinstance(cur, list):
                recs = cur; break
        if not recs: break

        for rec in recs:
            if STOP_REQUESTED or time_up(): break
            rid = rec.get("id") or rec.get("recordId") or rec.get("id_str") or ""
            record_link = f"{BDTD_HOST}/vufind/Record/{rid}" if rid else (rec.get("url") or "")
            titulo = (rec.get("title") or rec.get("title_full") or rec.get("title_fullStr") or rec.get("title_short") or "").strip()
            autores = normalize_authors(
                rec.get("authors") or rec.get("author") or rec.get("author_facet") or
                rec.get("dc.contributor.author") or rec.get("dc.contributor.author.fl_str_mv") or []
            )
            ano = pick_year_from(
                rec.get("publishDate"), rec.get("year"), rec.get("publicationDates"),
                rec.get("dc.date.issued"), rec.get("date"), rec.get("dc.date")
            )
            resumo = rec.get("summary") or rec.get("abstract") or rec.get("dc.description.abstract") or rec.get("description") or ""
            if isinstance(resumo, list): resumo = " ".join([str(x) for x in resumo if x])
            resumo = clean_text(resumo)
            doi = pick_doi_from(
                rec.get("doi"), rec.get("DOI"), rec.get("identifier"), rec.get("dc.identifier"),
                rec.get("dc.identifier.doi"), rec.get("dc.identifier.uri"), rec.get("urls"), rec.get("url")
            )
            tipo = "thesis/dissertation"
            formats = rec.get("formats") or rec.get("format") or rec.get("format_str_mv") or rec.get("dc.type") or []
            fm = " ".join([str(x).lower() for x in formats]) if isinstance(formats, list) else str(formats).lower()
            if any(k in fm for k in ["doctoral","doutor","tese","phd"]):
                tipo = "thesis"
            elif any(k in fm for k in ["master","mestrado","disser"]):
                tipo = "dissertation"

            link = record_link
            api_url = rec.get("url")
            if isinstance(api_url, list):
                for u in api_url:
                    if u: link = u; break
            elif isinstance(api_url, str) and api_url:
                link = api_url

            hit_ctx = {"fonte": "BDTD", "endpoint": "bdtd.ibict.br/vufind/api/v1/search",
                       "lookfor": q, "page": page}

            need = (not resumo or not autores or not ano or not doi or not link or "Record/" in link or "bdtd.ibict.br" in link)
            if need and enriched < enrich_max and record_link and not time_up():
                det = bdtd_enrich(record_link, timeout_sec=enrich_timeout, debug=debug)
                enriched += 1
                if not resumo and det.get("resumo"): resumo = det["resumo"]
                if not autores and det.get("autores"): autores = det["autores"]
                if not ano and det.get("ano"): ano = det["ano"]
                if not doi and det.get("doi"): doi = det["doi"]
                if det.get("link_pdf"): link = det["link_pdf"]
                if tipo == "thesis/dissertation" and det.get("tipo"): tipo = det["tipo"]

            yield make_record(descritor_base, look, "BDTD", tipo, ano, titulo, autores, resumo, doi, link, hit_ctx)
        sleep_with_jitter(delay)

# ============ Orquestração ============
def run(descritores: List[str], fontes: List[str], year_min: int, year_max: int,
        delay: float, mailto: Optional[str],
        scielo_pages: int, scielo_enrich_max: int, scielo_enrich_timeout: float, scielo_exact: bool,
        openalex_pages: int, openalex_per_page: int, openalex_title_search: bool,
        crossref_pages: int, crossref_rows: int, crossref_title_search: bool,
        bdtd_pages: int, bdtd_limit_per_page: int, bdtd_enrich_max: int, bdtd_enrich_timeout: float, bdtd_exact: bool,
        expand_variants: bool, variants_file: Optional[str],
        debug: bool,
        search_form: str,
        variant_map_override: Optional[Dict[str, List[str]]],
        # Novos
        out_json: str, out_ndjson: Optional[str],
        checkpoint_seconds: int, checkpoint_records: int,
        resume: bool, max_seconds: Optional[int]) -> List[Dict[str, Any]]:

    start_ts = time.time()
    deadline_ts = (start_ts + max_seconds) if max_seconds and max_seconds > 0 else None

    all_records: List[Dict[str, Any]] = []
    fontes = [f.lower() for f in fontes]

    # variant map (tesauro + arquivo externo + default)
    variant_map = variant_map_override if variant_map_override is not None else load_variant_map(variants_file)

    # modos exatos quando inclui 'controlada'
    eff_scielo_exact = scielo_exact or (search_form in ("controlada", "both"))
    eff_bdtd_exact = bdtd_exact or (search_form in ("controlada", "both"))
    eff_openalex_title = openalex_title_search or (search_form in ("controlada", "both"))
    eff_crossref_title = crossref_title_search or (search_form in ("controlada", "both"))

    # checkpoint manager
    ckpt = CheckpointManager(out_json, out_ndjson, checkpoint_seconds, checkpoint_records, resume)

    for descr in descritores:
        if STOP_REQUESTED or (deadline_ts and time.time() >= deadline_ts):
            break
        print(f"\n[BUSCA] Descritor base: {descr}")

        # consultas conforme forma
        if search_form == "controlada":
            consultas = [descr]
        elif search_form == "ampliada":
            consultas = generate_variants(descr, variant_map, expand_variants=True)
            if descr in consultas:
                consultas.remove(descr); consultas.insert(0, descr)
        else:
            ctrl = [descr]
            ampl = generate_variants(descr, variant_map, expand_variants=True)
            if descr in ampl: ampl.remove(descr)
            consultas = ctrl + ampl

        for q in consultas:
            if STOP_REQUESTED or (deadline_ts and time.time() >= deadline_ts):
                break
            print(f"   • Variante: {q}")

            if "scielo" in fontes:
                print("     - SciELO …")
                for rec in scielo_search(
                    descritor_base=descr, consulta=q,
                    year_min=year_min, year_max=year_max,
                    max_pages=scielo_pages, enrich_max=scielo_enrich_max, enrich_timeout=scielo_enrich_timeout,
                    delay=delay, exact_phrase=eff_scielo_exact, deadline_ts=deadline_ts, debug=debug
                ):
                    all_records.append(rec)
                    ckpt.add(rec)
                    if STOP_REQUESTED or (deadline_ts and time.time() >= deadline_ts): break

            # OpenAlex (habilitada conforme fontes)
            if "openalex" in fontes and not (STOP_REQUESTED or (deadline_ts and time.time() >= deadline_ts)):
                for rec in openalex_search(
                    descritor_base=descr, consulta=q,
                    year_min=year_min, year_max=year_max,
                    per_page=openalex_per_page, max_pages=openalex_pages,
                    delay=delay, title_search=eff_openalex_title, deadline_ts=deadline_ts, debug=debug
                ):
                    all_records.append(rec)
                    ckpt.add(rec)
                    if STOP_REQUESTED or (deadline_ts and time.time() >= deadline_ts): break

            if "crossref" in fontes and not (STOP_REQUESTED or (deadline_ts and time.time() >= deadline_ts)):
                print("     - Crossref …")
                for rec in crossref_search(
                    descritor_base=descr, consulta=q,
                    year_min=year_min, year_max=year_max,
                    rows=crossref_rows, max_pages=crossref_pages,
                    delay=delay, mailto=mailto, title_search=eff_crossref_title, deadline_ts=deadline_ts, debug=debug
                ):
                    all_records.append(rec)
                    ckpt.add(rec)
                    if STOP_REQUESTED or (deadline_ts and time.time() >= deadline_ts): break

            if "bdtd" in fontes and not (STOP_REQUESTED or (deadline_ts and time.time() >= deadline_ts)):
                print("     - BDTD …")
                for rec in bdtd_api_search(
                    descritor_base=descr, consulta=q,
                    year_min=year_min, year_max=year_max,
                    limit_per_page=bdtd_limit_per_page, max_pages=bdtd_pages,
                    enrich_max=bdtd_enrich_max, enrich_timeout=bdtd_enrich_timeout,
                    delay=delay, exact_phrase=eff_bdtd_exact, deadline_ts=deadline_ts, debug=debug
                ):
                    all_records.append(rec)
                    ckpt.add(rec)
                    if STOP_REQUESTED or (deadline_ts and time.time() >= deadline_ts): break

    # flush final
    ckpt.finalize()
    final = []
    try:
        final = json.load(open(out_json, "r", encoding="utf-8"))
    except Exception:
        final = dedupe(all_records)
        tmp = out_json + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(final, f, ensure_ascii=False, indent=2)
        os.replace(tmp, out_json)

    elapsed = int(time.time() - start_ts)
    print(f"\n[INFO] Tempo decorrido: {elapsed}s")
    if deadline_ts:
        rem = int(max(0, deadline_ts - time.time()))
        print(f"[INFO] Deadline ativo (restante ~{rem}s).")

    print(f"[OK] JSON: {out_json} (registros: {len(final)})")
    if out_ndjson:
        print(f"[OK] NDJSON streaming: {out_ndjson}")
    return final

# ============ CLI ============
def parse_args():
    ap = argparse.ArgumentParser(description="Levantamento bibliográfico unificado (SciELO, OpenAlex, Crossref, BDTD) → JSON único (com tesauro, temporizador e checkpoints)")
    ap.add_argument("--fontes", nargs="+", default=["scielo", "openalex", "crossref", "bdtd"],
                    help="Fontes a consultar: scielo openalex crossref bdtd")
    ap.add_argument("--anos", default=f"1970:{dt.datetime.now().year}",
                    help="Intervalo AAAA:AAAA (ex.: 1970:2025)")
    ap.add_argument("--descritores", default=None,
                    help="Arquivo de descritores (um por linha). Se ausente, usa a união embutida.")
    ap.add_argument("--delay", type=float, default=REQUEST_DELAY, help="Delay (s) entre requisições")
    ap.add_argument("--mailto", default=None, help="Seu e-mail (boa prática para Crossref)")
    ap.add_argument("--debug", action="store_true", help="Logs detalhados")

    # Saídas & checkpoint
    ap.add_argument("--out", default=OUTPUT_JSON, help="Arquivo de saída JSON (snapshot deduplicado)")
    ap.add_argument("--out-ndjson", default=OUTPUT_NDJSON, help="Arquivo NDJSON (streaming de registros brutos)")
    ap.add_argument("--checkpoint-seconds", type=int, default=60, help="Intervalo em segundos entre snapshots")
    ap.add_argument("--checkpoint-records", type=int, default=50, help="Grava snapshot a cada N registros novos")
    ap.add_argument("--resume", action="store_true", help="Lê arquivos existentes e evita duplicar registros")

    # Temporizador
    ap.add_argument("--max-seconds", type=int, default=0, help="Tempo máximo de execução (0 = sem limite)")

    # Estratégias de busca
    ap.add_argument("--expand-variants", action="store_true", help="(Compat.) Expande variantes (PT/EN/sem acentos)")
    ap.add_argument("--variants-file", default=None, help="Arquivo JSON com variantes extras")

    # Tesauro e formas de busca
    ap.add_argument("--thesaurus-file", default=None, help="Arquivo JSON do tesauro (SKOS-like)")
    ap.add_argument("--search-form", choices=["both", "controlada", "ampliada"], default="both",
                    help="Forma: somente TPs (controlada), TPs+variantes (ampliada) ou ambas (both)")

    # SciELO
    ap.add_argument("--scielo-pages", type=int, default=SCIELO_MAX_PAGES)
    ap.add_argument("--scielo-enrich-max", type=int, default=SCIELO_ENRICH_MAX)
    ap.add_argument("--scielo-enrich-timeout", type=float, default=SCIELO_ENRICH_TIMEOUT)
    ap.add_argument("--scielo-exact", action="store_true", help="(Compat.) Usa frase exata (aspas) na SciELO")

    # OpenAlex
    ap.add_argument("--openalex-pages", type=int, default=OPENALEX_MAX_PAGES)
    ap.add_argument("--openalex-per-page", type=int, default=OPENALEX_PER_PAGE)
    ap.add_argument("--openalex-title-search", action="store_true", help="(Compat.) Usa title.search na OpenAlex")

    # Crossref
    ap.add_argument("--crossref-pages", type=int, default=CROSSREF_MAX_PAGES)
    ap.add_argument("--crossref-rows", type=int, default=CROSSREF_ROWS)
    ap.add_argument("--crossref-title-search", action="store_true", help="(Compat.) Usa query.title no Crossref")

    # BDTD
    ap.add_argument("--bdtd-pages", type=int, default=BDTD_MAX_PAGES)
    ap.add_argument("--bdtd-limit-per-page", type=int, default=BDTD_LIMIT_PER_PAGE)
    ap.add_argument("--bdtd-enrich-max", type=int, default=BDTD_ENRICH_MAX)
    ap.add_argument("--bdtd-enrich-timeout", type=float, default=BDTD_ENRICH_TIMEOUT)
    ap.add_argument("--bdtd-exact", action="store_true", help="(Compat.) Usa frase exata no lookfor da BDTD")

    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    y1, y2 = args.anos.split(":")
    year_min, year_max = int(y1), int(y2)

    descrs = build_descritores(args.descritores)

    # Tesauro (se houver): TPs como descritores-base e variantes integradas
    variant_map_override = None
    if args.thesaurus_file:
        th = load_thesaurus_json(args.thesaurus_file)
        if th:
            tps_pt = extract_controlled_terms_pt(th)
            if tps_pt:
                if args.descritores is None:
                    descrs = tps_pt
                else:
                    seen, new_descrs = set(), []
                    for it in tps_pt + descrs:
                        if it not in seen:
                            seen.add(it); new_descrs.append(it)
                    descrs = new_descrs
            th_map = build_variant_map_from_thesaurus(th)
            variant_map_override = load_variant_map(args.variants_file, base=th_map)
        else:
            variant_map_override = load_variant_map(args.variants_file)
    else:
        variant_map_override = load_variant_map(args.variants_file)

    records = run(
        descritores=descrs,
        fontes=args.fontes,
        year_min=year_min, year_max=year_max,
        delay=args.delay, mailto=args.mailto,

        scielo_pages=args.scielo_pages,
        scielo_enrich_max=args.scielo_enrich_max,
        scielo_enrich_timeout=args.scielo_enrich_timeout,
        scielo_exact=args.scielo_exact,

        openalex_pages=args.openalex_pages,
        openalex_per_page=args.openalex_per_page,
        openalex_title_search=args.openalex_title_search,

        crossref_pages=args.crossref_pages,
        crossref_rows=args.crossref_rows,
        crossref_title_search=args.crossref_title_search,

        bdtd_pages=args.bdtd_pages,
        bdtd_limit_per_page=args.bdtd_limit_per_page,
        bdtd_enrich_max=args.bdtd_enrich_max,
        bdtd_enrich_timeout=args.bdtd_enrich_timeout,
        bdtd_exact=args.bdtd_exact,

        expand_variants=args.expand_variants,
        variants_file=args.variants_file,
        debug=args.debug,

        search_form=args.search_form,
        variant_map_override=variant_map_override,

        # novos
        out_json=args.out,
        out_ndjson=args.out_ndjson,
        checkpoint_seconds=args.checkpoint_seconds,
        checkpoint_records=args.checkpoint_records,
        resume=args.resume,
        max_seconds=args.max_seconds if args.max_seconds and args.max_seconds > 0 else None
    )
