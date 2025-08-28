#!/usr/bin/env python3
"""
pdf2articles.py — extraction fiable des Articles depuis un PDF juridique

- doc_id : extrait sur la 1re page via regex du bandeau (ex: 0.747.205)
- doc_title : concaténation des plus gros blocs de la 1re page
- Articles : segments "Art. N" -> "Art. N+1" (ou fin)
- title par article : "Art. N, <doc_title>"

Sortie JSONL (une ligne par article) avec :
  doc_id, article_number, title, page_start, page_end,
  text_full, text_full_ascii, text_preview, text_preview_ascii, text_chars, pdf_path
"""

import argparse
import json
import re
import unicodedata
from pathlib import Path

import fitz  # PyMuPDF


ART_HEADING_RE = re.compile(r"^\s*Art\.\s*(\d+[a-zA-Z]?)\s*$", re.IGNORECASE | re.MULTILINE)
DOC_ID_RE = re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\b")  # ex: 0.747.205

def strip_accents(s: str) -> str:
    if s is None:
        return ""
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)).lower()

def guess_doc_id_from_first_page(page_text: str) -> str | None:
    m = DOC_ID_RE.search(page_text)
    return m.group(0) if m else None

def guess_doc_title_from_first_page(page):
    """
    Titre = texte de la moitié supérieure de la page 1 :
      - Si 'Texte original' trouvé : entre 'Texte original' et 'Conclu/Conclue'.
      - Sinon : du début jusqu'à 'Conclu/Conclue'.
    """
    import re, unicodedata

    def _norm(s: str) -> str:
        return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

    # 1) Récupérer texte haut de page
    data = page.get_text("dict")
    blocks = data.get("blocks", [])
    page_h = float(page.rect.height)

    lines = []
    for b in blocks:
        for l in b.get("lines", []):
            spans = l.get("spans", [])
            if not spans:
                continue
            y0 = spans[0]["bbox"][1]
            y1 = spans[-1]["bbox"][3]
            y_mid = 0.5 * (y0 + y1)
            if y_mid <= page_h / 2.0:
                txt = "".join(s.get("text", "") or "" for s in spans).strip()
                if txt:
                    lines.append((y_mid, txt))

    if not lines:
        return _title_between_markers_fallback(page.get_text() or "")

    lines.sort(key=lambda t: t[0])
    top_text = "\n".join(t[1] for t in lines)

    # 2) Recherche bornes
    start_pat = re.compile(r"texte\s+original", re.IGNORECASE)
    end_pat = re.compile(r"\bconclu(e)?\b", re.IGNORECASE)

    norm_text = _norm(top_text)
    m_start = start_pat.search(norm_text)
    m_end = end_pat.search(norm_text, m_start.end() if m_start else 0) if m_start else end_pat.search(norm_text)

    if m_start and m_end and m_end.start() > m_start.end():
        segment = top_text[m_start.end():m_end.start()]
    elif not m_start and m_end:
        # Exception : pas de "Texte original" -> début jusqu'à Conclu
        segment = top_text[:m_end.start()]
    else:
        return _title_between_markers_fallback(page.get_text() or "")

    # 3) Nettoyage
    seg = "\n".join(l for l in (line.strip() for line in segment.splitlines()) if l)
    seg = re.sub(r"\s+", " ", seg).strip()
    seg = seg.replace("\xad", "")
    seg = re.sub(r"\s*-\s+", "-", seg)

    return seg if seg else _title_between_markers_fallback(page.get_text() or "")


def _title_between_markers_fallback(raw_page_text: str) -> str:
    import re, unicodedata
    def _norm(s: str) -> str:
        return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

    DOC_ID_LINE = re.compile(r"^\s*\d{1,3}\.\d{1,3}\.\d{1,3}\s*$")
    start_pat = re.compile(r"texte\s+original", re.IGNORECASE)
    end_pat = re.compile(r"\bconclu(e)?\b", re.IGNORECASE)

    norm = _norm(raw_page_text)
    ms = start_pat.search(norm)
    me = end_pat.search(norm, ms.end() if ms else 0) if ms else end_pat.search(norm)

    if ms and me and me.start() > ms.end():
        seg = raw_page_text[ms.end():me.start()]
    elif not ms and me:
        seg = raw_page_text[:me.start()]
    else:
        seg = ""

    seg = "\n".join(l for l in (line.strip() for line in seg.splitlines()) if l)
    seg = re.sub(r"\s+", " ", seg).strip()
    seg = seg.replace("\xad", "")
    seg = re.sub(r"\s*-\s+", "-", seg)

    if seg:
        return seg

    for line in raw_page_text.splitlines():
        t = line.strip()
        if t and not DOC_ID_LINE.match(t) and not t.lower().startswith("texte original"):
            return t
    return "Titre inconnu"


def load_pdf_text_with_pages(pdf_path: Path):
    """
    Retourne:
      full_text: tout le doc en une string avec séparateurs de pages,
      page_texts: liste str par page,
      page_offsets: mapping offset->page index pour remonter aux pages des occurrences.
    """
    doc = fitz.open(pdf_path)
    page_texts = []
    full_text = ""
    page_offsets = []  # offset de début pour chaque page
    for i, page in enumerate(doc):
        txt = page.get_text()  # extraction en "lecture" (préserve à peu près l’ordre)
        page_offsets.append(len(full_text))
        # Ajoute un marqueur de page
        full_text += txt + f"\n<<<PAGE_BREAK_{i+1}>>>\n"
        page_texts.append(txt)
    return doc, full_text, page_texts, page_offsets

def find_articles_segments(full_text: str):
    """
    Détecte les positions des entêtes d’article "Art. X".
    Retourne une liste de tuples: (article_number, start_idx, end_idx_excl)
    """
    # repère tous les matches "Art. N" en début de ligne
    matches = list(ART_HEADING_RE.finditer(full_text))
    segments = []
    for idx, m in enumerate(matches):
        art_num = m.group(1)
        start = m.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(full_text)
        segments.append((art_num, start, end))
    return segments

def span_to_page_range(start_idx: int, end_idx: int, page_offsets: list[int]) -> tuple[int, int]:
    """
    Convertit un intervalle [start_idx, end_idx) en plage de pages (1-based).
    """
    # page = max index tel que page_offsets[page] <= pos
    def pos_to_page(pos):
        p = 0
        for i, off in enumerate(page_offsets):
            if off <= pos:
                p = i
            else:
                break
        return p + 1  # 1-based
    p1 = pos_to_page(start_idx)
    p2 = pos_to_page(end_idx - 1 if end_idx > 0 else 0)
    return p1, p2

def make_preview(s: str, n: int = 4000) -> str:
    s = s.strip()
    return s[:n]

def process_pdf(pdf_path: Path):
    doc, full_text, page_texts, page_offsets = load_pdf_text_with_pages(pdf_path)

    # DOC_ID à partir de la 1re page
    first_page_text = page_texts[0] if page_texts else ""
    doc_id = guess_doc_id_from_first_page(first_page_text) or pdf_path.stem

    # Titre global à partir des plus gros blocs de la 1re page
    doc_title = guess_doc_title_from_first_page(doc[0]) if len(doc) > 0 else "Titre inconnu"

    # Segments d’articles
    segments = find_articles_segments(full_text)

    # Si aucun "Art." détecté : on sort un "Art. 1" unique avec tout le doc
    if not segments:
        segments = [("1", 0, len(full_text))]

    for art_num, start, end in segments:
        raw = full_text[start:end].strip()

        # Nettoyage léger : on supprime les marqueurs de page
        body = re.sub(r"<<<PAGE_BREAK_\d+>>>", "", raw).strip()

        # Titre d’article combiné
        article_number = f"Art. {art_num}"
        article_title = f"{article_number}, {doc_title}"

        # Pages
        p_start, p_end = span_to_page_range(start, end, page_offsets)

        # Variantes texte
        text_full = body
        text_full_ascii = strip_accents(body)
        text_preview = make_preview(text_full)
        text_preview_ascii = strip_accents(text_preview)

        yield {
            "doc_id": doc_id,
            "article_number": article_number,
            "title": article_title,         # <= Art. N, <Titre du doc>
            "page_start": p_start,
            "page_end": p_end,
            "text_full": text_full,
            "text_full_ascii": text_full_ascii,
            "text_preview": text_preview,
            "text_preview_ascii": text_preview_ascii,
            "text_chars": len(text_full),
            "pdf_path": str(pdf_path.resolve()),
        }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", nargs="+", help="PDF(s) en entrée", required=False)
    ap.add_argument("--pdfs", help="Dossier de PDF", required=False)
    ap.add_argument("--out", default="./out/articles.jsonl")
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    inputs = []
    if args.pdf:
        inputs.extend([Path(p) for p in args.pdf])
    if args.pdfs:
        inputs.extend(list(Path(args.pdfs).glob("*.pdf")))
    inputs = [p for p in inputs if p.exists()]

    if not inputs:
        print("[pdf2articles] Aucun PDF trouvé.")
        return

    count = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for pdf in inputs:
            for rec in process_pdf(pdf):
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                count += 1

    print(f"[pdf2articles] Écrit {count} articles -> {out_path}")

if __name__ == "__main__":
    main()
