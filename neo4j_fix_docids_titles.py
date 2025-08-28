#!/usr/bin/env python3
"""
neo4j_fix_docids_titles.py
--------------------------
Corrige doc_id et title des Articles/Document à partir de la 1re page du PDF.

- Pour chaque groupe d'Articles partageant le même pdf_path :
  * doc_id := regex sur la 1re page (ex: 0.747.205)
  * doc_title := agrégation des plus gros blocs typographiques de la 1re page
  * Article.title := f"Art. <N>, {doc_title}"
  * Article.doc_id := <doc_id>
  * MERGE (:Document {doc_id}) SET d.title = doc_title
  * Re-câble (:Document)-[:HAS_ARTICLE]->(:Article)
  * Supprime l'ancien (:Document) s'il est orphelin

Par défaut: dry-run (affiche les changements). Utiliser --commit pour appliquer.

Usage:
  python neo4j_fix_docids_titles.py --uri bolt://127.0.0.1:7687 --user neo4j --password "pwd" [--limit 50] [--commit]
"""

import argparse
import re
import sys
from pathlib import Path

import fitz  # PyMuPDF
from neo4j import GraphDatabase

DOC_ID_RE = re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\b")
ART_RE = re.compile(r"^\s*Art\.\s*([\w\-]+)", re.IGNORECASE)

def guess_doc_id_from_first_page_text(text: str) -> str | None:
    m = DOC_ID_RE.search(text or "")
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





def probe_pdf_first_page(pdf_path: str) -> tuple[str | None, str | None]:
    """Retourne (doc_id, doc_title) ou (None, None) si lecture impossible."""
    try:
        p = Path(pdf_path)
        if not p.exists():
            return None, None
        doc = fitz.open(p)
        if len(doc) == 0:
            return None, None
        page0 = doc[0]
        text = page0.get_text()
        doc_id = guess_doc_id_from_first_page_text(text)
        doc_title = guess_doc_title_from_first_page(page0)
        return doc_id, doc_title
    except Exception:
        return None, None

CYPHER_LIST_PDFS = """
MATCH (a:Article)
WHERE a.pdf_path IS NOT NULL AND a.pdf_path <> ''
WITH a.pdf_path AS pdf_path, collect({doc_id:a.doc_id, article_number:a.article_number}) AS arts
RETURN pdf_path, arts
ORDER BY pdf_path
"""

CYPHER_UPDATE_GROUP = """
// Paramètres:
// $oldDocIds  : [string] doc_id(s) actuels observés sur les articles du groupe
// $pdf_path   : string
// $newDocId   : string (recalculé)
// $docTitle   : string (titre documentaire)
// $arts       : [{article_number: "..."}] articles à mettre à jour

// 1) MERGE le Document cible
MERGE (d2:Document {doc_id: $newDocId})
ON CREATE SET d2.title = $docTitle
ON MATCH  SET d2.title = coalesce($docTitle, d2.title)

// 2) Pour chaque Article de ce PDF, mettre à jour doc_id/title & recâbler
WITH d2, $pdf_path AS pdf_path, $newDocId AS newDocId, $docTitle AS docTitle, $arts AS arts
UNWIND arts AS art
MATCH (a:Article {pdf_path: pdf_path, article_number: art.article_number})
// on garde la valeur d'origine pour le re-câblage
WITH d2, a, newDocId, docTitle
OPTIONAL MATCH (d1:Document)-[r:HAS_ARTICLE]->(a)
SET a.doc_id = newDocId,
    a.title  = a.article_number + ', ' + docTitle
// recâblage
DELETE r
MERGE (d2)-[:HAS_ARTICLE]->(a)

// 3) Nettoyage: supprimer Documents orphelins (sans articles)
WITH collect(DISTINCT d2) AS keepers
MATCH (dx:Document)
WHERE NOT (dx)-[:HAS_ARTICLE]->(:Article)
  AND NOT dx IN keepers
DETACH DELETE dx
"""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--limit", type=int, default=None, help="Limiter le nombre de pdf_path traités")
    ap.add_argument("--commit", action="store_true", help="Appliquer les changements (sinon dry-run)")
    args = ap.parse_args()

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    total = 0
    fixed = 0
    skipped = 0

    with driver.session() as session:
        groups = session.run(CYPHER_LIST_PDFS).data()
        if args.limit:
            groups = groups[: args.limit]

        for g in groups:
            pdf_path = g["pdf_path"]
            arts = g["arts"]
            total += 1

            new_doc_id, doc_title = probe_pdf_first_page(pdf_path)
            if not new_doc_id and not doc_title:
                print(f"[skip] PDF illisible ou absent: {pdf_path}")
                skipped += 1
                continue

            # doc_id fallback si manquant
            if not new_doc_id:
                # fallback: garder l'ancien doc_id majoritaire
                old_ids = [a["doc_id"] for a in arts if a.get("doc_id")]
                new_doc_id = old_ids[0] if old_ids else Path(pdf_path).stem

            if not doc_title:
                doc_title = "Titre inconnu"

            # journal
            sample_arts = ", ".join(sorted(a["article_number"] for a in arts)[:5])
            print(f"\n[pdf] {pdf_path}")
            print(f"  -> new_doc_id = {new_doc_id}")
            print(f"  -> doc_title  = {doc_title}")
            print(f"  -> articles   = {len(arts)} (ex: {sample_arts}{'…' if len(arts)>5 else ''})")

            if args.commit:
                session.run(
                    CYPHER_UPDATE_GROUP,
                    {
                        "oldDocIds": list({a["doc_id"] for a in arts if a.get("doc_id")}),
                        "pdf_path": pdf_path,
                        "newDocId": new_doc_id,
                        "docTitle": doc_title,
                        "arts": [{"article_number": a["article_number"]} for a in arts],
                    },
                )
                print("  [ok] appliqué dans Neo4j")
                fixed += 1
            else:
                print("  [dry-run] aucune écriture (utilise --commit pour appliquer)")

    driver.close()
    print(f"\n[Résumé] groupés par pdf_path: {total} | appliqués: {fixed} | ignorés: {skipped}")

if __name__ == "__main__":
    sys.exit(main())
