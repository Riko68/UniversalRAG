#!/usr/bin/env python3
"""
tables_pass.py
--------------
Extract tables from PDFs referenced in out/articles.jsonl.
- Uses Camelot if available (better for vector PDFs), else falls back to pdfplumber.
- Saves per-page CSV files and aggregates per-article JSON metadata listing all tables.

Usage:
    python tables_pass.py --articles ./out/articles.jsonl --out ./out

Notes:
- Camelot requires Ghostscript and works best with "lattice" on vector PDFs; "stream" can help for text-based.
- pdfplumber fallback is pure-Python but less precise.
"""
import argparse, json, os
from pathlib import Path

def try_import_camelot():
    try:
        import camelot
        return camelot
    except Exception:
        return None

def try_import_pdfplumber():
    try:
        import pdfplumber
        return pdfplumber
    except Exception:
        return None

def extract_with_camelot(pdf_path, pages):
    camelot = try_import_camelot()
    if camelot is None:
        return []
    results = []
    page_spec = ",".join(str(p) for p in pages)
    # Try lattice first, then stream as a fallback per doc
    for flavor in ("lattice", "stream"):
        try:
            tables = camelot.read_pdf(str(pdf_path), pages=page_spec, flavor=flavor)
            for t in tables:
                results.append({
                    "page": t.page,
                    "flavor": flavor,
                    "df": t.df
                })
            break
        except Exception:
            continue
    return results

def extract_with_pdfplumber(pdf_path, pages):
    pdfplumber = try_import_pdfplumber()
    if pdfplumber is None:
        return []
    out = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for pno in pages:
            if pno - 1 < 0 or pno - 1 >= len(pdf.pages):
                continue
            page = pdf.pages[pno - 1]
            try:
                tables = page.extract_tables()
            except Exception:
                tables = []
            for tbl in tables:
                out.append({
                    "page": pno,
                    "rows": tbl
                })
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--articles", required=True, help="Path to out/articles.jsonl")
    ap.add_argument("--out", required=True, help="Output directory (same as used by pdf2articles.py)")
    args = ap.parse_args()

    articles_path = Path(args.articles)
    out_dir = Path(args.out)
    tables_dir = out_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    use_camelot = try_import_camelot() is not None
    use_pdfplumber = try_import_pdfplumber() is not None

    if not (use_camelot or use_pdfplumber):
        raise RuntimeError("Install either camelot-py[cv] + Ghostscript OR pdfplumber to enable table extraction.")

    with articles_path.open("r", encoding="utf-8") as fin:
        for line in fin:
            art = json.loads(line)
            doc_id = art["doc_id"]
            art_no = art["article_number"].replace(" ", "_").replace(".", "")
            pdf_path = Path(art["pdf_path"])
            pages = list(range(int(art["page_start"]), int(art["page_end"]) + 1))

            per_article_dir = tables_dir / doc_id / art_no
            per_article_dir.mkdir(parents=True, exist_ok=True)

            aggregated = []
            if use_camelot:
                try:
                    tables = extract_with_camelot(pdf_path, pages)
                    for idx, t in enumerate(tables, start=1):
                        csv_path = per_article_dir / f"page{t['page']}_t{idx}_camelot.csv"
                        # Camelot DataFrame has to_csv
                        t["df"].to_csv(csv_path, index=False, header=False)
                        aggregated.append({
                            "engine": "camelot",
                            "page": t["page"],
                            "csv": csv_path.as_posix()
                        })
                except Exception:
                    pass

            if not aggregated and use_pdfplumber:
                # fallback / supplement
                tables = extract_with_pdfplumber(pdf_path, pages)
                for idx, t in enumerate(tables, start=1):
                    csv_path = per_article_dir / f"page{t['page']}_t{idx}_plumber.csv"
                    # Write CSV manually
                    import csv
                    with open(csv_path, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        for row in t["rows"]:
                            writer.writerow(row)
                    aggregated.append({
                        "engine": "pdfplumber",
                        "page": t["page"],
                        "csv": csv_path.as_posix()
                    })

            meta_path = per_article_dir / "tables.json"
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(aggregated, f, ensure_ascii=False, indent=2)

            print(f"[tables_pass] {doc_id} {art['article_number']}: {len(aggregated)} table(s)")

if __name__ == "__main__":
    main()
