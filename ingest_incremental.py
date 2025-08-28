#!/usr/bin/env python3
"""
ingest_incremental.py
---------------------
Incremental ingestion for PDFs:
- Scans a PDF folder.
- Computes a hash (md5) per file to detect NEW or CHANGED PDFs.
- Runs pdf2articles.py ONLY for those PDFs and appends to out/articles.jsonl.
- Maintains ./out/ingest_manifest.json with file hashes + last processed time.

Usage:
    python ingest_incremental.py --pdfs ./pdfs --out ./out --jurisdiction CH --lang fr

Notes:
- If you rename a PDF without changing content, its hash stays the same but doc_id (filename) changes;
  we treat that as NEW (because doc_id derives from filename). If you want stable IDs, keep names stable.
- To force re-indexing a file, delete its entry from ingest_manifest.json or pass --force.
"""
import argparse, hashlib, json, os, subprocess, sys
from pathlib import Path
from datetime import datetime

def md5_file(path: Path, blocksize=65536):
    h = hashlib.md5()
    with open(path, 'rb') as f:
        while True:
            data = f.read(blocksize)
            if not data:
                break
            h.update(data)
    return h.hexdigest()

def load_manifest(out_dir: Path):
    mpath = out_dir / "ingest_manifest.json"
    if mpath.exists():
        try:
            return json.loads(mpath.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_manifest(out_dir: Path, manifest: dict):
    mpath = out_dir / "ingest_manifest.json"
    mpath.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdfs", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--jurisdiction", default="CH")
    ap.add_argument("--lang", default="fr")
    ap.add_argument("--force", action="store_true", help="Re-index all PDFs regardless of manifest")
    args = ap.parse_args()

    pdf_dir = Path(args.pdfs)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = {} if args.force else load_manifest(out_dir)

    pdfs = sorted([p for p in pdf_dir.glob("**/*.pdf") if p.is_file()])
    to_process = []

    for p in pdfs:
        key = str(p.resolve())
        h = md5_file(p)
        rec = manifest.get(key)
        if not rec or rec.get("md5") != h:
            to_process.append(p)
            manifest[key] = {"md5": h, "last_indexed": None}

    if not to_process:
        print("[ingest_incremental] Nothing new to ingest.")
        save_manifest(out_dir, manifest)
        sys.exit(0)

    print(f"[ingest_incremental] Will ingest {len(to_process)} new/changed PDF(s).")

    # Call pdf2articles.py once for the whole folder (will append)
    # but to be safe with very large corpora, we could loop per file.
    cmd = [sys.executable, "pdf2articles.py", "--pdfs", str(pdf_dir), "--out", str(out_dir),
           "--jurisdiction", args.jurisdiction, "--lang", args.lang]
    print("[ingest_incremental] Running:", " ".join(cmd))
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr, file=sys.stderr)
        print(f"[ingest_incremental] ERROR running pdf2articles.py", file=sys.stderr)
        sys.exit(1)

    # Mark all processed
    now = datetime.utcnow().isoformat() + "Z"
    for p in to_process:
        manifest[str(p.resolve())]["last_indexed"] = now

    save_manifest(out_dir, manifest)
    print("[ingest_incremental] Done. Manifest updated:", out_dir / "ingest_manifest.json")

if __name__ == "__main__":
    main()
