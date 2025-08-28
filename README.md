# PDF → Article-level ingestion (GraphRAG-friendly)

This mini toolchain ingests **legal PDFs** while **preserving structure & provenance**, then produces **article-level** units for a GraphRAG index.

## Why this?
- Legal QA needs **events/covariates** grounded in *who/what/when/where (doc/page)*.
- Random chunking ruins chronology. Instead we emit **one record per article** and keep **page ranges** + **HTML sidecars** for visual provenance.
- Works with the official GraphRAG pipeline (or Nano/LlamaIndex) because we output simple JSONL units with rich `metadata`.

---

## Files
- `pdf2articles.py` — parses PDFs with PyMuPDF and writes:
  - `out/articles.jsonl` — one JSON object **per article**.
  - `out/html/<doc_id>_pN.html` — per-page HTML snapshots (absolutely-positioned spans, useful for citations/UI).
- `to_graphrag_jsonl.py` — reshapes `articles.jsonl` into GraphRAG-friendly JSONL (`id`, `title`, `text`, `metadata`).

> Note: We *do not* rasterize PDFs; we extract text with font/position info and keep page-level HTML sidecars for structure. You still keep the original PDF path in metadata.

---

## Install
On your machine (or inside your GraphRAG env):
```bash
pip install pymupdf
```

## Usage
1) Put your PDFs in `./pdfs/` (subfolders OK).
2) Extract articles:
```bash
python pdf2articles.py --pdfs ./pdfs --out ./out --jurisdiction CH --lang fr
```
Outputs:
- `./out/articles.jsonl`
- `./out/html/*.html`

3) Convert to GraphRAG-ready JSONL:
```bash
python to_graphrag_jsonl.py --articles ./out/articles.jsonl --out ./graphrag_input.jsonl
```

4) Feed `graphrag_input.jsonl` into your GraphRAG ingestion step (as the input document set). Each record = one article with provenance in `metadata`.

---

## Tuning notes
- **Article header regex**: supports `Article|Art.|Articolo|Artikel|§` plus roman numerals or digits (e.g., `Art. 5`, `Article IV`). Tweak `ARTICLE_HEADER` in `pdf2articles.py` if your corpus uses different headings.
- **Layout preservation**: per-page HTML uses absolute coordinates + font sizes from PyMuPDF. This lets a UI highlight exact spans later.
- **Covariates/events**: once indexed, have your extractor generate events using the `metadata` (`doc_id`, `article_number`, `page_start/end`) for exact citations in answers.
- **Tables**: if your corpus contains important tables, consider running a table extractor (e.g., Camelot/Tabula) in a second pass and attach extracted CSV/JSON to the same article records.

## Security
If you serve the HTML sidecars or the Gradio UI from a VM, bind to `0.0.0.0` and use bridged networking or NAT+port-forwarding. Add basic auth or put it behind a reverse proxy/tunnel.

---

## License
MIT (feel free to adapt).
