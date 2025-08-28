#!/usr/bin/env python3
"""
viewer_app.py (enhanced)
------------------------
Gradio UI to browse article-level records and view side-by-side:
- Article metadata and text
- Per-page HTML snapshots (absolute-positioned) with prev/next navigation
- Tables detected on the current page (from tables_pass.py)

Usage:
    python viewer_app.py --articles ./out/articles.jsonl --bind 0.0.0.0 --port 7860 --auth user:pass
"""
import argparse, json, os, gradio as gr
from pathlib import Path

def load_articles(path: Path):
    items = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            a = json.loads(line)
            items.append(a)
    items.sort(key=lambda x: (x["doc_id"], x["article_number"]))
    return items

def list_tables_for(articles_base: Path, doc_id: str, article_number: str, page: int):
    # tables stored at out/tables/<doc_id>/<Art_X>/tables.json
    safe_art = article_number.replace(" ", "_").replace(".", "")
    meta_path = articles_base.parent / "tables" / doc_id / safe_art / "tables.json"
    results = []
    if meta_path.exists():
        try:
            data = json.loads(meta_path.read_text(encoding="utf-8"))
            for t in data:
                if int(t.get("page", -1)) == int(page):
                    results.append(t["csv"])
        except Exception:
            pass
    return results

def make_ui(articles, articles_path: Path):
    labels = [f"{a['doc_id']} | {a['article_number']} | p{a['page_start']}-{a['page_end']}" for a in articles]

    def pick(idx):
        idx = int(idx)
        a = articles[idx]
        pages = a.get("html_pages", [])
        current = 0 if pages else -1
        html_path = pages[current] if current >= 0 else ""
        text = f"### {a['doc_id']} — {a['article_number']}\n\n**Pages:** {a['page_start']}–{a['page_end']}\n\n" + a["text"]
        # deduce actual page number shown
        page_no = a["page_start"] + current if current >=0 else None
        tables = list_tables_for(articles_path, a["doc_id"], a["article_number"], page_no) if page_no else []
        return text, current, len(pages), html_path, "\n".join(tables)

    def nav(idx, current, step):
        idx = int(idx)
        a = articles[idx]
        pages = a.get("html_pages", [])
        if not pages:
            return current, "", gr.update(visible=False), ""
        current = max(0, min(current + step, len(pages) - 1))
        html_path = pages[current]
        page_no = a["page_start"] + current
        tables = list_tables_for(articles_path, a["doc_id"], a["article_number"], page_no)
        return current, html_path, gr.update(visible=True), "\n".join(tables)

    with gr.Blocks() as demo:
        gr.Markdown("# Legal PDF Articles — Viewer")

        with gr.Row():
            with gr.Column(scale=1):
                dropdown = gr.Dropdown(choices=labels, label="Pick an article", value=labels[0] if labels else None)
                idx_state = gr.State(0)
                current_page = gr.State(0)
                total_pages = gr.State(0)

                def on_change(label):
                    i = labels.index(label)
                    text, cur, total, html, tbls = pick(i)
                    return i, text, cur, total, gr.update(value=html, visible=bool(html)), tbls

                out_text = gr.Markdown()
                html_view = gr.HTML(visible=False)
                prev_btn = gr.Button("Prev page")
                next_btn = gr.Button("Next page")
                tables_box = gr.Textbox(label="Tables (CSV paths) on current page", interactive=False, lines=6)

                dropdown.change(
                    on_change,
                    inputs=[dropdown],
                    outputs=[idx_state, out_text, current_page, total_pages, html_view, tables_box]
                )

                def do_prev(idx, cur):
                    new_cur, html, vis, tbls = nav(idx, cur, -1)
                    return new_cur, gr.update(value=html), vis, tbls

                def do_next(idx, cur):
                    new_cur, html, vis, tbls = nav(idx, cur, +1)
                    return new_cur, gr.update(value=html), vis, tbls

                prev_btn.click(do_prev, inputs=[idx_state, current_page], outputs=[current_page, html_view, html_view, tables_box])
                next_btn.click(do_next, inputs=[idx_state, current_page], outputs=[current_page, html_view, html_view, tables_box])

            with gr.Column(scale=1):
                gr.Markdown("### Page snapshot")
                html_view2 = gr.HTML()
                def mirror(html):
                    return html
                html_view.change(mirror, inputs=[html_view], outputs=[html_view2])

    return demo

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--articles", required=True, help="Path to out/articles.jsonl")
    ap.add_argument("--bind", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7860)
    ap.add_argument("--auth", default=None, help="Format user:pass (optional)")
    args = ap.parse_args()

    articles = load_articles(Path(args.articles))
    demo = make_ui(articles, Path(args.articles))

    auth = None
    if args.auth and ":" in args.auth:
        user, pwd = args.auth.split(":", 1)
        auth = (user, pwd)

    demo.launch(server_name=args.bind, server_port=args.port, auth=auth)

if __name__ == "__main__":
    main()
