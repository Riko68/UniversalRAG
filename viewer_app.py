#!/usr/bin/env python3
"""
viewer_app.py (enhanced+)
-------------------------
- Browse articles and HTML page snapshots.
- Show CSV tables on current page and preview selected table inline.
- Provide a deep link to open the original PDF at the current page.
"""
import argparse, json, os, gradio as gr, pandas as pd
from pathlib import Path

def load_articles(path: Path):
    items = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            a = json.loads(line)
            items.append(a)
    items.sort(key=lambda x: (x["doc_id"], x["article_number"]))
    return items

def list_tables_for(out_dir: Path, doc_id: str, article_number: str, page: int):
    safe_art = article_number.replace(" ", "_").replace(".", "")
    meta_path = out_dir / "tables" / doc_id / safe_art / "tables.json"
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

def load_csv_preview(path_str: str):
    try:
        df = pd.read_csv(path_str, header=None)
        return df
    except Exception as e:
        return pd.DataFrame({"error":[str(e)], "path":[path_str]})

def make_ui(articles, out_dir: Path):
    labels = [f"{a['doc_id']} | {a['article_number']} | p{a['page_start']}-{a['page_end']}" for a in articles]

    def pick(idx):
        idx = int(idx)
        a = articles[idx]
        pages = a.get("html_pages", [])
        current = 0 if pages else -1
        html_path = pages[current] if current >= 0 else ""
        html_content = Path(html_path).read_text(encoding="utf-8") if html_path else ""
        text = f"### {a['doc_id']} — {a['article_number']}\n\n**Pages:** {a['page_start']}–{a['page_end']}\n\n" + a["text"]
        page_no = a["page_start"] + current if current >=0 else None
        tables = list_tables_for(out_dir, a["doc_id"], a["article_number"], page_no) if page_no else []
        pdf_link = ""
        if a.get("pdf_path") and page_no:
            pdf_link = f"[Open PDF page {page_no}]({a['pdf_path']}#page={page_no})"
        return text, current, len(pages), html_content, "\n".join(tables), tables, pdf_link

    def nav(idx, current, step):
        idx = int(idx)
        a = articles[idx]
        pages = a.get("html_pages", [])
        if not pages:
            return current, "", gr.update(visible=False), "", [], ""
        current = max(0, min(current + step, len(pages) - 1))
        html_path = pages[current]
        html_content = Path(html_path).read_text(encoding="utf-8") if html_path else ""
        page_no = a["page_start"] + current
        tables = list_tables_for(out_dir, a["doc_id"], a["article_number"], page_no)
        pdf_link = f"[Open PDF page {page_no}]({a['pdf_path']}#page={page_no})" if a.get("pdf_path") else ""
        return current, html_content, gr.update(visible=True), "\n".join(tables), tables, pdf_link

    with gr.Blocks() as demo:
        gr.Markdown("# Legal PDF Articles — Viewer")

        with gr.Row():
            with gr.Column(scale=1):
                dropdown = gr.Dropdown(choices=labels, label="Pick an article", value=labels[0] if labels else None)
                idx_state = gr.State(0)
                current_page = gr.State(0)
                total_pages = gr.State(0)

                out_text = gr.Markdown()
                html_view = gr.HTML(visible=False)
                prev_btn = gr.Button("Prev page")
                next_btn = gr.Button("Next page")
                tables_box = gr.Textbox(label="Tables (CSV paths) on current page", interactive=False, lines=6)
                table_picker = gr.Dropdown(label="Preview a table (CSV)", choices=[])
                table_preview = gr.Dataframe(headers=None, label="CSV preview (first rows)")
                pdf_deeplink = gr.Markdown("")

                def on_change(label):
                    i = labels.index(label)
                    text, cur, total, html, tbls_text, tbls_list, pdf_link = pick(i)
                    return i, text, cur, total, gr.update(value=html, visible=bool(html)), tbls_text, gr.update(choices=tbls_list, value=(tbls_list[0] if tbls_list else None)), pdf_link

                dropdown.change(
                    on_change,
                    inputs=[dropdown],
                    outputs=[idx_state, out_text, current_page, total_pages, html_view, tables_box, table_picker, pdf_deeplink]
                )

                def do_prev(idx, cur):
                    new_cur, html, vis, tbls_text, tbls_list, pdf_link = nav(idx, cur, -1)
                    return new_cur, gr.update(value=html), vis, tbls_text, gr.update(choices=tbls_list, value=(tbls_list[0] if tbls_list else None)), pdf_link

                def do_next(idx, cur):
                    new_cur, html, vis, tbls_text, tbls_list, pdf_link = nav(idx, cur, +1)
                    return new_cur, gr.update(value=html), vis, tbls_text, gr.update(choices=tbls_list, value=(tbls_list[0] if tbls_list else None)), pdf_link

                prev_btn.click(do_prev, inputs=[idx_state, current_page], outputs=[current_page, html_view, html_view, tables_box, table_picker, pdf_deeplink])
                next_btn.click(do_next, inputs=[idx_state, current_page], outputs=[current_page, html_view, html_view, tables_box, table_picker, pdf_deeplink])

                def on_pick_table(path):
                    if not path:
                        return gr.update(value=None)
                    df = load_csv_preview(path)
                    return df

                table_picker.change(on_pick_table, inputs=[table_picker], outputs=[table_preview])

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

    arts_path = Path(args.articles)
    out_dir = arts_path.parent  # 'out' folder
    articles = load_articles(arts_path)
    demo = make_ui(articles, out_dir)

    auth = None
    if args.auth and ":" in args.auth:
        user, pwd = args.auth.split(":", 1)
        auth = (user, pwd)

    demo.launch(server_name=args.bind, server_port=args.port, auth=auth)

if __name__ == "__main__":
    main()
