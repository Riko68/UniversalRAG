#!/usr/bin/env python3
"""
chat_app.py
-----------
A browser app with a chat-like interface to interact with your Neo4j + OpenAI GraphRAG pipeline.

Features
- Chat interface (per-session history, no persistence).
- Retrieval modes: metadata (titles/events) or full-text (Neo4j FT index).
- Shows generated Cypher + top results for transparency.
- Synthesizes a cited answer in FR/EN (every assertive sentence ends with a citation).
- "New conversation" button to reset history.
- Right-hand preview panel with clickable sources (opens snippet + PDF deeplink).
- Sends session conversation history back to OpenAI to keep context.
- Deduplicate identical Articles in results.
- Clickable PDF links via file:// or optional local HTTP static server.
- USE TITLES in citations, dropdown, and preview header.

Requirements
  pip install gradio neo4j "openai>=1.0.0,<2"

Env vars (optional)
  OPENAI_API_KEY
  NEO4J_URI (default bolt://127.0.0.1:7687)
  NEO4J_USER (default neo4j)
  NEO4J_PASSWORD

Run
  python chat_app.py
"""
import os
import re
import threading
import http.server
import socketserver
import functools
from pathlib import Path
import gradio as gr
from neo4j import GraphDatabase
from openai import OpenAI
from typing import List, Dict, Any, Tuple, Optional

# --------- Helpers ---------

def strip_accents(s: str) -> str:
    import unicodedata
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))

def build_ft_query_from_question(q: str) -> str:
    q_ascii = strip_accents(q.lower())
    toks = re.findall(r"[a-zA-Z]{3,}", q_ascii)
    if not toks:
        return q_ascii or "navigation"
    parts = []
    for t in toks[:8]:
        if len(t) >= 6:
            parts.append(f'{t}~2')
        elif len(t) >= 4:
            parts.append(f'{t}*')
        else:
            parts.append(t)
    return " OR ".join(parts)

def run_query(session, cypher: str, params=None):
    res = session.run(cypher, params or {})
    return [r.data() for r in res]

def tokenize_keywords(q: str) -> List[str]:
    return [w for w in re.findall(r"[a-zA-ZÀ-ÿ]{3,}", strip_accents(q.lower()))]

def pick_sentences(text: str, keywords: List[str], max_chars: int = 800) -> str:
    sents = re.split(r"(?<=[\.\!\?])\s+", (text or "").strip())
    def score(s):
        t = strip_accents(s.lower())
        return sum(k in t for k in keywords)
    ranked = sorted(sents, key=score, reverse=True)
    buf, total = [], 0
    for s in ranked:
        if score(s) == 0:
            continue
        if total + len(s) > max_chars:
            break
        buf.append(s.strip())
        total += len(s)
    if not buf:
        for s in sents[:3]:
            if total + len(s) > max_chars: break
            buf.append(s.strip()); total += len(s)
    return " ".join(buf)

def enforce_citations(text: str) -> str:
    lines = [l.rstrip() for l in text.splitlines()]
    fixed = []
    for l in lines:
        if l.strip().startswith("- "):
            if not re.search(r"\[[^\]]+\]\.?(\s*\(.+\))?$", l):
                l += " [source manquante]"
        fixed.append(l)
    return "\n".join(fixed)

def _display_title(e: dict, max_len: int = 100) -> str:
    """Prefer the human title; fallback to doc_id. Truncate politely."""
    t = (e.get("title") or e.get("doc_id") or "Document").strip()
    t = re.sub(r"\s+", " ", t)
    if len(t) > max_len:
        t = t[: max_len - 1].rstrip() + "…"
    return t

def synthesize_answer_with_history(
    client: OpenAI,
    history_msgs: List[Dict[str, str]],
    question: str,
    evidence: List[Dict[str, Any]],
    language: str,
    model: str
) -> str:
    """Chat synthesis that includes prior turns (stateless API, we resend history)."""
    sys_prompt = (
        "ROLE: Juriste assistant, réponses factuelles et sourcées.\n"
        "BUT: Répondre à la question dans la langue demandée, uniquement avec des informations "
        "provenant des PREUVES fournies (articles et extraits).\n"
        "REGLES:\n"
        "1) Chaque phrase assertive DOIT finir par une citation [Titre, Art. X, pA–B].\n"
        "2) N'invente rien; si l'info manque, dis-le explicitement.\n"
        "3) Regrouper/dédupliquer, style clair et concis.\n"
        "4) Conserver la terminologie juridique telle quelle.\n"
        "5) Si un lien PDF est fourni, tu peux l'ajouter entre parenthèses juste après la citation.\n"
        "SORTIE: Liste à puces de points, puis un court paragraphe de synthèse également sourcé.\n"
    )
    # Compact evidence
    ev_lines = []
    for e in evidence or []:
        line = (
            f"- {_display_title(e, 110)} | {e.get('article_number','?')} | "
            f"p{e.get('page_start','?')}-{e.get('page_end','?')}\n"
            f"  Titre: {e.get('title','(sans titre)')}\n"
            f"  Extraits: {e.get('snippet','')}\n"
            f"  Citation: {e.get('citation','[source ?]')}"
        )
        pdf = e.get("pdf")
        if pdf:
            line += f"\n  PDF: {pdf}"
        ev_lines.append(line)
    ev_block = "\n".join(ev_lines) if ev_lines else "(aucune preuve)"
    user_prompt = (
        f"Question: {question}\n\n"
        f"PREUVES:\n{ev_block}\n\n"
        f"Réponds en {language}. Chaque phrase doit être sourcée."
    )
    # Build messages with prior history (send last ~8)
    msgs = [{"role": "system", "content": sys_prompt}]
    for m in (history_msgs or [])[-8:]:
        msgs.append({"role": m.get("role","user"), "content": m.get("content","")})
    msgs.append({"role": "user", "content": user_prompt})
    resp = client.chat.completions.create(
        model=model or "gpt-4o-mini",
        temperature=0.2,
        messages=msgs,
    )
    draft = resp.choices[0].message.content.strip()
    return enforce_citations(draft)

def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep first row per (article_doc_id, article_number)."""
    seen = set()
    out = []
    for r in rows:
        key = (
            r.get("article_doc_id") or r.get("a.doc_id") or r.get("node.doc_id") or r.get("doc_id"),
            r.get("article_number") or r.get("a.article_number") or r.get("node.article_number"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def path_to_file_uri(p: str) -> Optional[str]:
    if not p:
        return None
    try:
        return Path(p).resolve().as_uri()  # -> file:///C:/...
    except Exception:
        return None

def http_url_for_pdf(pdf_path: str, root: str, host: str, port: int) -> Optional[str]:
    """Return http://host:port/<relative> if pdf_path is inside root."""
    try:
        root_path = Path(root).resolve()
        pdf = Path(pdf_path).resolve()
        rel = pdf.relative_to(root_path)
        return f"http://{host}:{port}/{str(rel).replace(os.sep, '/')}"
    except Exception:
        return None

def gather_evidence(
    session,
    rows: List[Dict[str, Any]],
    keywords: List[str],
    k: int = 8,
    evidence_chars: int = 800,
    pdf_http: bool = False,
    pdf_root: str = "",
    pdf_host: str = "127.0.0.1",
    pdf_port: int = 7863,
) -> Tuple[List[Dict[str, Any]], str]:
    """Return (evidence_list, debug_markdown)"""
    use = rows[:k]
    ev, dbg = [], []
    for idx, r in enumerate(use, start=1):
        # Always carry both IDs:
        doc_id = r.get("doc_id") or r.get("d.doc_id") or r.get("node.doc_id")
        article_doc_id = (
            r.get("article_doc_id") or r.get("a.doc_id") or r.get("node.doc_id") or doc_id
        )
        art = r.get("article_number") or r.get("a.article_number") or r.get("node.article_number")
        title = r.get("title") or r.get("a.title") or r.get("node.title")
        ps = r.get("page_start") or r.get("a.page_start") or r.get("node.page_start")
        pe = r.get("page_end") or r.get("a.page_end") or r.get("node.page_end")

        # Primary: match the Article by its own doc_id + number
        rec = session.run(
            "MATCH (a:Article {doc_id:$doc, article_number:$art}) "
            "RETURN a.text_full AS t, coalesce(a.pdf_path, '') AS pdf",
            {"doc": article_doc_id, "art": art}
        ).single()

        full = ""; pdf_path = ""
        if rec:
            full = rec.get("t") or ""
            pdf_path = rec.get("pdf") or ""

        # Fallback: if not found, use the Document relation
        if not full:
            rec2 = session.run(
                "MATCH (d:Document {doc_id:$doc})-[:HAS_ARTICLE]->(a:Article {article_number:$art}) "
                "RETURN a.text_full AS t, coalesce(a.pdf_path, '') AS pdf",
                {"doc": doc_id, "art": art}
            ).single()
            if rec2:
                full = rec2.get("t") or ""
                pdf_path = rec2.get("pdf") or ""

        snippet = pick_sentences(full, keywords, evidence_chars)

        # ----- CITATION uses TITLE instead of doc_id -----
        title_for_cite = title or doc_id or "Document"
        title_for_cite = re.sub(r"[\[\]]", "", title_for_cite).strip()
        if len(title_for_cite) > 140:
            title_for_cite = title_for_cite[:137].rstrip() + "…"
        citation = f"[{title_for_cite}, {art}, p{ps}–{pe}]"

        # Build link
        link = None
        if pdf_http and pdf_root:
            http_url = http_url_for_pdf(pdf_path, pdf_root, pdf_host, pdf_port)
            if http_url:
                link = f"{http_url}#page={ps}"
        if not link:
            file_uri = path_to_file_uri(pdf_path)
            link = f"{file_uri}#page={ps}" if file_uri else None

        ev.append({
            "doc_id": doc_id, "article_number": art, "title": title,
            "page_start": ps, "page_end": pe, "snippet": snippet,
            "citation": citation, "pdf": link
        })
        dbg.append(f"{idx}. {title_for_cite} | {art} | p{ps}-{pe} — {len(snippet)} chars")
    return ev, "\n".join(dbg)

def render_preview_md(e: Dict[str, Any]) -> str:
    """Pretty Markdown for the right-hand preview."""
    if not e:
        return ""
    header_title = _display_title(e, max_len=140)  # use TITLE in header
    lines = [
        f"### {header_title} — {e.get('article_number','?')} (p{e.get('page_start','?')}-{e.get('page_end','?')})",
        f"**Titre** : {e.get('title','(sans titre)')}",
    ]
    snip = e.get("snippet") or ""
    if snip:
        lines += ["", "**Extrait**", "", "> " + snip.replace("\n", "\n> ")]
    lines += ["", f"**Citation** : {e.get('citation','[source ?]')}"]
    if e.get("pdf"):
        lines += [f"[Ouvrir le PDF]({e['pdf']})"]
    else:
        lines += ["_PDF indisponible pour cet article._"]
    return "\n".join(lines)

# --------- Tiny static HTTP server for PDFs ---------

class _PDFServer:
    """Manage a background HTTP server that serves a directory."""
    def __init__(self):
        self.server = None
        self.thread = None
        self.root = ""
        self.host = "127.0.0.1"
        self.port = 7863

    def start(self, root: str, host: str = "127.0.0.1", port: int = 7863):
        if self.server:  # already running
            return
        handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(Path(root).resolve()))
        httpd = socketserver.TCPServer((host, int(port)), handler)
        t = threading.Thread(target=httpd.serve_forever, daemon=True)
        t.start()
        self.server = httpd
        self.thread = t
        self.root = str(Path(root).resolve())
        self.host = host
        self.port = int(port)

    def info(self):
        if not self.server:
            return None
        return {"root": self.root, "host": self.host, "port": self.port}

_pdf_server = _PDFServer()

# --------- Gradio App ---------

def make_app():
    default_uri = os.environ.get("NEO4J_URI", "bolt://127.0.0.1:7687")
    default_user = os.environ.get("NEO4J_USER", "neo4j")
    default_pwd = os.environ.get("NEO4J_PASSWORD", "")
    default_model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    have_key = bool(os.environ.get("OPENAI_API_KEY"))

    with gr.Blocks(title="GraphRAG Legal Chat") as demo:
        gr.Markdown("# 🧭 GraphRAG Legal Chat\nPosez une question; je récupère des articles/événements et j'écris une réponse sourcée.")

        with gr.Accordion("⚙️ Connexion & options", open=False):
            uri = gr.Textbox(value=default_uri, label="Neo4j URI")
            user = gr.Textbox(value=default_user, label="Neo4j User")
            pwd = gr.Textbox(value=default_pwd, label="Neo4j Password", type="password")
            lang = gr.Dropdown(["fr","en","de","it"], value="fr", label="Langue")
            mode = gr.Radio(["Full-text","Metadata"], value="Full-text", label="Mode de recherche")
            model = gr.Textbox(value=default_model, label="OpenAI Model")
            topk = gr.Slider(1, 20, 8, step=1, label="Top-K preuves")
            ev_chars = gr.Slider(200, 3000, 800, step=50, label="Taille max extrait / article")
            show_debug = gr.Checkbox(False, label="Afficher le Cypher/diagnostic")

            gr.Markdown("### 📄 Prévisualisation PDF")
            pdf_http = gr.Checkbox(True, label="Servir les PDF via HTTP (recommandé)")
            pdf_root = gr.Textbox(value=str(Path("./pdfs").resolve()), label="Répertoire racine des PDF (servi en HTTP)")
            pdf_port = gr.Number(value=7863, precision=0, label="Port HTTP pour les PDF")

            gr.Markdown(f"**OpenAI API key**: {'✅ trouvée dans l’environnement' if have_key else '❌ absente (mettez OPENAI_API_KEY)'}")

        # Layout in two columns
        with gr.Row():
            with gr.Column(scale=2):
                chat = gr.Chatbot(type="messages", height=420)
                question = gr.Textbox(placeholder="Votre question…", label="Message")
                with gr.Row():
                    ask_btn = gr.Button("Envoyer", variant="primary")
                    new_btn = gr.Button("Nouvelle conversation")
                debug = gr.Markdown(visible=False)

            with gr.Column(scale=1):
                gr.Markdown("### 📎 Sources trouvées")
                sources = gr.Dropdown(choices=[], label="Cliquez pour prévisualiser", interactive=True)
                preview = gr.Markdown(value="", elem_id="preview_md")

        # backend state (per session)
        state = gr.State({
            "history": [],         # list of {"role": "...", "content": "..."}
            "last_cypher": None,
            "last_rows": [],
            "evidence": [],
            "source_choices": [],
            "pdf_server_started": False,
            "pdf_server_info": None
        })

        def respond(msg, st, uri, user, pwd, lang, mode, model, topk, ev_chars, show_debug, pdf_http_opt, pdf_root_dir, pdf_http_port):
            # Start PDF server if requested (once per session)
            if pdf_http_opt and not st.get("pdf_server_started"):
                try:
                    _pdf_server.start(pdf_root_dir, port=int(pdf_http_port))
                    st["pdf_server_started"] = True
                    st["pdf_server_info"] = _pdf_server.info()
                except Exception:
                    st["pdf_server_started"] = False
                    st["pdf_server_info"] = None

            driver = GraphDatabase.driver(uri, auth=(user, pwd))
            rows = []
            cypher = ""
            with driver.session() as session:
                if mode == "Full-text":
                    q = build_ft_query_from_question(msg)
                    cypher = """
CALL db.index.fulltext.queryNodes('articleText', $q)
YIELD node, score
MATCH (d:Document)-[:HAS_ARTICLE]->(node)
RETURN
  d.doc_id AS doc_id,
  node.doc_id AS article_doc_id,
  node.article_number AS article_number,
  node.title AS title,
  node.page_start AS page_start,
  node.page_end AS page_end,
  score
ORDER BY score DESC, node.page_start
LIMIT 50
"""
                    rows = run_query(session, cypher, {"q": q})
                else:
                    seeds = list({t for t in re.findall(r"[a-zA-Z]{3,}", strip_accents(msg.lower()))})[:8]
                    cypher = """
WITH $seeds AS kws
MATCH (d:Document)-[:HAS_ARTICLE]->(a:Article)
WHERE any(k IN kws WHERE toLower(coalesce(a.title,'')) CONTAINS k OR toLower(coalesce(a.article_number,'')) CONTAINS k)
RETURN
  d.doc_id AS doc_id,
  a.doc_id AS article_doc_id,
  a.article_number AS article_number,
  a.title AS title,
  a.page_start AS page_start,
  a.page_end AS page_end
ORDER BY a.page_start
LIMIT 50
"""
                    rows = run_query(session, cypher, {"seeds": seeds})
                    if not rows:
                        cypher = """
WITH $seeds AS kws
MATCH (e:Event)-[:FROM_ARTICLE]->(a:Article)<-[:HAS_ARTICLE]-(d:Document)
WHERE any(k IN kws WHERE toLower(coalesce(e.snippet,'')) CONTAINS k)
RETURN
  d.doc_id AS doc_id,
  a.doc_id AS article_doc_id,
  a.article_number AS article_number,
  a.title AS title,
  a.page_start AS page_start,
  a.page_end AS page_end,
  e.type AS event_type,
  e.date AS event_date,
  e.snippet AS snippet
ORDER BY CASE WHEN event_date IS NULL THEN 1 ELSE 0 END, event_date DESC, a.page_start
LIMIT 50
"""
                        rows = run_query(session, cypher, {"seeds": seeds})
                    if not rows:
                        cypher = """
WITH $seeds AS kws
MATCH (d:Document)-[:HAS_ARTICLE]->(a:Article)
WHERE any(k IN kws WHERE toLower(coalesce(a.text_preview,'')) CONTAINS k
                      OR toLower(coalesce(a.text_preview_ascii,'')) CONTAINS k)
RETURN
  d.doc_id AS doc_id,
  a.doc_id AS article_doc_id,
  a.article_number AS article_number,
  a.title AS title,
  a.page_start AS page_start,
  a.page_end AS page_end
ORDER BY a.page_start
LIMIT 50
"""
                        rows = run_query(session, cypher, {"seeds": seeds})

                # Deduplicate identical Article results
                rows = dedupe_rows(rows)

                # build evidence and synthesize
                kw = tokenize_keywords(msg)
                info = st.get("pdf_server_info") or {}
                evidence, dbg = gather_evidence(
                    session, rows, kw,
                    k=int(topk),
                    evidence_chars=int(ev_chars),
                    pdf_http=bool(pdf_http_opt and st.get("pdf_server_started")),
                    pdf_root=(info.get("root") or str(Path(pdf_root_dir).resolve())),
                    pdf_host=(info.get("host") or "127.0.0.1"),
                    pdf_port=int(info.get("port") or int(pdf_http_port)),
                )
                client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
                answer = synthesize_answer_with_history(client, st["history"], msg, evidence, lang, model)

            # update histories
            history = st["history"] + [
                {"role":"user","content":msg},
                {"role":"assistant","content":answer}
            ]
            st["history"] = history
            st["last_cypher"] = cypher
            st["last_rows"] = rows

            # populate sources + default preview (use TITLE in dropdown)
            choices = []
            for i, e in enumerate(evidence):
                display_title = _display_title(e, max_len=110)
                choices.append(f"{i+1}. {display_title} | {e['article_number']} | p{e['page_start']}-{e['page_end']}")

            st["evidence"] = evidence
            st["source_choices"] = choices
            preview_md = render_preview_md(evidence[0]) if evidence else ""

            # debug panel
            dbg_md = ""
            if show_debug:
                dbg_md = "### Cypher\n```\n" + cypher.strip() + "\n```\n"
                dbg_md += f"**Rows**: {len(rows)}\n\n"
                if evidence:
                    dbg_md += "### Evidence\n" + dbg.replace("\n", "  \n")

            return (
                history,
                st,
                gr.update(choices=choices, value=(choices[0] if choices else None)),
                gr.update(value=preview_md),
                gr.update(visible=show_debug, value=dbg_md),
            )

        def on_select_source(choice, st):
            try:
                idx = st["source_choices"].index(choice)
            except (ValueError, KeyError):
                return gr.update(value="")
            ev = st.get("evidence", [])
            e = ev[idx] if 0 <= idx < len(ev) else None
            return gr.update(value=render_preview_md(e) if e else "")

        def on_new(st):
            st["history"] = []
            st["last_cypher"] = None
            st["last_rows"] = []
            st["evidence"] = []
            st["source_choices"] = []
            st["pdf_server_started"] = False
            st["pdf_server_info"] = None
            return [], st, gr.update(choices=[], value=None), gr.update(value=""), gr.update(visible=False, value="")

        # wire events
        ask_btn.click(
            respond,
            inputs=[question, state, uri, user, pwd, lang, mode, model, topk, ev_chars, show_debug, pdf_http, pdf_root, pdf_port],
            outputs=[chat, state, sources, preview, debug]
        )
        question.submit(
            respond,
            inputs=[question, state, uri, user, pwd, lang, mode, model, topk, ev_chars, show_debug, pdf_http, pdf_root, pdf_port],
            outputs=[chat, state, sources, preview, debug]
        )
        sources.change(on_select_source, inputs=[sources, state], outputs=[preview])
        new_btn.click(on_new, inputs=[state], outputs=[chat, state, sources, preview, debug])

    return demo

if __name__ == "__main__":
    app = make_app()
    app.launch(server_name="127.0.0.1", server_port=7862)
