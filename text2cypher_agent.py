#!/usr/bin/env python3
"""
text2cypher_agent.py
--------------------
Ask a natural-language question and retrieve answers from Neo4j.

Two modes:
- Default (metadata mode): search Article titles/numbers and Event snippets.
- Full-text mode: --fulltext uses Neo4j full-text index 'articleText' over Article text_* fields.

Usage (metadata mode):
  python text2cypher_agent.py --uri bolt://127.0.0.1:7687 --user neo4j --password "pwd" \
    --question "Quelles sont les règles de navigation sur les lacs?" --language fr --verbose

Usage (full-text mode):
  python text2cypher_agent.py --uri bolt://127.0.0.1:7687 --user neo4j --password "pwd" \
    --question "Quelles sont les règles de navigation sur les lacs?" --fulltext --verbose

Note: Full-text mode expects a FT index named 'articleText' over Article.text_full/text_full_ascii or previews.
"""
import argparse, re, unicodedata, sys
from neo4j import GraphDatabase


# --- imports near top ---
import re, textwrap
from neo4j import GraphDatabase
from openai import OpenAI

# add args:
# --k 8 (top-k rows to use)
# --evidence_chars 800 (per article)
# --cite_per_sentence (bool default True)

def pick_sentences(text, keywords, max_chars=800):
    # very simple sentence splitter
    sents = re.split(r'(?<=[\.\!\?])\s+', text.strip())
    # score by keyword presence
    def score(s):
        t = s.lower()
        return sum(k in t for k in keywords)
    ranked = sorted(sents, key=score, reverse=True)
    buf, total = [], 0
    for s in ranked:
        if score(s) == 0:  # only keep sentences with a keyword
            continue
        if total + len(s) > max_chars:
            break
        buf.append(s.strip())
        total += len(s)
    if not buf:
        # fallback: first sentences
        for s in sents[:3]:
            if total + len(s) > max_chars: break
            buf.append(s.strip()); total += len(s)
    return " ".join(buf)

def gather_evidence(rows, driver, keywords, k=8, evidence_chars=800):
    """rows = list of dicts from your main/fallback query"""
    use = rows[:k]
    ev = []
    with driver.session() as session:
        for r in use:
            doc_id = r.get("doc_id") or r.get("d.doc_id") or r.get("node.doc_id")
            art = r.get("article_number") or r.get("a.article_number") or r.get("node.article_number")
            title = r.get("title") or r.get("a.title") or r.get("node.title")
            ps = r.get("page_start") or r.get("a.page_start") or r.get("node.page_start")
            pe = r.get("page_end") or r.get("a.page_end") or r.get("node.page_end")
            # pull full text (if loaded by neo4j_article_text_loader.py)
            rec = session.run(
                "MATCH (a:Article {doc_id:$doc_id, article_number:$art}) RETURN a.text_full AS t, a.pdf_path AS pdf",
                {"doc_id": doc_id, "art": art}
            ).single()
            full = (rec["t"] or "") if rec else ""
            pdf_path = rec["pdf"] if rec else None
            snippet = pick_sentences(full, [k.lower() for k in keywords], evidence_chars)
            citation = f"[{doc_id}, {art}, p{ps}–{pe}]"
            link = f"{pdf_path}#page={ps}" if pdf_path else None
            ev.append({
                "doc_id": doc_id, "article_number": art, "title": title,
                "page_start": ps, "page_end": pe, "snippet": snippet,
                "citation": citation, "pdf": link
            })
    return ev



def synthesize_answer(
    question: str,
    evidence: list[dict],
    language: str = "fr",
    model: str = "gpt-4o-mini",
    verbose: bool = False,
) -> str:
    """
    Build a grounded French answer from evidence.
    - Every assertive sentence must end with a citation like: [doc_id, Art. X, pA–B]
    - If a PDF link is available, it may follow in parentheses.

    evidence: list of dicts with keys:
      - doc_id, article_number, page_start, page_end, title (opt), snippet, pdf (opt), citation
    """
    import os
    from openai import OpenAI

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return ("[synthesis skipped] OPENAI_API_KEY is not set. "
                "Set it and rerun, or use --no_synthesize to suppress this notice.")

    client = OpenAI(api_key=api_key)

    sys_prompt = (
        "ROLE: Juriste assistant, réponses factuelles et sourcées.\n"
        "BUT: Répondre à la question en français, uniquement avec des informations "
        "provenant des PREUVES fournies (articles et extraits).\n"
        "REGLES:\n"
        "1) Chaque phrase assertive DOIT finir par une citation [doc_id, Art. X, pA–B].\n"
        "2) N'invente rien; si l'info manque, dis-le explicitement.\n"
        "3) Regrouper/dédupliquer, style clair et concis.\n"
        "4) Conserver la terminologie juridique telle quelle.\n"
        "5) Si un lien PDF est fourni, tu peux l'ajouter entre parenthèses juste après la citation.\n"
        "SORTIE: Une liste à puces de points, puis un court paragraphe de synthèse également sourcé.\n"
    )

    # Compact evidence into a readable bundle for the model
    ev_lines = []
    for e in evidence or []:
        line = (
            f"- {e.get('doc_id','?')} | {e.get('article_number','?')} | "
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

    if verbose:
        print("\n[llm] calling OpenAI for synthesis...")

    resp = client.chat.completions.create(
        model=model or "gpt-4o-mini",
        temperature=0.2,
        messages=[
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    draft = resp.choices[0].message.content.strip()
    return enforce_citations(draft)


# Optional: provide a spelling alias if your call site uses `synthetize_answer`
synthetize_answer = synthesize_answer


def strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))

def build_ft_query_from_question(q: str) -> str:
    # extract simple keywords; keep words with letters, len>=3
    q_ascii = strip_accents(q.lower())
    toks = re.findall(r"[a-zA-Z]{3,}", q_ascii)
    if not toks:
        return q_ascii or "navigation"
    # build lucene OR with some fuzziness
    parts = []
    for t in toks[:8]:
        if len(t) >= 6:
            parts.append(f'{t}~2')
        elif len(t) >= 4:
            parts.append(f'{t}*')
        else:
            parts.append(t)
    return " OR ".join(parts)

def run_query(session, cypher: str, params=None, verbose=False):
    if verbose:
        print("\n=== Cypher ===")
        print(cypher)
        if params:
            print("Params:", params)
    try:
        res = session.run(cypher, params or {})
        rows = [r.data() for r in res]
        if verbose:
            print("\n=== Rows ===")
            print(len(rows))
        return rows, None
    except Exception as e:
        if verbose:
            print("[error]", type(e).__name__, e)
        return [], e


""" def enforce_citations(text):
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    ok = []
    for l in lines:
        if l.startswith("- ") and not re.search(r"\[[^\]]+\]\.?$", l):
            l += " [source manquante]"
        ok.append(l)
    return "\n".join(ok) """

def enforce_citations(text: str) -> str:
    """
    Ensure each bullet line ends with a [...] citation; mark if missing.
    Keeps other lines untouched.
    """
    import re
    lines = [l.rstrip() for l in text.splitlines()]
    fixed = []
    for l in lines:
        if l.strip().startswith("- "):
            if not re.search(r"\[[^\]]+\]\.?(\s*\(.+\))?$", l):
                l += " [source manquante]"
        fixed.append(l)
    return "\n".join(fixed)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--question", required=True)
    ap.add_argument("--language", default="fr")
    ap.add_argument("--model", default=None, help="(unused in this version)")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--fulltext", action="store_true", help="Use Neo4j full-text index 'articleText'")
    ap.add_argument("--fulltext_query", default=None, help="Override the Lucene query string")
    args = ap.parse_args()

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    with driver.session() as session:
        if args.fulltext:
            q = args.fulltext_query or build_ft_query_from_question(args.question)
            cypher = """
CALL db.index.fulltext.queryNodes('articleText', $q)
YIELD node, score
MATCH (d:Document)-[:HAS_ARTICLE]->(node)
RETURN d.doc_id AS doc_id, node.article_number AS article_number, node.title AS title,
       node.page_start AS page_start, node.page_end AS page_end, score
ORDER BY score DESC, node.page_start
LIMIT 50
"""
            rows, err = run_query(session, cypher, {"q": q}, args.verbose)
            if err:
                print("\n[hint] Ensure the 'articleText' full-text index exists and Article nodes have text fields.")
                print("Create index example:\n  CREATE FULLTEXT INDEX articleText IF NOT EXISTS FOR (a:Article) ON EACH [a.text_full, a.text_full_ascii, a.text_preview, a.text_preview_ascii];")
                sys.exit(1)
        else:
            # Metadata search #1: titles + article numbers (accent-insensitive)
            kws = strip_accents(args.question.lower())
            # crude keyword seeds
            seeds = list({t for t in re.findall(r"[a-zA-Z]{3,}", kws)})[:8]
            cypher1 = """
WITH $seeds AS kws
MATCH (d:Document)-[:HAS_ARTICLE]->(a:Article)
WHERE any(k IN kws WHERE toLower(coalesce(a.title,'')) CONTAINS k OR toLower(coalesce(a.article_number,'')) CONTAINS k)
RETURN d.doc_id AS doc_id, a.article_number AS article_number, a.title AS title, a.page_start AS page_start, a.page_end AS page_end
ORDER BY a.page_start
LIMIT 50
"""
            rows, err = run_query(session, cypher1, {"seeds": seeds}, args.verbose)
            if not rows:
                # Metadata search #2: event snippets (accent-insensitive) with safe ORDER BY
                cypher2 = """
WITH $seeds AS kws
MATCH (e:Event)-[:FROM_ARTICLE]->(a:Article)<-[:HAS_ARTICLE]-(d:Document)
WHERE any(k IN kws WHERE toLower(coalesce(e.snippet,'')) CONTAINS k)
RETURN d.doc_id AS doc_id, a.article_number AS article_number, a.title AS title,
       a.page_start AS page_start, a.page_end AS page_end, e.type AS event_type, e.date AS event_date, e.snippet AS snippet
ORDER BY CASE WHEN event_date IS NULL THEN 1 ELSE 0 END, event_date DESC, a.page_start
LIMIT 50
"""
                rows, err = run_query(session, cypher2, {"seeds": seeds}, args.verbose)
            if not rows:
                # Metadata search #3: article text previews if present
                cypher3 = """
WITH $seeds AS kws
MATCH (d:Document)-[:HAS_ARTICLE]->(a:Article)
WHERE any(k IN kws WHERE toLower(coalesce(a.text_preview,'')) CONTAINS k
                      OR toLower(coalesce(a.text_preview_ascii,'')) CONTAINS k)
RETURN d.doc_id AS doc_id, a.article_number AS article_number, a.title AS title, a.page_start AS page_start, a.page_end AS page_end
ORDER BY a.page_start
LIMIT 50
"""
                rows, err = run_query(session, cypher3, {"seeds": seeds}, args.verbose)

        # Print final answer
        if not rows:
            print("\n=== Answer ===")
            if args.language.startswith("fr"):
                print("Aucune entrée correspondante n'a été trouvée dans le graphe pour cette question.")
            else:
                print("No matching entries were found in the graph for this question.")
        else:
            print("\n=== Answer (top results) ===")
            # Compact print
            for r in rows[:10]:
                doc_id = r.get("doc_id")
                art = r.get("article_number")
                title = r.get("title")
                p1 = r.get("page_start")
                p2 = r.get("page_end")
                score = r.get("score")
                extra = f" | score={score:.3f}" if score is not None else ""
                print(f"- {doc_id} | {art} | p{p1}-{p2} | {title}{extra}")
            
        # Build keyword list from the question
        kw = [w for w in re.findall(r"\w+", args.question.lower()) if len(w) > 2]
        driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
        evidence = gather_evidence(rows, driver, kw, k=getattr(args, "k", 8), evidence_chars=getattr(args, "evidence_chars", 800))
        answer = synthesize_answer(args.question, evidence, language=args.language, model=args.model)
        print("\n=== Answer (grounded) ===\n" + enforce_citations(answer))

    driver.close()

if __name__ == "__main__":
    main()
