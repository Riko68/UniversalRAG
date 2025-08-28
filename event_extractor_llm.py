#!/usr/bin/env python3
"""
event_extractor_llm.py
----------------------
LLM-backed event extractor for legal articles.

- Reads article units from articles.jsonl (from pdf2articles.py)
- Calls OpenAI to extract EVENTS with strong constraints
- Emits events_llm.jsonl with the SAME schema as heuristic extractor
- Supports --verbose to print per-article progress and model outputs (redacted)

Usage:
  set OPENAI_API_KEY=...
  python event_extractor_llm.py --articles ./out/articles.jsonl --out ./out/events_llm.jsonl --model gpt-4o-mini --verbose
"""

import argparse, json, os, sys, time, hashlib
from pathlib import Path

# OpenAI client (>=1.0)
try:
    from openai import OpenAI
except Exception as e:
    OpenAI = None

SYSTEM_PROMPT = """Tu es un extracteur d'événements juridiques précis et conservateur.
Tu dois retourner une liste JSON d'objets "événement".
Chaque événement doit être explicitement mentionné dans le texte fourni, avec une date claire.
FORMAT STRICT (JSON valide uniquement, pas de texte autour):
[
  {
    "type": "FILING|ORDER|JUDGMENT|HEARING|DEADLINE|MOTION|APPEAL|OTHER",
    "date": "YYYY-MM-DD or null",
    "actors": ["..."],
    "case_no": "string or null",
    "snippet": "citation courte du passage",
    "notes": "facultatif"
  },
  ...
]
Règles:
- Pas d'invention de dates ni de parties.
- S'il y a ambiguïté, mets "date": null et explique dans "notes".
- Découpe en plusieurs événements si plusieurs dates/actions distinctes apparaissent.
- Utilise le français pour "notes" et "type" (mais respecte les étiquettes ci-dessus pour type).
"""

USER_PROMPT_TEMPLATE = """Métadonnées (ne pas inventer):
- doc_id: {doc_id}
- article_number: {article_number}
- page_range: {page_start}-{page_end}

Texte d'article:
\"\"\"
{article_text}
\"\"\"

Retourne UNIQUEMENT la liste JSON d'événements (pas de prose hors JSON).
"""

def stable_event_id(doc_id, article_number, date, ev_type, snippet, idx):
    h = hashlib.md5((doc_id + '|' + article_number + '|' + str(date) + '|' + ev_type + '|' + (snippet or '')).encode('utf-8')).hexdigest()[:10]
    return f"{doc_id}::{article_number}::{date}::{ev_type}::{idx}-{h}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--articles", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")
    ap.add_argument("--max_chars", type=int, default=8000, help="Truncate article text to this many chars per call")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    if OpenAI is None:
        print("Please `pip install openai>=1.0.0`", file=sys.stderr)
        sys.exit(1)
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("OPENAI_API_KEY not set", file=sys.stderr); sys.exit(1)

    client = OpenAI(api_key=api_key)

    in_path = Path(args.articles)
    out_path = Path(args.out)
    out_tmp = out_path.with_suffix(".tmp")
    total_events = 0
    total_articles = 0

    with in_path.open("r", encoding="utf-8") as fin, out_tmp.open("w", encoding="utf-8") as fout:
        for line in fin:
            a = json.loads(line)
            total_articles += 1
            text = (a.get("text") or "")[:args.max_chars]
            if args.verbose:
                print(f"\n[LLM] {a['doc_id']} {a['article_number']} pages {a['page_start']}-{a['page_end']} (chars={len(text)})")

            user_prompt = USER_PROMPT_TEMPLATE.format(
                doc_id=a["doc_id"],
                article_number=a["article_number"],
                page_start=a["page_start"],
                page_end=a["page_end"],
                article_text=text
            )

            # backoff
            for attempt in range(5):
                try:
                    resp = client.chat.completions.create(
                        model=args.model,
                        messages=[
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": user_prompt}
                        ],
                        temperature=0.1,
                        response_format={"type": "json_object"}  # ensure json, we will accept {"events":[...]} too
                    )
                    content = resp.choices[0].message.content.strip()
                    break
                except Exception as e:
                    wait = 1.5 * (attempt + 1)
                    if args.verbose:
                        print(f"  [warn] OpenAI error: {e} -> retry in {wait:.1f}s")
                    time.sleep(wait)
            else:
                if args.verbose:
                    print("  [error] giving up after retries")
                continue

            # Normalize JSON output: either a list or object with 'events'
            try:
                parsed = json.loads(content)
                if isinstance(parsed, dict) and "events" in parsed:
                    events = parsed["events"]
                elif isinstance(parsed, list):
                    events = parsed
                else:
                    events = []
            except Exception as e:
                if args.verbose:
                    print("  [warn] JSON parse failed; content was:\n", content[:4000])
                events = []

            if args.verbose:
                print(f"  -> extracted {len(events)} event(s)")

            for i, ev in enumerate(events):
                date = ev.get("date")
                ev_type = ev.get("type") or "OTHER"
                snippet = ev.get("snippet") or ""
                event_id = stable_event_id(a["doc_id"], a["article_number"], date, ev_type, snippet, i)
                out = {
                    "event_id": event_id,
                    "doc_id": a["doc_id"],
                    "article_number": a["article_number"],
                    "type": ev_type,
                    "date": date,
                    "actors": ev.get("actors") or [],
                    "case_no": ev.get("case_no"),
                    "jurisdiction": a.get("jurisdiction"),
                    "pages": list(range(int(a["page_start"]), int(a["page_end"]) + 1)),
                    "snippet": snippet,
                    "notes": ev.get("notes"),
                    "source": {
                        "pdf_path": a.get("pdf_path"),
                        "page_start": int(a["page_start"]),
                        "page_end": int(a["page_end"]),
                    }
                }
                fout.write(json.dumps(out, ensure_ascii=False) + "\n")
                total_events += 1

    out_tmp.replace(out_path)
    print(f"[event_extractor_llm] wrote {total_events} events from {total_articles} articles to {out_path}")

if __name__ == "__main__":
    main()
