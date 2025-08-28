#!/usr/bin/env python3
"""
event_extractor.py
------------------
Create a simple events file from article-level records.
- Uses lightweight regex heuristics to find dated LEGAL EVENTS.
- Emits events.jsonl with provenance (doc_id, article_number, page_range).
- Includes an OPTIONAL LLM hook you can enable to improve quality.

Usage:
    python event_extractor.py --articles ./out/articles.jsonl --out ./out/events.jsonl

Output event schema (per line):
{
  "event_id": "RS_173.110::Art. 5::2024-11-02::FILING::0",
  "doc_id": "RS_173.110",
  "article_number": "Art. 5",
  "type": "FILING|ORDER|JUDGMENT|HEARING|DEADLINE|MOTION|APPEAL|OTHER",
  "date": "2024-11-02",
  "actors": ["Party A", "Court ..."],   # best-effort tokens
  "case_no": "C-123/2024",              # best-effort tokens
  "jurisdiction": "CH",
  "pages": [12,13,14],
  "snippet": "…",
  "source": {"pdf_path": "...", "page_start": 12, "page_end": 14}
}
"""
import argparse, json, re
from pathlib import Path
from datetime import datetime

# Basic patterns for dates and event keywords
DATE_PATTERNS = [
    r"\b(\d{4})-(\d{2})-(\d{2})\b",                    # ISO YYYY-MM-DD
    r"\b(\d{2})/(\d{2})/(\d{4})\b",                    # DD/MM/YYYY
    r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b",              # DD.MM.YYYY
    r"\b(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})\b",
    r"\b(\d{1,2})\s+(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})\b",
]

EVENT_TYPES = {
    "FILING": [r"dép[oô]t", r"filed", r"submitted", r"requête", r"requ[ée]te", r"saisine"],
    "ORDER": [r"ordonnance", r"order(ed)?", r"arr[eé]t"],
    "JUDGMENT": [r"jugement", r"judgment", r"arr[eé]t"],
    "HEARING": [r"audience", r"hearing"],
    "DEADLINE": [r"deadline", r"[ée]ch[eé]ance", r"delai", r"délai"],
    "MOTION": [r"motion", r"requ[ée]te", r"demande"],
    "APPEAL": [r"appel", r"appeal"],
}

CASE_NO_PAT = re.compile(r"\b([A-Z]?-?\d{1,3}[/-]\d{2,4})\b")
ACTOR_PAT = re.compile(r"\b(tribunal|cour|minist[eè]re|procureur|plaignant|d[eé]fendeur|defendant|plaintiff)\b", re.I)

def norm_date(s: str) -> str | None:
    s = s.strip()
    fmts = ["%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y", "%d %b %Y", "%d %B %Y"]
    for pat, fmt in zip(DATE_PATTERNS, fmts):
        m = re.search(pat, s, re.I)
        if m:
            try:
                if fmt == "%d %B %Y" or fmt == "%d %b %Y":
                    # Map FR month names to en for parsing, quick hack
                    fr_map = {
                        "janvier":"January","février":"February","mars":"March","avril":"April","mai":"May",
                        "juin":"June","juillet":"July","août":"August","septembre":"September","octobre":"October",
                        "novembre":"November","décembre":"December"
                    }
                    ds = m.group(0)
                    for k,v in fr_map.items():
                        ds = re.sub(k, v, ds, flags=re.I)
                    dt = datetime.strptime(ds, "%d %B %Y")
                else:
                    dt = datetime.strptime(m.group(0), fmt)
                return dt.date().isoformat()
            except Exception:
                continue
    return None

def guess_event_type(text: str) -> str:
    for t, pats in EVENT_TYPES.items():
        for p in pats:
            if re.search(p, text, re.I):
                return t
    return "OTHER"

def extract_events_from_article(a: dict) -> list[dict]:
    events = []
    text = a["text"]
    # naive split into sentences
    sents = re.split(r"(?<=[\.\!\?])\s+", text)
    for i, s in enumerate(sents):
        d = norm_date(s)
        if not d:
            continue
        etype = guess_event_type(s)
        case = CASE_NO_PAT.search(s)
        actors = list(set([m.group(0) for m in ACTOR_PAT.finditer(s)]))
        ev = {
            "doc_id": a["doc_id"],
            "article_number": a["article_number"],
            "type": etype,
            "date": d,
            "actors": actors or [],
            "case_no": case.group(1) if case else None,
            "jurisdiction": a.get("jurisdiction"),
            "pages": list(range(int(a["page_start"]), int(a["page_end"])+1)),
            "snippet": s.strip(),
            "source": {
                "pdf_path": a.get("pdf_path"),
                "page_start": int(a["page_start"]),
                "page_end": int(a["page_end"]),
            }
        }
        ev["event_id"] = f"{ev['doc_id']}::{ev['article_number']}::{ev['date']}::{ev['type']}::{i}"
        events.append(ev)
    return events

# OPTIONAL: LLM hook (pseudo, user can wire their own model)
def llm_refine(events: list[dict], article_text: str) -> list[dict]:
    """
    Placeholder: take heuristic events and refine/merge with an LLM.
    Implement your own call here (OpenAI, local model, etc.).
    """
    return events

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--articles", required=True, help="Path to out/articles.jsonl")
    ap.add_argument("--out", required=True, help="events.jsonl path to write")
    args = ap.parse_args()

    outp = Path(args.out)
    count = 0
    with open(args.articles, "r", encoding="utf-8") as fin, outp.open("w", encoding="utf-8") as fout:
        for line in fin:
            a = json.loads(line)
            evs = extract_events_from_article(a)
            evs = llm_refine(evs, a["text"])
            for ev in evs:
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                count += 1
    print(f"[event_extractor] wrote {count} events to {outp}")

if __name__ == "__main__":
    main()
