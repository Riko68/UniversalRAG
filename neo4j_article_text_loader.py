#!/usr/bin/env python3
"""
neo4j_article_text_loader.py
----------------------------
Load FULL article text into Neo4j so you can search content, not just titles.

It writes the following properties on (:Article):
- text_full           : full article text (as-is)
- text_preview        : first N characters (default 4000), lowercased
- text_full_ascii     : accent-stripped version of full text, lowercased
- text_preview_ascii  : accent-stripped preview, lowercased
- text_chars          : original text length (int)

Usage (Windows PowerShell):
    python neo4j_article_text_loader.py --articles .\out\articles.jsonl ^
      --uri bolt://127.0.0.1:7687 --user neo4j --password "letmein!" --chars 8000

Notes:
- Safe to re-run: it will update properties.
- You can keep previews small if you only need lightweight keyword search.
- For large graphs, prefer full-text indexes for performance.
"""
import argparse, json, unicodedata
from pathlib import Path

from neo4j import GraphDatabase

def strip_accents(s: str) -> str:
    if not s:
        return s
    # NFKD decomposition + filter combining marks
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--articles", required=True, help="Path to out/articles.jsonl")
    ap.add_argument("--uri", required=True, help="Neo4j bolt URI, e.g., bolt://127.0.0.1:7687")
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--chars", type=int, default=4000, help="Preview length (chars)")
    args = ap.parse_args()

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))

    cypher = """
    MERGE (a:Article {doc_id:$doc_id, article_number:$article_number})
    SET a.text_full          = $text_full,
        a.text_preview       = $text_preview,
        a.text_full_ascii    = $text_full_ascii,
        a.text_preview_ascii = $text_preview_ascii,
        a.text_chars         = $text_chars
    """
    total = 0
    with driver.session() as session, open(args.articles, "r", encoding="utf-8") as fin:
        for line in fin:
            a = json.loads(line)
            full = a.get("text") or ""
            preview = (full[:args.chars]).lower()
            full_ascii = strip_accents(full).lower()
            preview_ascii = strip_accents(preview).lower()
            session.run(cypher, **{
                "doc_id": a["doc_id"],
                "article_number": a["article_number"],
                "text_full": full,
                "text_preview": preview,
                "text_full_ascii": full_ascii,
                "text_preview_ascii": preview_ascii,
                "text_chars": len(full),
            })
            total += 1
    driver.close()
    print(f"[neo4j_article_text_loader] Updated {total} Article nodes with text fields.")

if __name__ == "__main__":
    main()
