#!/usr/bin/env python3
"""
neo4j_fulltext_setup.py
-----------------------
Create helpful Neo4j full-text indexes for Articles and Events.

Usage:
    python neo4j_fulltext_setup.py --uri bolt://127.0.0.1:7687 --user neo4j --password "letmein!"
"""
import argparse
from neo4j import GraphDatabase

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    args = ap.parse_args()

    stmts = [
        # Articles: search both original and ascii-folded fields
        """
        CREATE FULLTEXT INDEX articleText IF NOT EXISTS
        FOR (a:Article) ON EACH [a.text_full, a.text_full_ascii, a.text_preview, a.text_preview_ascii, a.title, a.article_number]
        """,
        # Events: search snippet and type
        """
        CREATE FULLTEXT INDEX eventText IF NOT EXISTS
        FOR (e:Event) ON EACH [e.snippet, e.type, e.case_no, e.jurisdiction]
        """
    ]

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    with driver.session() as session:
        for s in stmts:
            session.run(s)
            print("[neo4j_fulltext_setup] Executed:", s.strip().splitlines()[0])
    driver.close()
    print("[neo4j_fulltext_setup] Full-text indexes are ready.")

if __name__ == "__main__":
    main()
