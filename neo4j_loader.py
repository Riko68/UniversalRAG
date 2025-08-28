#!/usr/bin/env python3
import argparse, json, os
from neo4j import GraphDatabase

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--articles", required=True, help="Path to out/articles.jsonl")
    ap.add_argument("--uri", default=None, help="bolt://host:7687")
    ap.add_argument("--user", default=None, help="Neo4j username")
    ap.add_argument("--password", default=None, help="Neo4j password")
    args = ap.parse_args()

    uri = args.uri or os.environ.get("NEO4J_URI", "bolt://127.0.0.1:7687")
    user = args.user or os.environ.get("NEO4J_USER", "neo4j")
    pwd = args.password or os.environ.get("NEO4J_PASSWORD", "neo4j")

    driver = GraphDatabase.driver(uri, auth=(user, pwd))

    cypher = """
    MERGE (d:Document {doc_id:$doc_id})
    ON CREATE SET d.first_seen_ts = timestamp()
    WITH d
    MERGE (a:Article {doc_id:$doc_id, article_number:$article_number})
    ON CREATE SET a.title=$title, a.created_ts = timestamp()
    SET a.page_start=$page_start, a.page_end=$page_end
    MERGE (d)-[r:HAS_ARTICLE]->(a)
    SET r.page_start=$page_start, r.page_end=$page_end, r.language=$language, r.jurisdiction=$jurisdiction
    """

    with driver.session() as session:
        with open(args.articles, "r", encoding="utf-8") as f:
            for line in f:
                a = json.loads(line)
                session.run(cypher, **{
                    "doc_id": a["doc_id"],
                    "article_number": a["article_number"],
                    "title": a.get("title") or a["article_number"],
                    "page_start": int(a["page_start"]),
                    "page_end": int(a["page_end"]),
                    "language": a.get("language"),
                    "jurisdiction": a.get("jurisdiction"),
                })
                print(f"[neo4j_loader] Loaded {a['doc_id']} {a['article_number']}")

    driver.close()

if __name__ == "__main__":
    main()
