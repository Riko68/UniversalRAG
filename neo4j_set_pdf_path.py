# neo4j_set_pdf_path.py
#!/usr/bin/env python3
import argparse, json
from neo4j import GraphDatabase

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--articles", required=True)
    ap.add_argument("--uri", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    args = ap.parse_args()

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    up = """
    MATCH (a:Article {doc_id:$doc_id, article_number:$article_number})
    SET a.pdf_path = $pdf_path
    """
    n = 0
    with driver.session() as s, open(args.articles, "r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            pdf_path = rec.get("pdf_path")
            if not pdf_path:
                continue
            s.run(up, {
                "doc_id": rec["doc_id"],
                "article_number": rec["article_number"],
                "pdf_path": pdf_path
            })
            n += 1
    driver.close()
    print(f"[neo4j_set_pdf_path] updated pdf_path on {n} Article nodes")

if __name__ == "__main__":
    main()
