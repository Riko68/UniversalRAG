#!/usr/bin/env python3
import argparse, json, os
from neo4j import GraphDatabase

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True, help="Path to events.jsonl")
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
    MERGE (a:Article {doc_id:$doc_id, article_number:$article_number})
    MERGE (e:Event {event_id:$event_id})
      ON CREATE SET e.type=$type, e.date=date($date), e.case_no=$case_no, e.jurisdiction=$jurisdiction,
                    e.actors=$actors, e.snippet=$snippet, e.created_ts=timestamp()
      ON MATCH SET  e.type=$type, e.date=date($date), e.case_no=$case_no, e.jurisdiction=$jurisdiction,
                    e.actors=$actors, e.snippet=$snippet
    MERGE (e)-[r1:EVIDENCED_BY]->(d)
      ON CREATE SET r1.page_start=$page_start, r1.page_end=$page_end
      ON MATCH SET  r1.page_start=$page_start, r1.page_end=$page_end
    MERGE (e)-[:FROM_ARTICLE]->(a)
    """

    with driver.session() as session, open(args.events, "r", encoding="utf-8") as f:
        for line in f:
            ev = json.loads(line)
            session.run(cypher, **{
                "doc_id": ev["doc_id"],
                "article_number": ev["article_number"],
                "event_id": ev["event_id"],
                "type": ev.get("type"),
                "date": ev.get("date"),
                "case_no": ev.get("case_no"),
                "jurisdiction": ev.get("jurisdiction"),
                "actors": ev.get("actors", []),
                "snippet": ev.get("snippet"),
                "page_start": ev.get("source",{}).get("page_start"),
                "page_end": ev.get("source",{}).get("page_end"),
            })
            print(f"[neo4j_events_loader] Loaded {ev['event_id']}")

    driver.close()

if __name__ == "__main__":
    main()
