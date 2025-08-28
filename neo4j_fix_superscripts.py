#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
neo4j_fix_superscripts.py
-------------------------
Normalise les numéros d'article OCR contenant des chiffres/renvois "collés" ou en exposant.

Exemples traités :
  - "Art.7064"   -> "Art. 70"   + footnote="64"
  - "Art. 70a³"  -> "Art. 70a"  + footnote="3"   (³ → 3)
  - "Art. IV²"   -> "Art. IV"   + footnote="2"
  - "Art.IV12"   -> "Art. IV"   + footnote="12"
  - "Art. 70a"   -> "Art. 70a"  + footnote=None
  - "art  70A 12"-> "Art. 70a"  + footnote="12"

Usage :
  python neo4j_fix_superscripts.py --uri bolt://127.0.0.1:7687 --user neo4j --password "pwd" --dry
  python neo4j_fix_superscripts.py --uri bolt://127.0.0.1:7687 --user neo4j --password "pwd" --tail_max 2
  python neo4j_fix_superscripts.py --uri bolt://127.0.0.1:7687 --user neo4j --password "pwd"

Remarques :
- On ne touche qu'à (:Article).article_number et on ajoute/écrase (:Article).article_footnote si détecté.
- Les index FT (sur text_full) ne sont pas impactés.
"""

import re
import argparse
from typing import Optional, Tuple
from neo4j import GraphDatabase

# --- Conversion des exposants Unicode vers ASCII ---
_SUP_MAP = str.maketrans({
    "⁰":"0","¹":"1","²":"2","³":"3","⁴":"4",
    "⁵":"5","⁶":"6","⁷":"7","⁸":"8","⁹":"9",
})

# --- Chiffres romains (validation) ---
_ROMAN_RE = re.compile(
    r"(?i)^(?=[IVXLCDM]{1,8}$)M{0,4}(CM|CD|D?C{0,3})"
    r"(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$"
)

def _is_roman(tok: str) -> bool:
    return bool(_ROMAN_RE.match(tok))

def _tidy_prefix(prefix: str) -> str:
    """Canonise le préfixe en 'Art.'."""
    return "Art."

def normalize_article_number(raw: str, tail_max: int = 2) -> Tuple[str, Optional[str]]:
    """
    Normalise un numéro d'article OCR/superscript.

    Paramètres
    ----------
    raw : str
        Valeur brute, ex. 'Art.7064', 'Art. 70a³', 'Art. IV2'
    tail_max : int
        Longueur max du renvoi accepté en fin (1..tail_max). Par défaut 2.

    Retour
    ------
    (article_number_normalisé, footnote_ou_None)
    """
    if not raw:
        return ("", None)

    # Normaliser espaces + exposants Unicode → ASCII
    s = re.sub(r"\s+", " ", raw.strip())
    s = s.translate(_SUP_MAP)

    # Extraire 'Art' / 'Art.' (insensible à la casse)
    m = re.match(r"(?is)^\s*(art\.?)\s*(.*)$", s)
    if not m:
        # Si le format ne commence pas par 'Art', on rend tel quel
        return (s, None)
    prefix_raw, rest = m.group(1), m.group(2)
    prefix = _tidy_prefix(prefix_raw)
    rest_compact = rest.replace(" ", "")

    # Motif pour un renvoi collé en fin (1..tail_max chiffres)
    tail_pat = rf"(?P<tail>\d{{1,{tail_max}}})?"

    # 1) Arabe avec suffixe lettre optionnel + renvoi collé optionnel
    #    Ex: 70a12 | 70a | 7 | 70
    m_ar = re.match(rf"^(?P<num>\d{{1,3}})(?P<letter>[A-Za-z])?{tail_pat}$", rest_compact)
    if m_ar:
        num = str(int(m_ar.group("num")))             # supprime les zéros en tête
        letter = (m_ar.group("letter") or "").lower() # suffixe lettre en minuscule
        tail = m_ar.group("tail")
        main = f"{num}{letter}"
        return (f"{prefix} {main}", tail)

    # 2) Romain + renvoi collé optionnel
    #    Ex: IV2 | xii | vi
    m_ro = re.match(rf"^(?P<roman>[IVXLCDMivxlcdm]{{1,8}}){tail_pat}$", rest_compact)
    if m_ro and _is_roman(m_ro.group("roman")):
        roman = m_ro.group("roman").upper()
        tail = m_ro.group("tail")
        return (f"{prefix} {roman}", tail)

    # 3) Secours : tokens séparés (ex : "70 a 12" ou "iv 2")
    toks = re.findall(r"[A-Za-z0-9]+", rest.translate(_SUP_MAP))
    if toks:
        # Arabe
        if re.fullmatch(r"\d{1,3}", toks[0]):
            num = str(int(toks[0])); letter=""; tail=None
            if len(toks) >= 2 and re.fullmatch(r"[A-Za-z]", toks[1]):
                letter = toks[1].lower()
                if len(toks) >= 3 and re.fullmatch(rf"\d{{1,{tail_max}}}", toks[2]):
                    tail = toks[2]
            elif len(toks) >= 2 and re.fullmatch(rf"\d{{1,{tail_max}}}", toks[1]):
                tail = toks[1]
            return (f"{prefix} {num}{letter}", tail)
        # Romain
        if _is_roman(toks[0]):
            roman = toks[0].upper()
            tail = toks[1] if len(toks) >= 2 and re.fullmatch(rf"\d{{1,{tail_max}}}", toks[1]) else None
            return (f"{prefix} {roman}", tail)

    # Fallback : conserver le 'rest' tel quel mais prefix canonique
    return (f"{prefix} {rest.strip()}", None)

def main():
    ap = argparse.ArgumentParser(description="Normalise les numéros d'articles (exposants/renvois collés) dans Neo4j.")
    ap.add_argument("--uri", required=True, help="bolt://host:port")
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--dry", action="store_true", help="Aperçu uniquement (aucune écriture).")
    ap.add_argument("--tail_max", type=int, default=2, help="Longueur max du renvoi en fin (par défaut 2).")
    args = ap.parse_args()

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    fixed, unchanged = 0, 0

    with driver.session() as s:
        # On traite tous les Articles qui commencent par 'Art' (peu coûteux, et simple).
        res = s.run("""
            MATCH (a:Article)
            WHERE toLower(a.article_number) STARTS WITH 'art'
            RETURN a.doc_id AS doc_id, a.article_number AS art, id(a) AS id
            ORDER BY a.doc_id, a.article_number
        """)

        for r in res:
            art: str = r["art"]
            new_art, foot = normalize_article_number(art, tail_max=args.tail_max)

            if new_art != art or foot:
                if args.dry:
                    print(f"[dry-run] {r['doc_id']} : '{art}' -> '{new_art}' (footnote={foot})")
                else:
                    s.run("""
                        MATCH (a:Article) WHERE id(a) = $id
                        SET a.article_number = $new_art,
                            a.article_footnote = $foot
                    """, {"id": r["id"], "new_art": new_art, "foot": foot})
                fixed += 1
            else:
                unchanged += 1

    driver.close()
    print(f"[fix_superscripts] fixed: {fixed} | unchanged: {unchanged}")

if __name__ == "__main__":
    main()
