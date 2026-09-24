"""Silver -> Gold: agrupar MATCH en entidades, golden record y publicación atómica."""
import uuid

import pandas as pd

NAMESPACE = uuid.UUID("6f1c2d8e-5b1a-4c3e-9a57-3d2f0e8b7c41")
FIELDS = ["given_name", "family_name", "email", "phone_e164", "birth_date", "city", "address"]


class UnionFind:
    def __init__(self, items):
        self.parent = {i: i for i in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


def clusters(entity_keys, match_edges) -> dict[str, str]:
    """entity_key -> master_id. Registros sin match forman un grupo de uno."""
    uf = UnionFind(entity_keys)
    for a, b in match_edges:
        uf.union(a, b)
    groups: dict[str, list[str]] = {}
    for k in entity_keys:
        groups.setdefault(uf.find(k), []).append(k)
    return {k: str(uuid.uuid5(NAMESPACE, min(members)))
            for members in groups.values() for k in members}


def golden_records(entities: pd.DataFrame, xref: dict[str, str], review_keys: set[str]) -> pd.DataFrame:
    """Supervivencia: por campo, el valor no nulo más reciente."""
    df = entities.assign(master_id=entities["entity_key"].map(xref))
    df = df.sort_values(["master_id", "updated_at", "entity_key"], ascending=[True, False, True])
    master = df.groupby("master_id", sort=True)[FIELDS].first()  # first() = primer no nulo
    master["source_count"] = df.groupby("master_id").size()
    master["has_pending_review"] = df.assign(r=df["entity_key"].isin(review_keys)).groupby("master_id")["r"].any()
    return master.reset_index()


PUBLISH_SQL = """
DROP TABLE IF EXISTS gold.customer_master;
DROP TABLE IF EXISTS gold.customer_xref;
ALTER TABLE gold_staging.customer_master SET SCHEMA gold;
ALTER TABLE gold_staging.customer_xref SET SCHEMA gold;
"""

GOLD_HASH_SQL = """
SELECT md5(
  (SELECT string_agg(t::text, '|' ORDER BY master_id) FROM gold.customer_master t) ||
  (SELECT string_agg(x::text, '|' ORDER BY entity_key) FROM gold.customer_xref x))
"""
