"""Blocking (SQL) y features determinísticas por par."""
import hashlib

import numpy as np
import pandas as pd
from rapidfuzz import fuzz

MAX_BLOCK_SIZE = 50
MIN_NAME_SIM = 0.6  # filtro barato para pares que solo comparten soundex+ciudad

# Un par es candidato si comparte email, teléfono o soundex(apellido)+ciudad.
# Bloques de más de MAX_BLOCK_SIZE registros se descartan (claves demasiado genéricas).
BLOCKING_SQL = f"""
WITH blk AS (
    SELECT entity_key, 'e:' || email AS k FROM silver.entities WHERE email IS NOT NULL
    UNION ALL
    SELECT entity_key, 'p:' || phone_e164 FROM silver.entities WHERE phone_e164 IS NOT NULL
    UNION ALL
    SELECT entity_key, 's:' || family_soundex || '|' || city FROM silver.entities
    WHERE family_soundex IS NOT NULL AND city IS NOT NULL
),
ok AS (SELECT k FROM blk GROUP BY k HAVING count(*) <= {MAX_BLOCK_SIZE})
SELECT DISTINCT a.entity_key AS left_key, b.entity_key AS right_key
FROM blk a
JOIN blk b ON a.k = b.k AND a.entity_key < b.entity_key
WHERE a.k IN (SELECT k FROM ok)
"""

FEATURES = ["email_agree", "phone_agree", "birth_agree", "city_agree", "name_sim", "address_sim"]


def pair_id(left: str, right: str) -> str:
    return hashlib.sha1(f"{left}|{right}".encode()).hexdigest()[:16]


def agree(a: pd.Series, b: pd.Series) -> np.ndarray:
    """1 = ambos presentes e iguales, 0 = falta alguno, -1 = ambos presentes y distintos."""
    both = a.notna() & b.notna()
    return np.where(~both, 0, np.where(a == b, 1, -1)).astype(int)


def _sim(a: pd.Series, b: pd.Series) -> list[float]:
    return [fuzz.token_set_ratio(x, y) / 100 if isinstance(x, str) and isinstance(y, str) else 0.0
            for x, y in zip(a, b)]


def build_features(candidates: pd.DataFrame, entities: pd.DataFrame) -> pd.DataFrame:
    e = entities.set_index("entity_key")
    L = e.loc[candidates["left_key"]].reset_index(drop=True)
    R = e.loc[candidates["right_key"]].reset_index(drop=True)
    out = pd.DataFrame({
        "pair_id": [pair_id(l, r) for l, r in zip(candidates["left_key"], candidates["right_key"])],
        "left_key": candidates["left_key"].values,
        "right_key": candidates["right_key"].values,
        "email_agree": agree(L["email"], R["email"]),
        "phone_agree": agree(L["phone_e164"], R["phone_e164"]),
        "birth_agree": agree(L["birth_date"], R["birth_date"]),
        "city_agree": agree(L["city"], R["city"]),
        "name_sim": _sim(L["full_name_norm"], R["full_name_norm"]),
        "address_sim": _sim(L["address"], R["address"]),
    })
    out["birth_date_conflict"] = out["birth_agree"] == -1
    strong = (out["email_agree"] == 1) | (out["phone_agree"] == 1)
    return out[strong | (out["name_sim"] >= MIN_NAME_SIM)].reset_index(drop=True)
