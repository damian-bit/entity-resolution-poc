import numpy as np
import pandas as pd

from er import decide, gold, judge, normalize, pairs


def test_normalize_email_phone_date_address():
    assert normalize.norm_email("Ana.Perez+billing@GMAIL.com") == "anaperez@gmail.com"
    assert normalize.norm_email("ana.perez@hotmail.com") == "ana.perez@hotmail.com"
    assert normalize.norm_email("") is None
    assert normalize.norm_phone("(011) 15-4555-1234") == "+5491145551234"
    assert normalize.norm_phone("+54 9 11 4555-1234") == "+5491145551234"
    assert normalize.norm_phone("123") is None
    assert str(normalize.norm_date("14/03/1988")) == "1988-03-14"
    assert normalize.norm_address("Av. Corrientes 1234 piso 3") == normalize.norm_address("Avenida Corrientes 1234")


def test_split_full_name():
    assert normalize.split_full_name("Pérez García, Ana") == ("Ana", "Pérez García")
    assert normalize.split_full_name("Ana Pérez") == ("Ana", "Pérez")


def test_agree_encoding():
    a = pd.Series(["x", "x", None, "y"])
    b = pd.Series(["x", "z", "x", None])
    assert pairs.agree(a, b).tolist() == [1, -1, 0, 0]


def test_extract_p_true_rejects_malformed():
    assert judge.extract_p_true({"type": "noul", "noul": 0.7}) == 0.7
    assert judge.extract_p_true({"noul": 1.5}) is None
    assert judge.extract_p_true({"noul": float("nan")}) is None
    assert judge.extract_p_true({}) is None
    assert judge.extract_p_true(None) is None


def test_record_text_has_no_ids():
    text = judge.record_text({"full_name_norm": "ana perez", "email": None, "entity_key": "crm:C1"})
    assert "crm:C1" not in text and "name=ana perez" in text and "email=;" in text


def test_route_never_auto_matches_conflicts_or_invalid():
    p = np.array([0.99, 0.99, 0.99, 0.01, 0.5])
    conflict = np.array([False, True, False, False, False])
    invalid = np.array([False, False, True, False, False])
    out = decide.route(p, conflict, invalid, tau_high=0.85, tau_low=0.15).tolist()
    assert out == ["MATCH", "REVIEW", "REVIEW", "NON_MATCH", "REVIEW"]


def test_tune_thresholds_hits_target_precision():
    rng = np.random.default_rng(0)
    y = rng.random(2000) < 0.3
    p = np.clip(np.where(y, 0.8, 0.2) + rng.normal(0, 0.15, 2000), 0, 1)
    hi, lo = decide.tune_thresholds(p, y)
    assert y[p >= hi].mean() >= 0.98
    assert (~y[p <= lo]).mean() >= 0.98
    assert lo < hi


def test_clusters_are_transitive_and_stable():
    keys = ["a", "b", "c", "d"]
    x1 = gold.clusters(keys, [("a", "b"), ("b", "c")])
    x2 = gold.clusters(list(reversed(keys)), [("c", "b"), ("b", "a")])
    assert x1["a"] == x1["b"] == x1["c"] != x1["d"]
    assert x1 == x2


def test_golden_record_takes_latest_non_null():
    ent = pd.DataFrame({
        "entity_key": ["a", "b"], "given_name": ["Ana", "Ana"], "family_name": ["Perez", "Perez Garcia"],
        "email": ["old@x.com", None], "phone_e164": [None, "+5491145551234"], "birth_date": [None, None],
        "city": ["rosario", "rosario"], "address": [None, None],
        "updated_at": pd.to_datetime(["2023-01-01", "2025-01-01"]),
    })
    xref = gold.clusters(["a", "b"], [("a", "b")])
    m = gold.golden_records(ent, xref, review_keys=set()).iloc[0]
    assert m["family_name"] == "Perez Garcia"      # más reciente
    assert m["email"] == "old@x.com"               # el más reciente es nulo
    assert m["source_count"] == 2 and not m["has_pending_review"]
