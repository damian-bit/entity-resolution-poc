"""PoC de resolución de identidades: CRM + Billing -> Gold con master_id.

ingest -> normalize -> blocking/features -> Laya (mapped + pool) -> decide
       -> gold_staging -> quality checks -> publish -> report
"""
import json
import os
import time
from pathlib import Path

import pandas as pd
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator, SQLTableCheckOperator
from airflow.sdk import Param, dag, get_current_context, task

from er import db, decide, gold, judge, normalize, pairs, report, synthetic

DATA_DIR = os.environ.get("ER_DATA_DIR", "/opt/airflow/data")
REPORTS_DIR = os.environ.get("ER_REPORTS_DIR", "/opt/airflow/reports")
SQL_DIR = Path("/opt/airflow/sql")
CONN = "er_warehouse"


def _primary_config(params) -> str:
    return "rules" if params["primary"] == "baseline" else f"rules+laya_{params['primary']}"


def _labeled_pairs() -> pd.DataFrame:
    return db.read_df("""
        SELECT p.*, gl.split, gl.true_entity_id = gr.true_entity_id AS is_match
        FROM silver.pairs p
        JOIN ops.ground_truth gl ON gl.entity_key = p.left_key
        JOIN ops.ground_truth gr ON gr.entity_key = p.right_key""")


def _run_id() -> str:
    return get_current_context()["run_id"]


@dag(
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["entity-resolution", "poc"],
    params={
        # Config que alimenta Gold: un checkpoint de Laya o "baseline" (solo reglas).
        "primary": Param("english", enum=[*judge.CHECKPOINTS, "baseline"]),
        # Checkpoints a correr. multilingual quedó descartado en el benchmark (AUC ≈ 0.49).
        "checkpoints": Param(["english"], type="array"),
        "batch_size": Param(250, type="integer", minimum=10),
        "n_people": Param(1000, type="integer"),
        "seed": Param(42, type="integer"),
        "hw_cost_usd_per_hour": Param(0.30, type="number"),
        "inject_duplicate": Param(False, type="boolean",
                                  description="Duplica un master en staging para demostrar que el gate falla"),
    },
)
def entity_resolution():

    @task
    def init_schema():
        db.execute((SQL_DIR / "01_schema.sql").read_text())

    @task
    def generate_sources(params=None):
        return synthetic.generate(DATA_DIR, n_people=params["n_people"], seed=params["seed"])

    @task
    def ingest_bronze():
        read = lambda name: pd.read_csv(f"{DATA_DIR}/{name}.csv", dtype=str, keep_default_na=False)
        db.insert_df("raw.crm_users", read("crm_users"))
        db.insert_df("raw.billing_users", read("billing_users"))
        db.execute("TRUNCATE ops.ground_truth")
        db.insert_df("ops.ground_truth", read("ground_truth"))

    @task
    def normalize_silver() -> int:
        crm = normalize.crm_to_entities(db.read_df("SELECT * FROM raw.crm_users"))
        billing = normalize.billing_to_entities(db.read_df("SELECT * FROM raw.billing_users"))
        entities = pd.concat([crm, billing], ignore_index=True)
        db.insert_df("silver.entities", entities)
        return len(entities)

    @task
    def build_pairs() -> int:
        candidates = db.read_df(pairs.BLOCKING_SQL)
        entities = db.read_df("SELECT * FROM silver.entities")
        db.insert_df("silver.pairs", pairs.build_features(candidates, entities))
        return len(candidates)

    @task
    def plan_batches(params=None) -> list[str]:
        """Solo los pares que las reglas dejaron en REVIEW van a Laya."""
        db.execute("""
            UPDATE silver.pairs p SET batch_id = b.batch_id
            FROM (SELECT pair_id,
                         'b' || lpad(((row_number() OVER (ORDER BY pair_id) - 1) / %s)::text, 3, '0') AS batch_id
                  FROM silver.pairs WHERE decision = 'REVIEW') b
            WHERE p.pair_id = b.pair_id""", (params["batch_size"],))
        return db.read_df("SELECT DISTINCT batch_id FROM silver.pairs WHERE batch_id IS NOT NULL ORDER BY 1")[
            "batch_id"].tolist()

    @task
    def get_checkpoints(params=None) -> list[str]:
        return params["checkpoints"]

    @task(pool="laya_pool", retries=2, retry_exponential_backoff=True)
    def judge_batch(batch_id: str, checkpoint: str) -> int:
        cols = ", ".join(f"{side}.{f} AS {side}_{f}" for side in ("l", "r") for f in judge.STATE_FIELDS)
        df = db.read_df(f"""
            SELECT p.pair_id, {cols}
            FROM silver.pairs p
            JOIN silver.entities l ON l.entity_key = p.left_key
            JOIN silver.entities r ON r.entity_key = p.right_key
            WHERE p.batch_id = %s ORDER BY p.pair_id""", (batch_id,))
        side = lambda s: [{f: row[f"{s}_{f}"] for f in judge.STATE_FIELDS} for _, row in df.iterrows()]

        t0 = time.perf_counter()
        agent = judge.load(checkpoint)
        t1 = time.perf_counter()
        scores = judge.judge(agent, side("l"), side("r"))
        t2 = time.perf_counter()

        db.insert_df("silver.laya_scores",
                     pd.DataFrame({"pair_id": df["pair_id"], "checkpoint": checkpoint, "p_same": scores}),
                     on_conflict="ON CONFLICT (pair_id, checkpoint) DO UPDATE SET p_same = EXCLUDED.p_same")
        db.insert_df("ops.laya_timing",
                     pd.DataFrame([{"run_id": _run_id(), "batch_id": batch_id, "checkpoint": checkpoint,
                                    "pairs": len(df), "load_s": t1 - t0, "infer_s": t2 - t1}]),
                     on_conflict="ON CONFLICT (run_id, batch_id, checkpoint) DO UPDATE SET "
                                 "load_s = EXCLUDED.load_s, infer_s = EXCLUDED.infer_s")
        return len(df)

    @task
    def decide_rules() -> dict:
        """Etapa 1: solo reglas, sobre todos los pares."""
        results, th = decide.run_config(_labeled_pairs(), None)
        db.insert_df("silver.pair_decisions", results[["pair_id", "p_match", "decision"]].assign(config="rules"))
        db.execute("""
            UPDATE silver.pairs p SET p_match = d.p_match, decision = d.decision
            FROM silver.pair_decisions d WHERE d.pair_id = p.pair_id AND d.config = 'rules'""")
        return th

    @task(trigger_rule="all_done")
    def decide_laya(params=None) -> dict:
        """Etapa 2: Laya decide solo sobre la cola de REVIEW de las reglas."""
        df = _labeled_pairs()
        scores = db.read_df("SELECT pair_id, checkpoint, p_same FROM silver.laya_scores")
        gray = df[df["decision"] == "REVIEW"].copy()
        thresholds = {}
        for ckpt in params["checkpoints"]:
            col = f"p_same_{ckpt}"
            s_ck = scores[scores["checkpoint"] == ckpt].set_index("pair_id")["p_same"]
            gray[col] = gray["pair_id"].map(s_ck)
            final = df.set_index("pair_id")[["p_match", "decision"]].copy()
            if gray.loc[gray["split"] == "dev", "is_match"].nunique() == 2:
                res, thresholds[ckpt] = decide.run_config(gray, col)
                final.update(res.set_index("pair_id")[["p_match", "decision"]])
            else:
                thresholds[ckpt] = None  # sin ejemplos de ambas clases en dev: la cola queda como está
            db.insert_df("silver.pair_decisions", final.reset_index().assign(config=f"rules+laya_{ckpt}"))
        if params["primary"] in params["checkpoints"]:
            db.execute("""
                UPDATE silver.pairs p SET p_match = d.p_match, decision = d.decision
                FROM silver.pair_decisions d WHERE d.pair_id = p.pair_id AND d.config = %s""",
                       (f"rules+laya_{params['primary']}",))
        return thresholds

    @task
    def build_gold_staging(params=None) -> int:
        entities = db.read_df("SELECT * FROM silver.entities")
        edges = db.read_df("SELECT left_key, right_key FROM silver.pairs WHERE decision = 'MATCH'")
        review = db.read_df("SELECT left_key, right_key FROM silver.pairs WHERE decision = 'REVIEW'")
        xref = gold.clusters(entities["entity_key"].tolist(), edges.itertuples(index=False, name=None))
        master = gold.golden_records(entities, xref, set(review["left_key"]) | set(review["right_key"]))
        if params["inject_duplicate"]:
            master = pd.concat([master, master.head(1)], ignore_index=True)
        db.insert_df("gold_staging.customer_master", master)
        db.insert_df("gold_staging.customer_xref",
                     pd.DataFrame({"entity_key": list(xref), "master_id": list(xref.values())}))
        return len(master)

    master_checks = SQLColumnCheckOperator(
        task_id="check_master_columns",
        conn_id=CONN,
        table="gold_staging.customer_master",
        column_mapping={
            "master_id": {"null_check": {"equal_to": 0}, "unique_check": {"equal_to": 0}},
            "given_name": {"null_check": {"equal_to": 0}},
            "family_name": {"null_check": {"equal_to": 0}},
        },
    )
    master_table_checks = SQLTableCheckOperator(
        task_id="check_master_table",
        conn_id=CONN,
        table="gold_staging.customer_master",
        checks={
            "not_empty": {"check_statement": "COUNT(*) > 0"},
            "email_unique_when_resolved": {"check_statement":
                "COUNT(email) FILTER (WHERE NOT has_pending_review) = "
                "COUNT(DISTINCT email) FILTER (WHERE NOT has_pending_review)"},
        },
    )
    xref_checks = SQLTableCheckOperator(
        task_id="check_xref",
        conn_id=CONN,
        table="gold_staging.customer_xref",
        checks={
            "entity_key_unique": {"check_statement": "COUNT(DISTINCT entity_key) = COUNT(*)"},
            "every_entity_mapped": {"check_statement": "COUNT(*) = (SELECT COUNT(*) FROM silver.entities)"},
        },
    )

    @task
    def publish_gold():
        db.execute(gold.PUBLISH_SQL)  # una transacción: Gold cambia entero o no cambia

    @task
    def build_report(rules_th: dict, laya_th: dict, params=None) -> str:
        ctx_af = get_current_context()
        run_id = ctx_af["run_id"]
        labeled = db.read_df("""
            SELECT d.pair_id, d.config, d.decision, gl.split, gl.true_entity_id = gr.true_entity_id AS is_match
            FROM silver.pair_decisions d
            JOIN silver.pairs p USING (pair_id)
            JOIN ops.ground_truth gl ON gl.entity_key = p.left_key
            JOIN ops.ground_truth gr ON gr.entity_key = p.right_key""")
        true_pairs = report.true_pair_counts(db.read_df("SELECT * FROM ops.ground_truth"))
        candidates_true = int(db.scalar("""
            SELECT count(*) FROM silver.pairs p
            JOIN ops.ground_truth gl ON gl.entity_key = p.left_key
            JOIN ops.ground_truth gr ON gr.entity_key = p.right_key
            WHERE gl.true_entity_id = gr.true_entity_id"""))
        scores = db.read_df("""
            SELECT s.checkpoint, s.p_same, gl.split, gl.true_entity_id = gr.true_entity_id AS is_match
            FROM silver.laya_scores s JOIN silver.pairs p USING (pair_id)
            JOIN ops.ground_truth gl ON gl.entity_key = p.left_key
            JOIN ops.ground_truth gr ON gr.entity_key = p.right_key""")
        timing = db.read_df("SELECT * FROM ops.laya_timing WHERE run_id = %s", (run_id,))

        configs = {name: report.config_metrics(g, true_pairs.get("test", 0))
                   for name, g in labeled.groupby("config", sort=False)}
        cascade = {c: report.cascade_metrics(labeled, c, timing[timing["checkpoint"] == c])
                   for c in params["checkpoints"]}
        gold_hash = db.scalar(gold.GOLD_HASH_SQL)
        prev = db.read_df("""SELECT dims->>'hash' AS h FROM ops.run_metrics
                             WHERE metric = 'gold_hash' ORDER BY created_at DESC LIMIT 1""")

        ctx = {
            "run_id": run_id, "generated_at": report.now(), "primary": _primary_config(params),
            "hardware": f"{os.cpu_count()} vCPU",
            "run_seconds": (pd.Timestamp.now(tz="UTC") - pd.Timestamp(ctx_af["dag_run"].start_date)).total_seconds(),
            "configs": configs, "cascade": cascade,
            "thresholds": {"rules": rules_th} | {f"rules+laya_{c}": t for c, t in (laya_th or {}).items()},
            "laya": report.laya_metrics(scores, timing, params["hw_cost_usd_per_hour"]),
            "cost_per_hour": params["hw_cost_usd_per_hour"],
            "entities": int(db.scalar("SELECT count(*) FROM silver.entities")),
            "candidates": int(db.scalar("SELECT count(*) FROM silver.pairs")),
            "blocking_recall": candidates_true / sum(true_pairs.values()),
            "masters": int(db.scalar("SELECT count(*) FROM gold.customer_master")),
            "pending_masters": int(db.scalar("SELECT count(*) FROM gold.customer_master WHERE has_pending_review")),
            "gold_hash": gold_hash,
            "idempotent": "—" if prev.empty else ("sí" if prev["h"].iloc[0] == gold_hash else "no"),
        }
        db.insert_df("ops.run_metrics", pd.DataFrame(
            [{"run_id": run_id, "metric": "gold_hash", "value": None, "dims": json.dumps({"hash": gold_hash})},
             {"run_id": run_id, "metric": "summary", "value": None,
              "dims": json.dumps({"configs": configs, "cascade": cascade, "laya": ctx["laya"]}, default=float)}]))
        return report.write(ctx, REPORTS_DIR)

    schema = init_schema()
    sources = generate_sources()
    silver = normalize_silver()
    candidates = build_pairs()
    rules_th = decide_rules()
    batches = plan_batches()
    judged = judge_batch.expand(batch_id=batches, checkpoint=get_checkpoints())
    laya_th = decide_laya()
    staging = build_gold_staging()
    published = publish_gold()

    schema >> sources >> ingest_bronze() >> silver >> candidates >> rules_th >> batches
    judged >> laya_th >> staging >> [master_checks, master_table_checks, xref_checks] >> published
    published >> build_report(rules_th, laya_th)


entity_resolution()
