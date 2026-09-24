-- Full refresh: raw, silver y gold_staging se recrean en cada run.
-- gold y ops persisten entre runs.
DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold_staging CASCADE;
CREATE SCHEMA raw;
CREATE SCHEMA silver;
CREATE SCHEMA gold_staging;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE raw.crm_users (
    crm_id text PRIMARY KEY, first_name text, last_name text, email text, phone text,
    birth_date text, address text, city text, updated_at text,
    _ingested_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE raw.billing_users (
    billing_id text PRIMARY KEY, full_name text, email text, phone text,
    billing_address text, city text, updated_at text,
    _ingested_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE silver.entities (
    entity_key text PRIMARY KEY,
    source text NOT NULL,
    source_id text NOT NULL,
    given_name text,            -- display
    family_name text,           -- display
    full_name_norm text,
    family_soundex text,
    email text,
    phone_e164 text,
    birth_date date,
    city text,
    address text,
    updated_at timestamptz
);

CREATE TABLE silver.pairs (
    pair_id text PRIMARY KEY,
    left_key text NOT NULL REFERENCES silver.entities,
    right_key text NOT NULL REFERENCES silver.entities,
    email_agree smallint NOT NULL,      -- 1 igual, 0 falta, -1 distinto
    phone_agree smallint NOT NULL,
    birth_agree smallint NOT NULL,
    city_agree smallint NOT NULL,
    name_sim real NOT NULL,
    address_sim real NOT NULL,
    birth_date_conflict boolean NOT NULL,
    batch_id text,
    p_match real,
    decision text CHECK (decision IN ('MATCH', 'NON_MATCH', 'REVIEW'))
);

-- Una fila por par y checkpoint de Laya.
CREATE TABLE silver.laya_scores (
    pair_id text NOT NULL REFERENCES silver.pairs,
    checkpoint text NOT NULL,
    p_same real CHECK (p_same BETWEEN 0 AND 1),
    PRIMARY KEY (pair_id, checkpoint)
);

-- Decisiones de todas las configuraciones (baseline y cada checkpoint), para el benchmark.
CREATE TABLE silver.pair_decisions (
    pair_id text NOT NULL REFERENCES silver.pairs,
    config text NOT NULL,
    p_match real NOT NULL,
    decision text NOT NULL,
    PRIMARY KEY (pair_id, config)
);

CREATE VIEW silver.review_queue AS
SELECT p.pair_id, p.p_match, p.birth_date_conflict,
       to_jsonb(l) AS left_record, to_jsonb(r) AS right_record
FROM silver.pairs p
JOIN silver.entities l ON l.entity_key = p.left_key
JOIN silver.entities r ON r.entity_key = p.right_key
WHERE p.decision = 'REVIEW';

CREATE TABLE gold_staging.customer_master (
    master_id uuid NOT NULL,
    given_name text, family_name text, email text, phone_e164 text,
    birth_date date, city text, address text,
    source_count int NOT NULL,
    has_pending_review boolean NOT NULL
);

CREATE TABLE gold_staging.customer_xref (
    entity_key text NOT NULL,
    master_id uuid NOT NULL
);

CREATE TABLE IF NOT EXISTS ops.ground_truth (
    entity_key text PRIMARY KEY,
    true_entity_id text NOT NULL,
    split text NOT NULL CHECK (split IN ('dev', 'test'))
);

CREATE TABLE IF NOT EXISTS ops.laya_timing (
    run_id text NOT NULL, batch_id text NOT NULL, checkpoint text NOT NULL,
    pairs int NOT NULL, load_s real NOT NULL, infer_s real NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, batch_id, checkpoint)
);

CREATE TABLE IF NOT EXISTS ops.run_metrics (
    run_id text NOT NULL, metric text NOT NULL, value double precision, dims jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
