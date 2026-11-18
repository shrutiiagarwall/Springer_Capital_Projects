-- ddl.sql
-- Target table DDL for the email_event_extraction dataset (DHAP-34).
--
-- Database:  postgres-target  (the warehouse DB, NOT the Airflow metadata DB)
-- Table:     email_events
--
-- This DDL is executed automatically by the airflow-init service on first
-- startup via the docker-compose.yaml entrypoint.  It is also safe to run
-- manually:
--
--     docker compose exec -T postgres-target \
--         psql -U ${POSTGRES_USER} -d ${TARGET_DB_NAME} < email_event_extraction/ddl.sql
--
-- The DAG uses TRUNCATE + INSERT (not CREATE), so this DDL only needs to run
-- once per fresh environment.  The IF NOT EXISTS guard makes it idempotent.

-- ---------------------------------------------------------------------------
-- Schema (optional — use public schema for simplicity)
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- Main table
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS email_events (

    -- Surrogate primary key assigned by flatten_to_csv.py.
    -- NOT GENERATED ALWAYS AS IDENTITY because the value comes from the CSV;
    -- we want explicit control over the ID space.
    id               INTEGER        NOT NULL,

    -- Source traceability
    thread_file      TEXT           NOT NULL,
    split            TEXT           NOT NULL,   -- 'train' | 'dev' | 'test'

    -- Annotation coordinates
    turn_id          TEXT           NOT NULL,   -- 'turn_0', 'turn_1', ...
    event_type       TEXT           NOT NULL,   -- e.g. 'Request_Meeting'

    -- Trigger span
    trigger_text     TEXT,                      -- NULL when no trigger span
    trigger_indices  TEXT,                      -- e.g. '0 1 2'

    -- Additional annotation fields
    extras           TEXT,                      -- meta-semantic role string
    sentence_text    TEXT,                      -- reconstructed from token list

    -- Argument count
    num_arguments    INTEGER        NOT NULL,   -- count of non-O BIO tags

    -- Constraints
    CONSTRAINT email_events_pk PRIMARY KEY (id),
    CONSTRAINT email_events_split_check
        CHECK (split IN ('train', 'dev', 'test')),
    CONSTRAINT email_events_num_arguments_nonneg
        CHECK (num_arguments >= 0)

);

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

-- Querying by event type is the most common analytical pattern
-- (e.g. SELECT event_type, COUNT(*) FROM email_events GROUP BY event_type).
CREATE INDEX IF NOT EXISTS idx_email_events_event_type
    ON email_events (event_type);

-- Querying by split is needed for train/dev/test comparisons.
CREATE INDEX IF NOT EXISTS idx_email_events_split
    ON email_events (split);

-- Composite index for the most common join pattern (split + event_type).
CREATE INDEX IF NOT EXISTS idx_email_events_split_event_type
    ON email_events (split, event_type);

-- ---------------------------------------------------------------------------
-- Comments
-- ---------------------------------------------------------------------------

COMMENT ON TABLE email_events IS
    'Flattened MailEx (Email-Event-Extraction) dataset. '
    'Source: Srivastava et al., EMNLP 2023. '
    'Each row represents one (thread, turn, event_type, trigger) combination. '
    'Loaded by Airflow DAG load_email_event_extraction_to_postgres. '
    'DHAP-34 — Glynac AI.';

COMMENT ON COLUMN email_events.id              IS 'Surrogate PK, assigned by flatten_to_csv.py.';
COMMENT ON COLUMN email_events.thread_file     IS 'Source JSON filename, e.g. lay-k_inbox187.json.';
COMMENT ON COLUMN email_events.split           IS 'Dataset split: train, dev, or test.';
COMMENT ON COLUMN email_events.turn_id         IS 'Turn within the email thread, e.g. turn_0.';
COMMENT ON COLUMN email_events.event_type      IS 'MailEx event type, e.g. Request_Meeting.';
COMMENT ON COLUMN email_events.trigger_text    IS 'Surface string of the event trigger.';
COMMENT ON COLUMN email_events.trigger_indices IS 'Space-separated token positions of the trigger.';
COMMENT ON COLUMN email_events.extras          IS 'Meta-semantic role string from annotation.';
COMMENT ON COLUMN email_events.sentence_text   IS 'Full sentence reconstructed from token list.';
COMMENT ON COLUMN email_events.num_arguments   IS 'Count of non-O BIO argument tags in this turn/event.';
