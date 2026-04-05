-- schemas/clickstream.sql
-- Redshift DDL for clickstream streaming pipeline
-- Run once on cluster setup

-- ── Schemas ────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS public;

-- ── Staging Tables (transient, truncated each load cycle) ──────────────────────

CREATE TABLE IF NOT EXISTS staging.clickstream_events (
    event_id            VARCHAR(64)     NOT NULL,
    session_id          VARCHAR(64),
    user_id_hashed      VARCHAR(64),
    event_type          VARCHAR(32),
    page_url            VARCHAR(512),
    page_path           VARCHAR(256),
    domain              VARCHAR(128),
    referrer            VARCHAR(512),
    device_type         VARCHAR(16),
    is_mobile           BOOLEAN,
    country_code        CHAR(2),
    event_ts            TIMESTAMP,
    processed_ts        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.session_aggregates (
    session_id          VARCHAR(64)     NOT NULL,
    window_start        TIMESTAMP       NOT NULL,
    window_end          TIMESTAMP       NOT NULL,
    device_type         VARCHAR(16),
    country_code        CHAR(2),
    event_count         INTEGER,
    unique_pages        INTEGER,
    cart_adds           INTEGER,
    session_start       TIMESTAMP,
    session_end         TIMESTAMP
);

-- ── Target Tables ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.fact_clickstream (
    event_id            VARCHAR(64)     NOT NULL,
    session_id          VARCHAR(64),
    user_id_hashed      VARCHAR(64),
    event_type          VARCHAR(32),
    page_path           VARCHAR(256),
    domain              VARCHAR(128),
    device_type         VARCHAR(16),
    is_mobile           BOOLEAN,
    country_code        CHAR(2),
    event_ts            TIMESTAMP,
    processed_ts        TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (event_id)
)
DISTKEY(user_id_hashed)
SORTKEY(event_ts);

CREATE TABLE IF NOT EXISTS public.fact_session_aggregates (
    session_id          VARCHAR(64)     NOT NULL,
    window_start        TIMESTAMP       NOT NULL,
    device_type         VARCHAR(16),
    country_code        CHAR(2),
    event_count         INTEGER,
    unique_pages        INTEGER,
    cart_adds           INTEGER,
    session_duration_s  INTEGER,
    _loaded_at          TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (session_id, window_start)
)
DISTKEY(session_id)
SORTKEY(window_start);

-- ── Materialized View: Hourly Event Summary ────────────────────────────────────

CREATE MATERIALIZED VIEW public.mv_hourly_event_summary AS
SELECT
    DATE_TRUNC('hour', event_ts)    AS event_hour,
    event_type,
    device_type,
    country_code,
    COUNT(*)                         AS event_count,
    COUNT(DISTINCT session_id)       AS unique_sessions,
    COUNT(DISTINCT user_id_hashed)   AS unique_users
FROM public.fact_clickstream
GROUP BY 1, 2, 3, 4;
