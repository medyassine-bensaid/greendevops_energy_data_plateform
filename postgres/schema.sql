-- ============================================================
-- GreenDevOps Platform — PostgreSQL Schema
-- Run: psql -U greenops -d greenops -f schema.sql
-- ============================================================

-- ─── Extensions ──────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ─── ENUM types ──────────────────────────────────────────────
DO $$ BEGIN
  CREATE TYPE measurement_src AS ENUM ('process', 'ebpf', 'unknown');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ─── Core tables ─────────────────────────────────────────────

-- 1. Job-level energy (from master_energy_database.csv)
CREATE TABLE IF NOT EXISTS energy_jobs (
  id                BIGSERIAL PRIMARY KEY,
  date              TIMESTAMPTZ NOT NULL,
  pipeline_id       TEXT NOT NULL,
  commit_id         TEXT,
  repo_name         TEXT NOT NULL,
  project_name      TEXT,
  category          TEXT DEFAULT 'UNKNOWN',
  branch            TEXT,
  trigger           TEXT,
  job_name          TEXT NOT NULL,
  duration_s        DOUBLE PRECISION,
  cpu_j             DOUBLE PRECISION DEFAULT 0,
  ram_j             DOUBLE PRECISION DEFAULT 0,
  sd_j              DOUBLE PRECISION DEFAULT 0,
  nic_j             DOUBLE PRECISION DEFAULT 0,
  gpu_j             DOUBLE PRECISION DEFAULT 0,
  total_energy_j    DOUBLE PRECISION DEFAULT 0,
  site              TEXT NOT NULL,
  arch              TEXT DEFAULT 'x86_64',
  measurement_src   TEXT DEFAULT 'unknown',
  source_file       TEXT,
  ingested_at       TIMESTAMPTZ DEFAULT NOW(),
  is_valid          BOOLEAN DEFAULT TRUE,
  validation_flags  TEXT DEFAULT '',
  CONSTRAINT energy_jobs_unique UNIQUE (pipeline_id, job_name, site, date)
);

-- 2. Component-level granularity (from granularity/history_*.csv)
CREATE TABLE IF NOT EXISTS energy_granularity (
  id                BIGSERIAL PRIMARY KEY,
  date              TIMESTAMPTZ NOT NULL,
  pipeline_id       TEXT NOT NULL,
  commit_id         TEXT,
  repo_name         TEXT NOT NULL,
  project_name      TEXT,
  category          TEXT DEFAULT 'UNKNOWN',
  branch            TEXT,
  trigger           TEXT,
  job_name          TEXT NOT NULL,
  component         TEXT NOT NULL,  -- cpu | ram | sd | nic | gpu
  duration_s        DOUBLE PRECISION,
  avg_power_w       DOUBLE PRECISION DEFAULT 0,
  total_energy_j    DOUBLE PRECISION DEFAULT 0,
  samples           INTEGER DEFAULT 0,
  site              TEXT NOT NULL,
  arch              TEXT DEFAULT 'x86_64',
  measurement_src   TEXT DEFAULT 'unknown',
  source_file       TEXT,
  ingested_at       TIMESTAMPTZ DEFAULT NOW(),
  is_valid          BOOLEAN DEFAULT TRUE,
  validation_flags  TEXT DEFAULT '',
  CONSTRAINT energy_gran_unique UNIQUE (pipeline_id, job_name, component, site, date)
);

-- 3. Pipeline-level totals (from pipelines_total/pipeline_summary.csv)
CREATE TABLE IF NOT EXISTS energy_pipelines (
  id                         BIGSERIAL PRIMARY KEY,
  date                       TIMESTAMPTZ NOT NULL,
  pipeline_id                TEXT NOT NULL,
  commit_id                  TEXT,
  repo_name                  TEXT NOT NULL,
  project_name               TEXT,
  category                   TEXT DEFAULT 'UNKNOWN',
  branch                     TEXT,
  trigger                    TEXT,
  total_pipeline_energy_j    DOUBLE PRECISION DEFAULT 0,
  site                       TEXT NOT NULL,
  arch                       TEXT DEFAULT 'x86_64',
  measurement_src            TEXT DEFAULT 'unknown',
  source_file                TEXT,
  ingested_at                TIMESTAMPTZ DEFAULT NOW(),
  is_valid                   BOOLEAN DEFAULT TRUE,
  validation_flags           TEXT DEFAULT '',
  CONSTRAINT energy_pipelines_unique UNIQUE (pipeline_id, site)
);

-- 4. Ingestion observability log
CREATE TABLE IF NOT EXISTS ingestion_log (
  id               BIGSERIAL PRIMARY KEY,
  run_at           TIMESTAMPTZ DEFAULT NOW(),
  files_collected  BIGINT DEFAULT 0,
  rows_ingested    BIGINT DEFAULT 0,
  rows_rejected    BIGINT DEFAULT 0,
  errors           BIGINT DEFAULT 0
);

-- ─── Indexes ──────────────────────────────────────────────────

-- energy_jobs: common query patterns
CREATE INDEX IF NOT EXISTS idx_ej_date          ON energy_jobs (date DESC);
CREATE INDEX IF NOT EXISTS idx_ej_site          ON energy_jobs (site);
CREATE INDEX IF NOT EXISTS idx_ej_repo          ON energy_jobs (repo_name);
CREATE INDEX IF NOT EXISTS idx_ej_category      ON energy_jobs (category);
CREATE INDEX IF NOT EXISTS idx_ej_job           ON energy_jobs (job_name);
CREATE INDEX IF NOT EXISTS idx_ej_measurement   ON energy_jobs (measurement_src);
CREATE INDEX IF NOT EXISTS idx_ej_arch          ON energy_jobs (arch);
CREATE INDEX IF NOT EXISTS idx_ej_valid         ON energy_jobs (is_valid);
CREATE INDEX IF NOT EXISTS idx_ej_date_site     ON energy_jobs (date DESC, site);
CREATE INDEX IF NOT EXISTS idx_ej_date_repo     ON energy_jobs (date DESC, repo_name);

-- energy_granularity
CREATE INDEX IF NOT EXISTS idx_eg_date          ON energy_granularity (date DESC);
CREATE INDEX IF NOT EXISTS idx_eg_site          ON energy_granularity (site);
CREATE INDEX IF NOT EXISTS idx_eg_component     ON energy_granularity (component);
CREATE INDEX IF NOT EXISTS idx_eg_job_comp      ON energy_granularity (job_name, component);

-- energy_pipelines
CREATE INDEX IF NOT EXISTS idx_ep_date          ON energy_pipelines (date DESC);
CREATE INDEX IF NOT EXISTS idx_ep_site          ON energy_pipelines (site);
CREATE INDEX IF NOT EXISTS idx_ep_repo          ON energy_pipelines (repo_name);

-- ingestion_log
CREATE INDEX IF NOT EXISTS idx_il_run_at        ON ingestion_log (run_at DESC);

-- ─── Aggregate views (used by Grafana) ───────────────────────

-- Daily energy per site
CREATE OR REPLACE VIEW v_daily_energy_by_site AS
SELECT
  DATE_TRUNC('day', date)   AS day,
  site,
  arch,
  SUM(total_energy_j)       AS total_j,
  AVG(total_energy_j)       AS avg_j,
  COUNT(*)                  AS job_runs,
  SUM(duration_s)           AS total_duration_s
FROM energy_jobs
WHERE is_valid = TRUE
GROUP BY 1, 2, 3;

-- Energy by measurement source
CREATE OR REPLACE VIEW v_energy_by_measurement AS
SELECT
  DATE_TRUNC('day', date)  AS day,
  site,
  measurement_src,
  SUM(total_energy_j)      AS total_j,
  COUNT(*)                 AS job_runs
FROM energy_jobs
WHERE is_valid = TRUE
GROUP BY 1, 2, 3;

-- Top energy jobs
CREATE OR REPLACE VIEW v_top_jobs_energy AS
SELECT
  job_name,
  repo_name,
  site,
  category,
  SUM(total_energy_j)      AS total_j,
  AVG(total_energy_j)      AS avg_j,
  AVG(duration_s)          AS avg_duration_s,
  COUNT(*)                 AS runs
FROM energy_jobs
WHERE is_valid = TRUE
GROUP BY 1, 2, 3, 4
ORDER BY total_j DESC;

-- Component breakdown
CREATE OR REPLACE VIEW v_component_energy AS
SELECT
  DATE_TRUNC('day', date)   AS day,
  site,
  job_name,
  component,
  SUM(total_energy_j)       AS total_j,
  AVG(avg_power_w)          AS avg_power_w,
  COUNT(*)                  AS samples
FROM energy_granularity
WHERE is_valid = TRUE
GROUP BY 1, 2, 3, 4;

-- Ingestion health view
CREATE OR REPLACE VIEW v_ingestion_health AS
SELECT
  DATE_TRUNC('hour', run_at)  AS hour,
  SUM(files_collected)         AS files,
  SUM(rows_ingested)           AS ingested,
  SUM(rows_rejected)           AS rejected,
  SUM(errors)                  AS errors,
  ROUND(
    100.0 * SUM(rows_ingested)::numeric /
    NULLIF(SUM(rows_ingested) + SUM(rows_rejected), 0), 2
  )                            AS success_rate_pct
FROM ingestion_log
GROUP BY 1
ORDER BY 1 DESC;

-- Monthly comparison (for notebook / analysis)
CREATE OR REPLACE VIEW v_monthly_job_energy AS
SELECT
  DATE_TRUNC('month', date)  AS month,
  repo_name,
  project_name,
  category,
  site,
  job_name,
  measurement_src,
  SUM(total_energy_j)        AS total_j,
  AVG(total_energy_j)        AS avg_j,
  AVG(cpu_j)                 AS avg_cpu_j,
  AVG(ram_j)                 AS avg_ram_j,
  AVG(sd_j)                  AS avg_sd_j,
  AVG(nic_j)                 AS avg_nic_j,
  AVG(gpu_j)                 AS avg_gpu_j,
  AVG(duration_s)            AS avg_duration_s,
  COUNT(*)                   AS runs
FROM energy_jobs
WHERE is_valid = TRUE
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- ─── Lineage table ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_lineage (
  id            BIGSERIAL PRIMARY KEY,
  recorded_at   TIMESTAMPTZ DEFAULT NOW(),
  source_file   TEXT NOT NULL,
  site          TEXT NOT NULL,
  file_type     TEXT NOT NULL,
  rows_parsed   INTEGER DEFAULT 0,
  rows_inserted INTEGER DEFAULT 0,
  rows_invalid  INTEGER DEFAULT 0,
  checksum      TEXT,
  notes         TEXT
);

CREATE INDEX IF NOT EXISTS idx_lineage_file ON data_lineage (source_file);
CREATE INDEX IF NOT EXISTS idx_lineage_site ON data_lineage (site);
