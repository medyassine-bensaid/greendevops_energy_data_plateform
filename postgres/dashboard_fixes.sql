-- ============================================================
-- dashboard_fixes.sql
-- psql -U greenops -d greenops -f dashboard_fixes.sql
-- ============================================================

-- ── FIX 1: measurement_src NULL → 'unknown' ─────────────────
UPDATE energy_jobs        SET measurement_src = 'unknown' WHERE measurement_src IS NULL OR measurement_src = '';
UPDATE energy_granularity SET measurement_src = 'unknown' WHERE measurement_src IS NULL OR measurement_src = '';
UPDATE energy_pipelines   SET measurement_src = 'unknown' WHERE measurement_src IS NULL OR measurement_src = '';

-- ── FIX 2: arch NULL → 'x86_64' ────────────────────────────
UPDATE energy_jobs SET arch = 'x86_64' WHERE arch IS NULL OR arch = '';

-- ── FIX 3: category NULL → 'UNKNOWN' ────────────────────────
UPDATE energy_jobs SET category = 'UNKNOWN' WHERE category IS NULL OR category = '';

-- ── FIX 4: Recreate v_top_jobs_energy (add avg_duration_s) ──
CREATE OR REPLACE VIEW v_top_jobs_energy AS
SELECT
  job_name,
  repo_name,
  site,
  category,
  measurement_src,
  SUM(total_energy_j)      AS total_j,
  AVG(total_energy_j)      AS avg_j,
  AVG(duration_s)          AS avg_duration_s,
  COUNT(*)                 AS runs
FROM energy_jobs
WHERE is_valid = TRUE
GROUP BY 1, 2, 3, 4, 5
ORDER BY total_j DESC;

-- ── FIX 5: Recreate v_daily_energy_by_site ──────────────────
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

-- ── FIX 6: Recreate v_energy_by_measurement ─────────────────
CREATE OR REPLACE VIEW v_energy_by_measurement AS
SELECT
  DATE_TRUNC('day', date)  AS day,
  site,
  COALESCE(measurement_src, 'unknown') AS measurement_src,
  SUM(total_energy_j)      AS total_j,
  COUNT(*)                 AS job_runs
FROM energy_jobs
WHERE is_valid = TRUE
GROUP BY 1, 2, 3;

-- ── FIX 7: Recreate v_component_energy ──────────────────────
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

-- ── FIX 8: Recreate v_monthly_job_energy ────────────────────
CREATE OR REPLACE VIEW v_monthly_job_energy AS
SELECT
  DATE_TRUNC('month', date)  AS month,
  repo_name,
  project_name,
  category,
  site,
  job_name,
  COALESCE(measurement_src,'unknown') AS measurement_src,
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

-- ── FIX 9: Recreate v_ingestion_health ──────────────────────
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

-- ── FIX 10: Pipeline energy — set 0→NULL guard ───────────────
-- energy_pipelines may have 0 values from incomplete runs
UPDATE energy_pipelines
SET is_valid = FALSE
WHERE total_pipeline_energy_j = 0 AND is_valid = TRUE;

-- ── FIX 11: Add pipeline evolution views ─────────────────────
CREATE OR REPLACE VIEW v_pipeline_commit_history AS
SELECT
  p.date,
  p.pipeline_id,
  p.commit_id,
  p.repo_name,
  p.branch,
  p.trigger,
  p.total_pipeline_energy_j,
  p.site,
  COALESCE(p.measurement_src,'unknown') AS measurement_src,
  COUNT(j.id)                AS job_count,
  AVG(j.duration_s)          AS avg_job_duration_s,
  SUM(j.cpu_j)               AS total_cpu_j,
  SUM(j.ram_j)               AS total_ram_j,
  SUM(j.gpu_j)               AS total_gpu_j
FROM energy_pipelines p
LEFT JOIN energy_jobs j
  ON j.pipeline_id = p.pipeline_id AND j.is_valid = TRUE
WHERE p.is_valid = TRUE
GROUP BY p.date, p.pipeline_id, p.commit_id, p.repo_name,
         p.branch, p.trigger, p.total_pipeline_energy_j,
         p.site, p.measurement_src;

CREATE OR REPLACE VIEW v_commit_energy_delta AS
WITH runs AS (
  SELECT
    commit_id, repo_name, branch, date,
    total_pipeline_energy_j,
    LAG(total_pipeline_energy_j)
      OVER (PARTITION BY repo_name, branch ORDER BY date) AS prev_j
  FROM energy_pipelines WHERE is_valid = TRUE
)
SELECT
  commit_id, repo_name, branch, date,
  total_pipeline_energy_j            AS current_j,
  prev_j,
  ROUND((total_pipeline_energy_j - prev_j)::numeric, 3)   AS delta_j,
  ROUND(100.0*(total_pipeline_energy_j - prev_j)/NULLIF(prev_j,0), 2) AS delta_pct
FROM runs WHERE prev_j IS NOT NULL;

CREATE OR REPLACE VIEW v_weekly_energy_delta AS
WITH weekly AS (
  SELECT DATE_TRUNC('week', date) AS wk, SUM(total_energy_j) AS total_j
  FROM energy_jobs WHERE is_valid = TRUE GROUP BY 1
)
SELECT wk AS week_start, total_j,
  LAG(total_j) OVER (ORDER BY wk)  AS prev_j,
  ROUND(100.0*(total_j - LAG(total_j) OVER (ORDER BY wk))
    / NULLIF(LAG(total_j) OVER (ORDER BY wk), 0), 2) AS delta_pct
FROM weekly;

-- ── FIX 12: Populate data_lineage from ingestion_log ─────────
-- If data_lineage is empty, seed it from existing ingestion_log
INSERT INTO data_lineage (recorded_at, source_file, site, file_type, rows_parsed, rows_inserted, rows_invalid, notes)
SELECT
  run_at,
  'ingestion_log_seed',
  'all_sites',
  'master',
  rows_ingested + rows_rejected,
  rows_ingested,
  rows_rejected,
  'seeded from ingestion_log'
FROM ingestion_log
WHERE run_at > NOW() - INTERVAL '7 days'
ON CONFLICT DO NOTHING;

-- ── VERIFY ALL ───────────────────────────────────────────────
\echo ''
\echo '=== VERIFICATION ==='
SELECT 'v_top_jobs_energy'        AS view_name, COUNT(*) AS rows FROM v_top_jobs_energy
UNION ALL SELECT 'v_daily_energy_by_site',   COUNT(*) FROM v_daily_energy_by_site
UNION ALL SELECT 'v_energy_by_measurement',  COUNT(*) FROM v_energy_by_measurement
UNION ALL SELECT 'v_component_energy',       COUNT(*) FROM v_component_energy
UNION ALL SELECT 'v_monthly_job_energy',     COUNT(*) FROM v_monthly_job_energy
UNION ALL SELECT 'v_ingestion_health',       COUNT(*) FROM v_ingestion_health
UNION ALL SELECT 'v_pipeline_commit_history',COUNT(*) FROM v_pipeline_commit_history
UNION ALL SELECT 'v_commit_energy_delta',    COUNT(*) FROM v_commit_energy_delta
UNION ALL SELECT 'v_weekly_energy_delta',    COUNT(*) FROM v_weekly_energy_delta
UNION ALL SELECT 'data_lineage',             COUNT(*) FROM data_lineage;

\echo ''
\echo '=== MEASUREMENT_SRC DISTRIBUTION ==='
SELECT measurement_src, COUNT(*) FROM energy_jobs WHERE is_valid=TRUE GROUP BY 1;

\echo '=== ARCH DISTRIBUTION ==='
SELECT arch, COUNT(*) FROM energy_jobs WHERE is_valid=TRUE GROUP BY 1;
