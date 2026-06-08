-- ============================================================
-- schema_additions.sql — Run AFTER existing schema.sql
-- Adds views and indexes required by the improved dashboards
-- psql -U greenops -d greenops -f schema_additions.sql
-- ============================================================

-- ─── Fix: ensure measurement_src is never NULL ───────────────
UPDATE energy_jobs SET measurement_src = 'unknown'
WHERE measurement_src IS NULL OR measurement_src = '';

UPDATE energy_granularity SET measurement_src = 'unknown'
WHERE measurement_src IS NULL OR measurement_src = '';

-- ─── Fix: ensure arch is never NULL ─────────────────────────
UPDATE energy_jobs SET arch = 'x86_64'
WHERE arch IS NULL OR arch = '';

-- ─── Fix: ensure category is never NULL ─────────────────────
UPDATE energy_jobs SET category = 'UNKNOWN'
WHERE category IS NULL OR category = '';

-- ─── New view: v_pipeline_commit_history ────────────────────
-- Used by Pipeline Evolution dashboard
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
  p.measurement_src,
  COUNT(j.id)                AS job_count,
  AVG(j.duration_s)          AS avg_job_duration_s,
  SUM(j.cpu_j)               AS total_cpu_j,
  SUM(j.ram_j)               AS total_ram_j,
  SUM(j.gpu_j)               AS total_gpu_j
FROM energy_pipelines p
LEFT JOIN energy_jobs j
  ON j.pipeline_id = p.pipeline_id
  AND j.is_valid = TRUE
WHERE p.is_valid = TRUE
GROUP BY p.date, p.pipeline_id, p.commit_id, p.repo_name,
         p.branch, p.trigger, p.total_pipeline_energy_j,
         p.site, p.measurement_src;

-- ─── New view: v_commit_energy_delta ────────────────────────
-- Energy regression per commit vs previous run on same branch
CREATE OR REPLACE VIEW v_commit_energy_delta AS
WITH runs AS (
  SELECT
    commit_id,
    repo_name,
    branch,
    date,
    total_pipeline_energy_j,
    LAG(total_pipeline_energy_j)
      OVER (PARTITION BY repo_name, branch ORDER BY date) AS prev_j
  FROM energy_pipelines
  WHERE is_valid = TRUE
)
SELECT
  commit_id,
  repo_name,
  branch,
  date,
  total_pipeline_energy_j                                    AS current_j,
  prev_j,
  ROUND((total_pipeline_energy_j - prev_j)::numeric, 3)     AS delta_j,
  ROUND(
    (100.0 * (total_pipeline_energy_j - prev_j) / NULLIF(prev_j, 0))::numeric, 2
  )                                                          AS delta_pct
FROM runs
WHERE prev_j IS NOT NULL;

-- ─── New view: v_job_structure_evolution ────────────────────
-- Track how many distinct jobs a repo has over time
CREATE OR REPLACE VIEW v_job_structure_evolution AS
SELECT
  DATE_TRUNC('day', date)    AS day,
  repo_name,
  COUNT(DISTINCT job_name)   AS distinct_jobs,
  COUNT(*)                   AS total_runs,
  SUM(total_energy_j)        AS total_j,
  AVG(total_energy_j)        AS avg_j,
  AVG(duration_s)            AS avg_duration_s
FROM energy_jobs
WHERE is_valid = TRUE
GROUP BY 1, 2;

-- ─── New view: v_weekly_energy_delta ────────────────────────
-- Week-over-week energy delta for the WoW panel
CREATE OR REPLACE VIEW v_weekly_energy_delta AS
WITH weekly AS (
  SELECT
    DATE_TRUNC('week', date) AS wk,
    SUM(total_energy_j)      AS total_j
  FROM energy_jobs
  WHERE is_valid = TRUE
  GROUP BY 1
)
SELECT
  wk                                                AS week_start,
  total_j,
  LAG(total_j) OVER (ORDER BY wk)                  AS prev_j,
  ROUND(
    (100.0 * (total_j - LAG(total_j) OVER (ORDER BY wk)) / NULLIF(LAG(total_j) OVER (ORDER BY wk), 0))::numeric, 2
  )                                                  AS delta_pct
FROM weekly;

-- ─── New index: support commit-level queries ─────────────────
CREATE INDEX IF NOT EXISTS idx_ep_commit   ON energy_pipelines (commit_id);
CREATE INDEX IF NOT EXISTS idx_ep_branch   ON energy_pipelines (branch);
CREATE INDEX IF NOT EXISTS idx_ep_trigger  ON energy_pipelines (trigger);
CREATE INDEX IF NOT EXISTS idx_ej_trigger  ON energy_jobs (trigger);
CREATE INDEX IF NOT EXISTS idx_ej_branch   ON energy_jobs (branch);
CREATE INDEX IF NOT EXISTS idx_ej_commit   ON energy_jobs (commit_id);
CREATE INDEX IF NOT EXISTS idx_ej_zero     ON energy_jobs (total_energy_j) WHERE total_energy_j = 0;

-- ─── Verify ───────────────────────────────────────────────────
SELECT 'v_pipeline_commit_history' AS view_name, COUNT(*) AS rows FROM v_pipeline_commit_history
UNION ALL
SELECT 'v_commit_energy_delta',   COUNT(*) FROM v_commit_energy_delta
UNION ALL
SELECT 'v_job_structure_evolution', COUNT(*) FROM v_job_structure_evolution
UNION ALL
SELECT 'v_weekly_energy_delta',   COUNT(*) FROM v_weekly_energy_delta;
