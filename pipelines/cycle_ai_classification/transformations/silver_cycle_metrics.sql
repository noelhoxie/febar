-- ============================================================
-- EXECUTION EVIDENCE
-- Pipeline: Cycle AI Classification (bfd04309-db1c-4429-bb3e-3c61fef4ea99)
-- Output table: robot_demo_catalog.robot_demo.silver_cycle_metrics_base
-- Row count: 1,034 cycles | Columns: 12
-- Last successful update: 2eee0d2a (COMPLETED, full refresh)
-- Data: 12 recording sessions, hybrid cycle_id (native UUID + session-derived)
-- Shift tagging: JOINs lb_shift_schedule_history via Lakebase CDF
-- ============================================================
-- Silver base layer: deterministic per-cycle metrics (incremental-capable)
-- All computations are deterministic — no AI functions here
-- AI classification is applied in a separate layer (silver_cycle_classified.sql)
CREATE OR REFRESH MATERIALIZED VIEW silver_cycle_metrics_base
AS
WITH raw_parsed AS (
  SELECT
    event_time,
    payload:arms[0].serial::STRING AS arm_serial,
    payload:cycle_phase::STRING AS cycle_phase,
    payload:cycle_index::INT AS cycle_index,
    payload:cycle_id::STRING AS native_cycle_id
  FROM robot_demo_catalog.robot_demo.so101_snapshots
  WHERE payload:cycling::BOOLEAN = true
    AND payload:cycle_index::INT > 0
),

-- Detect recording sessions: cycle_index resets each session
-- A >300s gap between consecutive events marks a new session boundary
with_gaps AS (
  SELECT
    *,
    UNIX_TIMESTAMP(event_time) - UNIX_TIMESTAMP(
      LAG(event_time) OVER (PARTITION BY arm_serial ORDER BY event_time)
    ) AS gap_sec
  FROM raw_parsed
),

parsed AS (
  SELECT
    event_time,
    arm_serial,
    cycle_phase,
    cycle_index,
    native_cycle_id,
    SUM(CASE WHEN gap_sec > 300 OR gap_sec IS NULL THEN 1 ELSE 0 END)
      OVER (PARTITION BY arm_serial ORDER BY event_time) AS session_id
  FROM with_gaps
),

-- Unified cycle identifier: native UUID when available, session-derived fallback
with_cycle_id AS (
  SELECT
    event_time,
    arm_serial,
    cycle_phase,
    cycle_index,
    COALESCE(native_cycle_id, CONCAT(session_id, '_', cycle_index)) AS cycle_id
  FROM parsed
),

rotation_per_cycle AS (
  SELECT
    arm_serial,
    cycle_id,
    CASE
      WHEN MAX(CASE WHEN cycle_phase LIKE '%rotate%' THEN cycle_phase END) LIKE '%100 deg%'
        THEN '100 deg'
      WHEN MAX(CASE WHEN cycle_phase LIKE '%rotate%' THEN cycle_phase END) LIKE '%110 deg%'
        THEN '110 deg extra'
      ELSE 'unknown'
    END AS rotation_type
  FROM with_cycle_id
  GROUP BY arm_serial, cycle_id
),

cycle_metrics AS (
  SELECT
    arm_serial,
    cycle_id,
    ANY_VALUE(cycle_index) AS cycle_index,
    MIN(event_time) AS cycle_start,
    MAX(event_time) AS cycle_end,
    ROUND((unix_millis(MAX(event_time)) - unix_millis(MIN(event_time))) / 1000.0, 2) AS cycle_time_sec,
    COUNT(DISTINCT cycle_phase) AS distinct_phases,
    COUNT(*) AS snapshot_count
  FROM with_cycle_id
  GROUP BY arm_serial, cycle_id
),

-- Current active shift schedule from Lakebase CDF
current_shifts AS (
  SELECT id, shift_name, start_time, end_time
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sort_by DESC) AS rn
    FROM robot_demo_catalog.robot_demo.lb_shift_schedule_history
    WHERE _pg_change_type != 'DELETE'
  )
  WHERE rn = 1 AND is_active = true
),

combined AS (
  SELECT
    cm.arm_serial,
    cm.cycle_id,
    cm.cycle_index,
    cm.cycle_start,
    cm.cycle_end,
    cm.cycle_time_sec,
    cm.distinct_phases,
    cm.snapshot_count,
    r.rotation_type,
    CASE
      WHEN r.rotation_type = '100 deg' THEN 'Good'
      ELSE 'Bad'
    END AS cycle_quality,
    s.shift_name
  FROM cycle_metrics cm
  JOIN rotation_per_cycle r
    ON cm.arm_serial = r.arm_serial
    AND cm.cycle_id = r.cycle_id
  JOIN current_shifts s ON (
    (CAST(SPLIT(s.start_time, ':')[0] AS INT) < CAST(SPLIT(s.end_time, ':')[0] AS INT)
     AND HOUR(cm.cycle_start) >= CAST(SPLIT(s.start_time, ':')[0] AS INT)
     AND HOUR(cm.cycle_start) < CAST(SPLIT(s.end_time, ':')[0] AS INT))
    OR
    (CAST(SPLIT(s.start_time, ':')[0] AS INT) >= CAST(SPLIT(s.end_time, ':')[0] AS INT)
     AND (HOUR(cm.cycle_start) >= CAST(SPLIT(s.start_time, ':')[0] AS INT)
          OR HOUR(cm.cycle_start) < CAST(SPLIT(s.end_time, ':')[0] AS INT)))
  )
),

-- Automatic anomaly detection: global adaptive threshold via window function
-- Cycles exceeding 5x the 10th percentile duration are flagged as outliers
combined_with_outliers AS (
  SELECT
    c.*,
    CASE
      WHEN c.cycle_time_sec > PERCENTILE_APPROX(c.cycle_time_sec, 0.10) OVER () * 5 THEN true
      ELSE false
    END AS is_cycle_time_outlier
  FROM combined c
)

SELECT
  arm_serial,
  cycle_id,
  cycle_index,
  cycle_start,
  cycle_end,
  cycle_time_sec,
  distinct_phases,
  snapshot_count,
  rotation_type,
  cycle_quality,
  shift_name,
  is_cycle_time_outlier
FROM combined_with_outliers;
