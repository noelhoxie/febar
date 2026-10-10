-- ============================================================
-- EXECUTION EVIDENCE
-- Pipeline: Cycle AI Classification (bfd04309-db1c-4429-bb3e-3c61fef4ea99)
-- Output table: robot_demo_catalog.robot_demo.gold_operational_kpis
-- Row count: 7 (per arm × per shift × per day)
-- OEE range: 0.46% - 16.28% | Availability: 2.61% - 75.05%
-- Quality: 0% - 59.15% | Arm: 5C821097531 across Day/Swing/Night shifts
-- Uses P20 ideal_cycle_time, excludes outlier cycles (>5x P10 duration)
-- ============================================================
-- Gold layer: OEE (Overall Equipment Effectiveness) per robot arm
-- Reads directly from so101_snapshots for availability (no bronze intermediate)
-- Quality component uses AI classification: normal_operation = good cycle
CREATE OR REFRESH MATERIALIZED VIEW gold_operational_kpis
COMMENT 'Per-arm OEE metrics with AI-driven quality classification'
AS
-- Step 1: Extract raw events from source for availability
WITH raw_events AS (
  SELECT
    event_time,
    payload:arms[0].serial::STRING AS arm_serial,
    payload:cycling::BOOLEAN AS cycling
  FROM robot_demo_catalog.robot_demo.so101_snapshots
),

-- Step 2: Current active shift schedule from Lakebase CDF
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

-- Step 3: Inter-event durations, shift tagging and shift date for availability
timed_events AS (
  SELECT
    re.event_time,
    DATE(re.event_time) AS shift_date,
    re.arm_serial,
    re.cycling,
    s.shift_name,
    UNIX_TIMESTAMP(LEAD(re.event_time) OVER (PARTITION BY re.arm_serial ORDER BY re.event_time))
      - UNIX_TIMESTAMP(re.event_time) AS duration_sec
  FROM raw_events re
  JOIN current_shifts s ON (
    (CAST(SPLIT(s.start_time, ':')[0] AS INT) < CAST(SPLIT(s.end_time, ':')[0] AS INT)
     AND HOUR(re.event_time) >= CAST(SPLIT(s.start_time, ':')[0] AS INT)
     AND HOUR(re.event_time) < CAST(SPLIT(s.end_time, ':')[0] AS INT))
    OR
    (CAST(SPLIT(s.start_time, ':')[0] AS INT) >= CAST(SPLIT(s.end_time, ':')[0] AS INT)
     AND (HOUR(re.event_time) >= CAST(SPLIT(s.start_time, ':')[0] AS INT)
          OR HOUR(re.event_time) < CAST(SPLIT(s.end_time, ':')[0] AS INT)))
  )
),

-- Step 4: Availability per arm per shift per day
availability AS (
  SELECT
    arm_serial,
    shift_name,
    shift_date,
    SUM(duration_sec) AS total_observed_sec,
    SUM(CASE WHEN cycling = true THEN duration_sec ELSE 0 END) AS run_time_sec
  FROM timed_events
  GROUP BY ALL
),

-- Step 5: Aggregate cycle metrics per arm per shift per day (excluding cycle time outliers)
cycle_agg AS (
  SELECT
    arm_serial,
    shift_name,
    DATE(cycle_start) AS shift_date,
    COUNT(*) AS total_cycles,
    SUM(CASE WHEN is_cycle_time_outlier THEN 1 ELSE 0 END) AS outlier_cycles,
    SUM(CASE WHEN ai_failure_category = 'normal_operation' AND NOT is_cycle_time_outlier THEN 1 ELSE 0 END) AS good_cycles,
    SUM(CASE WHEN ai_failure_category != 'normal_operation' AND NOT is_cycle_time_outlier THEN 1 ELSE 0 END) AS defective_cycles,
    PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY cycle_time_sec)
      FILTER (WHERE NOT is_cycle_time_outlier) AS ideal_cycle_time_sec,
    AVG(CASE WHEN NOT is_cycle_time_outlier THEN cycle_time_sec END) AS avg_cycle_duration_sec
  FROM silver_cycle_metrics
  GROUP BY ALL
)

-- Final: OEE = Availability x Performance x Quality per arm per shift per day
SELECT
  c.arm_serial,
  c.shift_date,
  c.shift_name,
  c.total_cycles,
  c.outlier_cycles,
  c.good_cycles,
  c.defective_cycles,
  ROUND(a.run_time_sec, 2) AS run_time_sec,
  ROUND(a.total_observed_sec, 2) AS total_observed_sec,
  ROUND(c.ideal_cycle_time_sec, 2) AS ideal_cycle_time_sec,
  ROUND(c.avg_cycle_duration_sec, 2) AS avg_cycle_duration_sec,
  ROUND(a.run_time_sec * 100.0 / NULLIF(a.total_observed_sec, 0), 2) AS availability_pct,
  ROUND(c.ideal_cycle_time_sec * c.total_cycles * 100.0 / NULLIF(a.run_time_sec, 0), 2) AS performance_pct,
  ROUND(c.good_cycles * 100.0 / NULLIF(c.total_cycles, 0), 2) AS quality_pct,
  ROUND(
    (a.run_time_sec / NULLIF(a.total_observed_sec, 0))
    * (c.ideal_cycle_time_sec * c.total_cycles / NULLIF(a.run_time_sec, 0))
    * (c.good_cycles * 1.0 / NULLIF(c.total_cycles, 0))
    * 100.0,
    2
  ) AS oee_pct
FROM cycle_agg c
JOIN availability a
  ON c.arm_serial = a.arm_serial
  AND c.shift_name = a.shift_name
  AND c.shift_date = a.shift_date
ORDER BY c.arm_serial, c.shift_date, c.shift_name;
