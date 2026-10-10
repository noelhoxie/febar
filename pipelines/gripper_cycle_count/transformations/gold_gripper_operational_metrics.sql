-- ============================================================
-- EXECUTION EVIDENCE
-- Pipeline: Gripper Cycle Count (5b456359-3fa5-4ddd-b8ab-66299601c16b)
-- Status: SUPERSEDED by Cycle AI Classification Pipeline
-- Output tables: No longer exist (pipeline deleted)
-- Historical: These SQL definitions ran successfully and produced
--   bronze_gripper_events, bronze_gripper_all_events,
--   silver_gripper_cycle_counts, gold_gripper_operational_metrics
-- Kept as reference for the original OEE computation approach
-- ============================================================
-- Gold layer: OEE (Overall Equipment Effectiveness) per robot arm
CREATE OR REFRESH MATERIALIZED VIEW gold_gripper_operational_metrics
COMMENT 'Per-arm OEE metrics: availability, performance, quality, and overall equipment effectiveness'
AS
-- Step 1: All events with inter-event duration via LEAD
WITH all_events AS (
  SELECT
    event_time,
    arm_serial,
    cycling,
    UNIX_TIMESTAMP(LEAD(event_time) OVER (PARTITION BY arm_serial ORDER BY event_time))
      - UNIX_TIMESTAMP(event_time) AS duration_sec
  FROM bronze_gripper_all_events
),
-- Step 2: Availability — total observed time vs cycling run time
availability AS (
  SELECT
    arm_serial,
    SUM(duration_sec) AS total_observed_sec,
    SUM(CASE WHEN cycling = true THEN duration_sec ELSE 0 END) AS run_time_sec
  FROM all_events
  GROUP BY ALL
),
-- Step 3: Cycling events with phase transition detection
cycling_events AS (
  SELECT
    event_time,
    arm_serial,
    cycle_phase,
    position,
    min_limit,
    max_limit,
    LAG(cycle_phase) OVER (PARTITION BY arm_serial ORDER BY event_time) AS prev_phase
  FROM bronze_gripper_all_events
  WHERE cycling = true
),
-- Step 4: Mark close→open boundaries and assign cycle numbers
cycle_marked AS (
  SELECT
    event_time,
    arm_serial,
    cycle_phase,
    position,
    min_limit,
    max_limit,
    CASE WHEN cycle_phase = 'open' AND prev_phase = 'close' THEN 1 ELSE 0 END AS is_cycle_start,
    SUM(CASE WHEN cycle_phase = 'open' AND prev_phase = 'close' THEN 1 ELSE 0 END)
      OVER (PARTITION BY arm_serial ORDER BY event_time) AS cycle_number
  FROM cycling_events
),
-- Step 5: Cycle boundary timestamps for inter-cycle duration
cycle_boundaries AS (
  SELECT
    arm_serial,
    cycle_number,
    event_time AS cycle_time,
    UNIX_TIMESTAMP(event_time) - UNIX_TIMESTAMP(
      LAG(event_time) OVER (PARTITION BY arm_serial ORDER BY event_time)
    ) AS cycle_duration_sec
  FROM cycle_marked
  WHERE is_cycle_start = 1
),
-- Step 6: Per-cycle quality — travel range as pct of full range
cycle_quality AS (
  SELECT
    arm_serial,
    cycle_number,
    (MAX(position) - MIN(position)) * 100.0
      / NULLIF(MAX(max_limit) - MIN(min_limit), 0) AS travel_pct
  FROM cycle_marked
  WHERE cycle_number > 0
  GROUP BY ALL
),
-- Step 7: Join duration and quality per cycle
cycle_metrics AS (
  SELECT
    q.arm_serial,
    q.cycle_number,
    b.cycle_duration_sec,
    q.travel_pct,
    CASE WHEN q.travel_pct >= 45.0 THEN 1 ELSE 0 END AS is_good
  FROM cycle_quality q
  LEFT JOIN cycle_boundaries b
    ON q.arm_serial = b.arm_serial AND q.cycle_number = b.cycle_number
),
-- Step 8: Aggregate cycle metrics per arm
cycle_agg AS (
  SELECT
    arm_serial,
    COUNT(*) AS total_cycles,
    SUM(is_good) AS good_cycles,
    COUNT(*) - SUM(is_good) AS defective_cycles,
    MIN(cycle_duration_sec) AS ideal_cycle_time_sec,
    AVG(cycle_duration_sec) AS avg_cycle_duration_sec
  FROM cycle_metrics
  GROUP BY ALL
)
-- Final: Compute OEE = Availability × Performance × Quality
SELECT
  c.arm_serial,
  c.total_cycles,
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
JOIN availability a ON c.arm_serial = a.arm_serial;
