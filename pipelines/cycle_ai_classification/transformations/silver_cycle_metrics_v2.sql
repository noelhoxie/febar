-- ============================================================
-- EXECUTION EVIDENCE
-- Pipeline: Cycle AI Classification (bfd04309-db1c-4429-bb3e-3c61fef4ea99)
-- Output table: robot_demo_catalog.robot_demo.silver_cycle_metrics_v2
-- Type: STREAMING TABLE (with watermark for incremental processing)
-- Row count: 1,034 cycles | Columns: 14
-- Note: This definition was created inline via the pipeline UI, not as a
--   workspace file. Extracted via SHOW CREATE TABLE for reproducibility.
-- Combines cycle metrics + AI classification in a single streaming query
--   (unlike the MV-based silver_cycle_metrics_base + silver_cycle_metrics split)
-- ============================================================

CREATE STREAMING TABLE silver_cycle_metrics_v2 (
  arm_serial STRING,
  cycle_index INT,
  cycle_start TIMESTAMP,
  cycle_end TIMESTAMP,
  cycle_time_sec DECIMAL(24,2),
  distinct_phases BIGINT,
  snapshot_count BIGINT,
  rotation_type STRING,
  cycle_quality STRING,
  shift_name STRING,
  is_cycle_time_outlier BOOLEAN,
  ai_failure_category STRING,
  ai_confidence DOUBLE,
  ai_rationale STRING
)
AS WITH parsed AS (
  SELECT
    event_time,
    payload:arms[0].serial::STRING AS arm_serial,
    payload:cycle_phase::STRING AS cycle_phase,
    payload:cycle_index::INT AS cycle_index
  FROM STREAM(robot_demo_catalog.robot_demo.so101_snapshots)
    WATERMARK event_time DELAY OF INTERVAL 10 MINUTES
  WHERE payload:cycling::BOOLEAN = true
    AND payload:cycle_index::INT > 0
),

-- Merged cycle metrics and rotation info (single GROUP BY for streaming compatibility)
cycle_data AS (
  SELECT
    arm_serial,
    cycle_index,
    MIN(event_time) AS cycle_start,
    MAX(event_time) AS cycle_end,
    ROUND((unix_millis(MAX(event_time)) - unix_millis(MIN(event_time))) / 1000.0, 2) AS cycle_time_sec,
    APPROX_COUNT_DISTINCT(cycle_phase) AS distinct_phases,
    COUNT(*) AS snapshot_count,
    MAX(CASE WHEN cycle_phase LIKE '%rotate%' THEN cycle_phase END) AS rotation_phase,
    CASE
      WHEN MAX(CASE WHEN cycle_phase LIKE '%rotate%' THEN cycle_phase END) LIKE '%100 deg%'
        THEN '100 deg'
      WHEN MAX(CASE WHEN cycle_phase LIKE '%rotate%' THEN cycle_phase END) LIKE '%110 deg%'
        THEN '110 deg extra'
      ELSE 'unknown'
    END AS rotation_type
  FROM parsed
  GROUP BY arm_serial, cycle_index
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
    cd.arm_serial,
    cd.cycle_index,
    cd.cycle_start,
    cd.cycle_end,
    cd.cycle_time_sec,
    cd.distinct_phases,
    cd.snapshot_count,
    cd.rotation_type,
    CASE
      WHEN cd.rotation_type = '100 deg' THEN 'Good'
      ELSE 'Bad'
    END AS cycle_quality,
    s.shift_name,
    false AS is_cycle_time_outlier
  FROM cycle_data cd
  JOIN current_shifts s ON (
    (CAST(SPLIT(s.start_time, ':')[0] AS INT) < CAST(SPLIT(s.end_time, ':')[0] AS INT)
     AND HOUR(cd.cycle_start) >= CAST(SPLIT(s.start_time, ':')[0] AS INT)
     AND HOUR(cd.cycle_start) < CAST(SPLIT(s.end_time, ':')[0] AS INT))
    OR
    (CAST(SPLIT(s.start_time, ':')[0] AS INT) >= CAST(SPLIT(s.end_time, ':')[0] AS INT)
     AND (HOUR(cd.cycle_start) >= CAST(SPLIT(s.start_time, ':')[0] AS INT)
          OR HOUR(cd.cycle_start) < CAST(SPLIT(s.end_time, ':')[0] AS INT)))
  )
),

classified AS (
  SELECT
    *,
    ai_classify(
      CONCAT(
        'Robot arm ', arm_serial,
        ', cycle ', cycle_index,
        ': cycle_time=', cycle_time_sec, 's',
        ', distinct_phases=', distinct_phases,
        ', rotation_type=', rotation_type,
        ', snapshots=', snapshot_count
      ),
      '{
        "normal_operation": "Cycle completed within expected parameters - standard 100 deg rotation, typical cycle time around 30s, all 11 phases present",
        "mechanical_drift": "Cycle time is unusually long or short, suggesting servo wear, friction, or belt slippage",
        "calibration_error": "Rotation shows 110 deg extra instead of standard 100 deg, indicating arm calibration or homing offset issue",
        "incomplete_cycle": "Fewer than expected distinct phases, suggesting the cycle was interrupted or a phase was skipped"
      }',
      MAP(
        'version', '2.1',
        'instructions', 'Classify each robot arm pick-and-place cycle into the single most likely root cause category. A normal cycle has 100 deg rotation, approximately 30 second duration, and 11 distinct phases. The 110 deg extra rotation is a known calibration defect.',
        'enableConfidenceScores', 'true',
        'enableRationales', 'true'
      )
    ) AS classification
  FROM combined
)

SELECT
  arm_serial,
  cycle_index,
  cycle_start,
  cycle_end,
  cycle_time_sec,
  distinct_phases,
  snapshot_count,
  rotation_type,
  cycle_quality,
  shift_name,
  is_cycle_time_outlier,
  classification:response[0].value::STRING AS ai_failure_category,
  classification:response[0].confidence_score::DOUBLE AS ai_confidence,
  classification:response[0].rationale::STRING AS ai_rationale
FROM classified;
