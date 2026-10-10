-- Silver classification layer: AI failure category assignment
-- Reads from deterministic base (silver_cycle_metrics_base) and applies ai_classify
-- Non-deterministic: only this MV full-recomputes; the base refreshes incrementally
CREATE OR REFRESH MATERIALIZED VIEW silver_cycle_metrics
AS
WITH classified AS (
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
        "normal_operation": "Cycle completed within expected parameters \u2014 standard 100 deg rotation, typical cycle time around 30s, all 11 phases present",
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
  FROM robot_demo_catalog.robot_demo.silver_cycle_metrics_base
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
  is_cycle_time_outlier,
  classification:response[0].value::STRING AS ai_failure_category,
  classification:response[0].confidence_score::DOUBLE AS ai_confidence,
  classification:response[0].rationale::STRING AS ai_rationale
FROM classified;