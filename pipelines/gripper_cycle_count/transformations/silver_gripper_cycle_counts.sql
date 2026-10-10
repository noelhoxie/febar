-- Silver layer: per-cycle robot performance metrics (cycle time, velocity, acceleration)
CREATE OR REFRESH MATERIALIZED VIEW silver_gripper_cycle_counts
COMMENT 'Per-cycle robot performance: cycle time, avg velocity, and acceleration across all joints'
AS
-- Step 1: All cycling events with per-joint velocity
WITH cycling_events AS (
  SELECT
    event_time,
    arm_serial,
    joint_id,
    joint_name,
    position,
    cycle_phase,
    LAG(cycle_phase) OVER (PARTITION BY arm_serial, joint_id ORDER BY event_time) AS prev_phase,
    ABS(position - LAG(position) OVER (PARTITION BY arm_serial, joint_id ORDER BY event_time)) * 1.0
      / NULLIF(
        UNIX_TIMESTAMP(event_time)
          - UNIX_TIMESTAMP(LAG(event_time) OVER (PARTITION BY arm_serial, joint_id ORDER BY event_time)),
        0
      ) AS velocity
  FROM robot_demo_catalog.robot_demo.so101_joints
  WHERE cycling = true
    AND arm_serial NOT IN ('probe')
),
-- Step 2: Compute acceleration (velocity change rate per joint)
with_acceleration AS (
  SELECT
    event_time,
    arm_serial,
    joint_id,
    velocity,
    ABS(velocity - LAG(velocity) OVER (PARTITION BY arm_serial, joint_id ORDER BY event_time))
      / NULLIF(
        UNIX_TIMESTAMP(event_time)
          - UNIX_TIMESTAMP(LAG(event_time) OVER (PARTITION BY arm_serial, joint_id ORDER BY event_time)),
        0
      ) AS acceleration
  FROM cycling_events
),
-- Step 3: Assign cycle numbers from gripper close-to-open transitions
cycle_map AS (
  SELECT
    event_time,
    arm_serial,
    SUM(CASE WHEN cycle_phase = 'open' AND prev_phase = 'close' THEN 1 ELSE 0 END)
      OVER (PARTITION BY arm_serial ORDER BY event_time) AS cycle_number
  FROM cycling_events
  WHERE joint_name = 'Gripper'
)
-- Step 4: Aggregate per cycle per arm
SELECT
  a.arm_serial,
  c.cycle_number,
  MIN(a.event_time) AS cycle_start_time,
  MAX(a.event_time) AS cycle_end_time,
  ROUND(UNIX_TIMESTAMP(MAX(a.event_time)) - UNIX_TIMESTAMP(MIN(a.event_time)), 2) AS cycle_time_sec,
  ROUND(AVG(a.velocity), 2) AS avg_velocity,
  ROUND(MAX(a.velocity), 2) AS peak_velocity,
  ROUND(AVG(a.acceleration), 2) AS avg_acceleration,
  ROUND(MAX(a.acceleration), 2) AS peak_acceleration
FROM with_acceleration a
JOIN cycle_map c
  ON a.arm_serial = c.arm_serial
  AND a.event_time = c.event_time
WHERE c.cycle_number > 0
GROUP BY a.arm_serial, c.cycle_number;
