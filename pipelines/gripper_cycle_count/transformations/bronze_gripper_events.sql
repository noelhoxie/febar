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
-- Bronze layer: stream gripper cycling events from source
CREATE OR REFRESH STREAMING TABLE bronze_gripper_events
COMMENT 'Filtered streaming data for gripper joint cycling events'
AS
SELECT
  event_time,
  arm_serial,
  cycle_phase
FROM STREAM(robot_demo_catalog.robot_demo.so101_joints)
WHERE joint_name = 'Gripper'
  AND cycling = true;
