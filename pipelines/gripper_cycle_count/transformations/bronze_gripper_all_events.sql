-- Bronze layer: all gripper events including stopped/probe states with position data
CREATE OR REFRESH STREAMING TABLE bronze_gripper_all_events
COMMENT 'All gripper joint events for OEE calculation including idle and cycling states'
AS
SELECT
  event_time,
  arm_serial,
  cycling,
  cycle_phase,
  position,
  min_limit,
  max_limit
FROM STREAM(robot_demo_catalog.robot_demo.so101_joints)
WHERE joint_name = 'Gripper'
  AND arm_serial NOT IN ('probe');
