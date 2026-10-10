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
