import os
import uuid
import time
import json
from flask import Flask, jsonify, request, send_from_directory, make_response
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import psycopg2
import psycopg2.extras

app = Flask(__name__, static_folder='static')
w = WorkspaceClient()

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "6780d2ebf14114da")
TABLE = "robot_demo_catalog.robot_demo.gold_operational_kpis"

# Lakebase connection state
_pg_token = None
_pg_token_time = 0
TOKEN_REFRESH_SECS = 900  # 15 minutes
SCHEMA = "app_data"
SCHED_TABLE = f"{SCHEMA}.shift_schedule"


def get_pg_connection():
    """Get a Postgres connection with auto-refreshing token."""
    global _pg_token, _pg_token_time
    if _pg_token is None or time.time() - _pg_token_time > TOKEN_REFRESH_SECS:
        instance = os.environ.get("LAKEBASE_INSTANCE_NAME", "gripper-maintenance")
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[instance]
        )
        _pg_token = cred.token
        _pg_token_time = time.time()

    conn = psycopg2.connect(
        host=os.environ.get("PGHOST", ""),
        database=os.environ.get("PGDATABASE", "databricks_postgres"),
        user=os.environ.get("PGUSER", ""),
        port=int(os.environ.get("PGPORT", 5432)),
        password=_pg_token,
        sslmode=os.environ.get("PGSSLMODE", "require")
    )
    conn.autocommit = True
    return conn


def init_schedule_table():
    """Create the app-owned schema and shift_schedule table with full datetime."""
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            # Drop old TIME-based table if it exists and recreate with TIMESTAMP
            cur.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{SCHEMA}'
                  AND table_name = 'shift_schedule'
                  AND column_name = 'start_time'
            """)
            col = cur.fetchone()
            if col and col[1] in ('time without time zone', 'time with time zone'):
                cur.execute(f"DROP TABLE {SCHED_TABLE}")
                print("Dropped old TIME-based table, recreating with TIMESTAMP")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHED_TABLE} (
                    id SERIAL PRIMARY KEY,
                    shift_name TEXT NOT NULL,
                    shift_date DATE NOT NULL DEFAULT CURRENT_DATE,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP NOT NULL,
                    arm_serial TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute(f"SELECT COUNT(*) FROM {SCHED_TABLE}")
            if cur.fetchone()[0] == 0:
                cur.execute(f"""
                    INSERT INTO {SCHED_TABLE} (shift_name, shift_date, start_time, end_time, arm_serial, is_active)
                    VALUES
                        ('Day Shift',   CURRENT_DATE, CURRENT_DATE + TIME '06:00', CURRENT_DATE + TIME '14:00', NULL, TRUE),
                        ('Swing Shift', CURRENT_DATE, CURRENT_DATE + TIME '14:00', CURRENT_DATE + TIME '22:00', NULL, TRUE),
                        ('Night Shift', CURRENT_DATE, CURRENT_DATE + TIME '22:00', CURRENT_DATE + INTERVAL '1 day' + TIME '06:00', NULL, TRUE)
                """)
            print(f"Schedule table ready in {SCHED_TABLE}")
        conn.close()
    except Exception as e:
        print(f"Warning: Could not init schedule table: {e}")


TELEMETRY_TABLE = "robot_demo_catalog.robot_demo.so101_snapshots"
JOINT_NAMES = ['Shoulder pan', 'Shoulder lift', 'Elbow flex', 'Wrist flex', 'Wrist roll', 'Gripper']
DEFECTS_TABLE = "robot_demo_catalog.robot_demo.silver_cycle_metrics"
HOURLY_METRICS_TABLE = "robot_demo_catalog.robot_demo.silver_cycle_metrics"
HEALTH_TABLE = "robot_demo_catalog.robot_demo.silver_cycle_metrics_v2"
JOINT_NAMES = ['Shoulder pan', 'Shoulder lift', 'Elbow flex', 'Wrist flex', 'Wrist roll', 'Gripper']


import time as _time
_boot_ts = str(int(_time.time()))

@app.route('/')
def index():
    from flask import redirect
    return redirect(f'/app?v={_boot_ts}', code=302)

@app.route('/app')
def app_page():
    resp = make_response(send_from_directory('static', 'dashboard.html'))
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


@app.route('/api/telemetry')
def get_telemetry():
    """Return recent joint telemetry for time series chart."""
    minutes = request.args.get('minutes', '5')
    try:
        stmt = f"""
            SELECT event_time, payload
            FROM {TELEMETRY_TABLE}
            WHERE event_time >= current_timestamp() - INTERVAL {int(minutes)} MINUTES
            ORDER BY event_time
        """
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=stmt,
            wait_timeout="30s"
        )
        if response.status.state != StatementState.SUCCEEDED:
            return jsonify({"error": "Telemetry query failed"}), 500

        series = {name: [] for name in JOINT_NAMES}
        timestamps = []
        if response.result and response.result.data_array:
            for row_data in response.result.data_array:
                ts = row_data[0]
                try:
                    payload = json.loads(row_data[1])
                except (json.JSONDecodeError, TypeError):
                    continue
                arm = payload.get('arms', [{}])[0]
                joints = arm.get('joints', [])
                timestamps.append(ts)
                jmap = {j.get('name'): j for j in joints}
                for name in JOINT_NAMES:
                    j = jmap.get(name, {})
                    series[name].append(j.get('position'))

        return jsonify({
            "timestamps": timestamps,
            "series": series
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/robot_status')
def get_robot_status():
    """Return current robot phase based on latest telemetry."""
    try:
        stmt = f"""
            SELECT
                event_time,
                CAST(get_json_object(payload, '$.arms[0].joints[5].position') AS DOUBLE) as gripper_pos,
                CAST(get_json_object(payload, '$.arms[0].joints[0].position') AS DOUBLE) as shoulder_pan_pos
            FROM {TELEMETRY_TABLE}
            ORDER BY event_time DESC
            LIMIT 1
        """
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=stmt,
            wait_timeout="30s"
        )
        if response.status.state != StatementState.SUCCEEDED:
            return jsonify({"error": "Status query failed"}), 500

        if response.result and response.result.data_array:
            r = response.result.data_array[0]
            ts = r[0]
            gripper = float(r[1]) if r[1] else 0
            shoulder = float(r[2]) if r[2] else 0

            # Derive phase from gripper position
            # Gripper range: ~1627 (closed/gripping) to ~2921 (open/releasing)
            if gripper < 1900:
                phase = "DROP OFF"
            elif gripper > 2600:
                phase = "PICK UP"
            else:
                phase = "ROTATE"

            return jsonify({
                "phase": phase,
                "gripper_pos": gripper,
                "shoulder_pos": shoulder,
                "timestamp": ts
            })
        return jsonify({"phase": "UNKNOWN", "timestamp": ""})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/hourly_metrics')
def get_hourly_metrics():
    """Return OEE metrics broken down by hour."""
    try:
        stmt = f"""
            WITH cycles AS (
                SELECT
                    date_trunc('hour', cycle_start) as hour,
                    COUNT(*) as total_cycles,
                    SUM(CASE WHEN ai_failure_category = 'normal_operation'
                             OR ai_failure_category IS NULL THEN 1 ELSE 0 END) as good_cycles,
                    SUM(CASE WHEN ai_failure_category IS NOT NULL
                             AND ai_failure_category != 'normal_operation' THEN 1 ELSE 0 END) as defective_cycles,
                    PERCENTILE(CAST(cycle_time_sec AS DOUBLE), 0.5) as median_cycle_sec,
                    MIN(CAST(cycle_time_sec AS DOUBLE)) as ideal_cycle_sec
                FROM {HOURLY_METRICS_TABLE}
                WHERE CAST(cycle_time_sec AS DOUBLE) < 120
                GROUP BY 1
            )
            SELECT
                hour,
                total_cycles, good_cycles, defective_cycles,
                ROUND(LEAST(total_cycles * median_cycle_sec / 3600.0 * 100, 100), 1) as availability_pct,
                ROUND(LEAST(
                    CASE WHEN median_cycle_sec > 0
                         THEN ideal_cycle_sec / median_cycle_sec * 100 ELSE 0 END, 100), 1) as performance_pct,
                ROUND(CASE WHEN total_cycles > 0
                      THEN good_cycles * 100.0 / total_cycles ELSE 0 END, 1) as quality_pct,
                ROUND(
                    LEAST(total_cycles * median_cycle_sec / 3600.0, 1) *
                    LEAST(CASE WHEN median_cycle_sec > 0
                          THEN ideal_cycle_sec / median_cycle_sec ELSE 0 END, 1) *
                    CASE WHEN total_cycles > 0
                         THEN good_cycles * 1.0 / total_cycles ELSE 0 END * 100, 1
                ) as oee_pct
            FROM cycles
            ORDER BY hour
        """
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=stmt,
            wait_timeout="30s"
        )
        if response.status.state != StatementState.SUCCEEDED:
            return jsonify({"error": "Hourly metrics query failed"}), 500

        rows = []
        if response.result and response.result.data_array:
            for r in response.result.data_array:
                rows.append({
                    "hour": r[0],
                    "total_cycles": int(r[1]) if r[1] else 0,
                    "good_cycles": int(r[2]) if r[2] else 0,
                    "defective_cycles": int(r[3]) if r[3] else 0,
                    "availability_pct": float(r[4]) if r[4] else 0,
                    "performance_pct": float(r[5]) if r[5] else 0,
                    "quality_pct": float(r[6]) if r[6] else 0,
                    "oee_pct": float(r[7]) if r[7] else 0
                })
        return jsonify({"data": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/defects')
def get_defects():
    """Return top defect categories from AI classification."""
    try:
        stmt = f"""
            SELECT ai_failure_category,
                   COUNT(*) as total,
                   ROUND(AVG(ai_confidence), 2) as avg_confidence,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct
            FROM {DEFECTS_TABLE}
            WHERE ai_failure_category IS NOT NULL
              AND ai_failure_category != 'normal_operation'
            GROUP BY ai_failure_category
            ORDER BY total DESC
        """
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=stmt,
            wait_timeout="30s"
        )
        if response.status.state != StatementState.SUCCEEDED:
            return jsonify({"error": "Defects query failed"}), 500

        rows = []
        if response.result and response.result.data_array:
            for r in response.result.data_array:
                rows.append({
                    "category": r[0],
                    "total": int(r[1]) if r[1] else 0,
                    "avg_confidence": float(r[2]) if r[2] else 0,
                    "pct": float(r[3]) if r[3] else 0
                })
        return jsonify({"data": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/defect_detail')
def get_defect_detail():
    """Return detailed breakdown for a specific defect category."""
    category = request.args.get('category', '')
    if not category:
        return jsonify({"error": "category parameter required"}), 400
    try:
        # Recent cycles for this category with AI rationale
        stmt_cycles = f"""
            SELECT cycle_index, cycle_start, cycle_time_sec, distinct_phases,
                   cycle_quality, ai_confidence, ai_rationale, rotation_type
            FROM {HEALTH_TABLE}
            WHERE ai_failure_category = '{category}'
            ORDER BY cycle_start DESC
            LIMIT 25
        """
        res_c = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID, statement=stmt_cycles, wait_timeout="30s"
        )
        cycles = []
        if res_c.status.state == StatementState.SUCCEEDED and res_c.result and res_c.result.data_array:
            for r in res_c.result.data_array:
                cycles.append({
                    "cycle_index": int(r[0]) if r[0] else 0,
                    "cycle_start": r[1],
                    "cycle_time": float(r[2]) if r[2] else 0,
                    "phases": int(r[3]) if r[3] else 0,
                    "quality": r[4] or "",
                    "confidence": float(r[5]) if r[5] else 0,
                    "rationale": r[6] or "",
                    "rotation_type": r[7] or ""
                })

        # Confidence distribution buckets
        stmt_conf = f"""
            SELECT
                CASE
                    WHEN ai_confidence < 0.5 THEN '< 50%'
                    WHEN ai_confidence < 0.6 THEN '50-60%'
                    WHEN ai_confidence < 0.7 THEN '60-70%'
                    WHEN ai_confidence < 0.8 THEN '70-80%'
                    WHEN ai_confidence < 0.9 THEN '80-90%'
                    ELSE '90-100%'
                END as bucket,
                COUNT(*) as cnt
            FROM {HEALTH_TABLE}
            WHERE ai_failure_category = '{category}'
            GROUP BY 1
            ORDER BY MIN(ai_confidence)
        """
        res_conf = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID, statement=stmt_conf, wait_timeout="30s"
        )
        confidence_dist = []
        if res_conf.status.state == StatementState.SUCCEEDED and res_conf.result and res_conf.result.data_array:
            for r in res_conf.result.data_array:
                confidence_dist.append({"bucket": r[0], "count": int(r[1]) if r[1] else 0})

        # Trend over time (defects per day)
        stmt_trend = f"""
            SELECT DATE(cycle_start) as day, COUNT(*) as cnt
            FROM {HEALTH_TABLE}
            WHERE ai_failure_category = '{category}'
            GROUP BY 1 ORDER BY 1
        """
        res_trend = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID, statement=stmt_trend, wait_timeout="30s"
        )
        trend = []
        if res_trend.status.state == StatementState.SUCCEEDED and res_trend.result and res_trend.result.data_array:
            for r in res_trend.result.data_array:
                trend.append({"day": r[0], "count": int(r[1]) if r[1] else 0})

        return jsonify({"cycles": cycles, "confidence_dist": confidence_dist, "trend": trend})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/metrics')
def get_metrics():
    # If old cached page calls without ?v=2, tell user to refresh
    if request.args.get('v') != '2':
        return jsonify({"error": "Dashboard updated! Please refresh your browser (Cmd+Shift+R or Ctrl+Shift+R) to see new features including hover charts and robot status."}), 200
    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=f"SELECT * FROM {TABLE}",
            wait_timeout="30s"
        )
        if response.status.state != StatementState.SUCCEEDED:
            return jsonify({"error": "Query failed", "details": str(response.status)}), 500

        columns = [col.name for col in response.manifest.schema.columns]
        rows = []
        if response.result and response.result.data_array:
            for row_data in response.result.data_array:
                row = {}
                for i, col_name in enumerate(columns):
                    val = row_data[i]
                    if val is not None:
                        try:
                            val = float(val)
                        except (ValueError, TypeError):
                            pass
                    row[col_name] = val
                rows.append(row)

        return jsonify({"data": rows, "columns": columns, "_v": 2})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/schedules', methods=['GET'])
def get_schedules():
    try:
        conn = get_pg_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT id, shift_name, shift_date::text,
                       start_time::text, end_time::text,
                       arm_serial, is_active, created_at::text
                FROM {SCHED_TABLE} ORDER BY shift_date, start_time
            """)
            rows = cur.fetchall()
        conn.close()
        return jsonify({"data": [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/schedules', methods=['POST'])
def create_schedule():
    try:
        body = request.get_json()
        conn = get_pg_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                INSERT INTO {SCHED_TABLE} (shift_name, shift_date, start_time, end_time, arm_serial, is_active)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id, shift_name, shift_date::text, start_time::text, end_time::text, arm_serial, is_active, created_at::text
            """, (
                body['shift_name'],
                body['shift_date'],
                body['start_time'],
                body['end_time'],
                body.get('arm_serial'),
                body.get('is_active', True)
            ))
            row = dict(cur.fetchone())
        conn.close()
        return jsonify({"data": row}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/schedules/<int:schedule_id>', methods=['PUT'])
def update_schedule(schedule_id):
    try:
        body = request.get_json()
        conn = get_pg_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                UPDATE {SCHED_TABLE}
                SET shift_name = %s, shift_date = %s, start_time = %s, end_time = %s,
                    arm_serial = %s, is_active = %s
                WHERE id = %s
                RETURNING id, shift_name, shift_date::text, start_time::text, end_time::text, arm_serial, is_active, created_at::text
            """, (
                body['shift_name'],
                body['shift_date'],
                body['start_time'],
                body['end_time'],
                body.get('arm_serial'),
                body.get('is_active', True),
                schedule_id
            ))
            row = cur.fetchone()
            if not row:
                conn.close()
                return jsonify({"error": "Schedule not found"}), 404
            row = dict(row)
        conn.close()
        return jsonify({"data": row})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/schedules/<int:schedule_id>', methods=['DELETE'])
def delete_schedule(schedule_id):
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {SCHED_TABLE} WHERE id = %s RETURNING id", (schedule_id,))
            deleted = cur.fetchone()
        conn.close()
        if not deleted:
            return jsonify({"error": "Schedule not found"}), 404
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/current_context')
def get_current_context():
    """Return current shift info and cycle count."""
    try:
        from datetime import datetime, timedelta
        now = datetime.utcnow()
        hour = now.hour

        if 6 <= hour < 14:
            shift_name = "Day Shift"
            s_start = now.replace(hour=6, minute=0, second=0, microsecond=0)
            s_end = now.replace(hour=14, minute=0, second=0, microsecond=0)
        elif 14 <= hour < 22:
            shift_name = "Swing Shift"
            s_start = now.replace(hour=14, minute=0, second=0, microsecond=0)
            s_end = now.replace(hour=22, minute=0, second=0, microsecond=0)
        else:
            shift_name = "Night Shift"
            if hour >= 22:
                s_start = now.replace(hour=22, minute=0, second=0, microsecond=0)
                s_end = (now + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)
            else:
                s_start = (now - timedelta(days=1)).replace(hour=22, minute=0, second=0, microsecond=0)
                s_end = now.replace(hour=6, minute=0, second=0, microsecond=0)

        start_str = s_start.strftime('%Y-%m-%dT%H:%M:%S')
        end_str = s_end.strftime('%Y-%m-%dT%H:%M:%S')

        stmt = f"""
            SELECT COUNT(*) as cycle_count
            FROM {HEALTH_TABLE}
            WHERE cycle_start >= '{start_str}'
              AND cycle_start < '{end_str}'
        """
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=stmt,
            wait_timeout="30s"
        )
        cycle_count = 0
        if response.status.state == StatementState.SUCCEEDED and response.result and response.result.data_array:
            cycle_count = int(response.result.data_array[0][0]) if response.result.data_array[0][0] else 0

        return jsonify({
            "shift_name": shift_name,
            "start_time": start_str,
            "end_time": end_str,
            "cycle_count": cycle_count,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/recent_cycles')
def get_recent_cycles():
    """Return recent completed cycles with AI classification results."""
    try:
        limit = request.args.get('limit', '15')
        stmt = f"""
            SELECT
                cycle_index,
                cycle_start,
                cycle_quality,
                ai_failure_category,
                ROUND(ai_confidence, 2) as ai_confidence,
                ROUND(CAST(cycle_time_sec AS DOUBLE), 1) as cycle_time_sec,
                distinct_phases,
                rotation_type
            FROM {HEALTH_TABLE}
            ORDER BY cycle_start DESC
            LIMIT {int(limit)}
        """
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=stmt,
            wait_timeout="30s"
        )
        if response.status.state != StatementState.SUCCEEDED:
            return jsonify({"error": "Recent cycles query failed"}), 500

        rows = []
        if response.result and response.result.data_array:
            for r in response.result.data_array:
                cat = r[3] or 'unknown'
                rows.append({
                    "cycle_index": int(r[0]) if r[0] else 0,
                    "cycle_start": r[1],
                    "quality": r[2] or 'Unknown',
                    "category": cat,
                    "confidence": float(r[4]) if r[4] else 0,
                    "cycle_time": float(r[5]) if r[5] else 0,
                    "phases": int(r[6]) if r[6] else 0,
                    "rotation": r[7] or 'unknown',
                    "is_normal": cat == 'normal_operation'
                })
        return jsonify({"data": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/cycle_phase')
def get_cycle_phase():
    """Return current cycle phase for the progress ring."""
    try:
        import re as _re
        stmt = f"""
            SELECT
                payload:cycle_phase::STRING as cycle_phase,
                payload:cycle_index::INT as cycle_index,
                payload:cycling::BOOLEAN as cycling,
                event_time
            FROM {TELEMETRY_TABLE}
            ORDER BY event_time DESC
            LIMIT 1
        """
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=stmt,
            wait_timeout="30s"
        )
        if response.status.state != StatementState.SUCCEEDED:
            return jsonify({"error": "Phase query failed"}), 500

        phases = [
            "raise gripper", "open gripper", "rotate 100 deg to pick",
            "reach table", "hold for block", "close gripper",
            "raise to carry", "rotate 100 deg to drop",
            "reach drop", "open gripper drop", "raise clear"
        ]
        result = {"current_index": -1, "current_phase": "idle", "cycle_index": 0, "cycling": False}

        if response.result and response.result.data_array:
            r = response.result.data_array[0]
            raw_phase = r[0] or ''
            phase_name = _re.sub(r'^(cycle|press) \d+: ', '', raw_phase)
            cycling = r[2] == "true" if r[2] else False
            current = -1
            for i, p in enumerate(phases):
                if p == phase_name:
                    current = i
                    break
            result["current_index"] = current
            result["current_phase"] = phase_name
            result["cycle_index"] = int(r[1]) if r[1] else 0
            result["cycling"] = cycling

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/joint_heatmap')
def get_joint_heatmap():
    """Return per-cycle per-joint deviation z-scores for heatmap."""
    try:
        num_cycles = int(request.args.get('cycles', '10'))
        stmt = f"""
            WITH recent_cycles AS (
                SELECT cycle_index, cycle_start, cycle_end,
                       CAST(cycle_time_sec AS DOUBLE) as ct,
                       cycle_quality, ai_failure_category
                FROM {HEALTH_TABLE}
                WHERE CAST(cycle_time_sec AS DOUBLE) BETWEEN 10 AND 60
                ORDER BY cycle_start DESC
                LIMIT {num_cycles}
            ),
            cycle_joints AS (
                SELECT
                    c.cycle_index, c.cycle_quality, c.ai_failure_category,
                    AVG(t.payload:arms[0].joints[0].position::DOUBLE) as j0,
                    AVG(t.payload:arms[0].joints[1].position::DOUBLE) as j1,
                    AVG(t.payload:arms[0].joints[2].position::DOUBLE) as j2,
                    AVG(t.payload:arms[0].joints[3].position::DOUBLE) as j3,
                    AVG(t.payload:arms[0].joints[4].position::DOUBLE) as j4,
                    AVG(t.payload:arms[0].joints[5].position::DOUBLE) as j5
                FROM recent_cycles c
                JOIN {TELEMETRY_TABLE} t
                    ON t.event_time BETWEEN c.cycle_start AND c.cycle_end
                    AND t.event_time >= (SELECT MIN(cycle_start) FROM recent_cycles)
                GROUP BY c.cycle_index, c.cycle_quality, c.ai_failure_category, c.cycle_start
                ORDER BY c.cycle_start DESC
            ),
            baselines AS (
                SELECT
                    PERCENTILE(j0, 0.5) as m0, COALESCE(NULLIF(STDDEV(j0),0),1) as s0,
                    PERCENTILE(j1, 0.5) as m1, COALESCE(NULLIF(STDDEV(j1),0),1) as s1,
                    PERCENTILE(j2, 0.5) as m2, COALESCE(NULLIF(STDDEV(j2),0),1) as s2,
                    PERCENTILE(j3, 0.5) as m3, COALESCE(NULLIF(STDDEV(j3),0),1) as s3,
                    PERCENTILE(j4, 0.5) as m4, COALESCE(NULLIF(STDDEV(j4),0),1) as s4,
                    PERCENTILE(j5, 0.5) as m5, COALESCE(NULLIF(STDDEV(j5),0),1) as s5
                FROM cycle_joints
            )
            SELECT
                cj.cycle_index, cj.cycle_quality, cj.ai_failure_category,
                ROUND((cj.j0-b.m0)/b.s0,2), ROUND((cj.j1-b.m1)/b.s1,2),
                ROUND((cj.j2-b.m2)/b.s2,2), ROUND((cj.j3-b.m3)/b.s3,2),
                ROUND((cj.j4-b.m4)/b.s4,2), ROUND((cj.j5-b.m5)/b.s5,2)
            FROM cycle_joints cj CROSS JOIN baselines b
        """
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=stmt,
            wait_timeout="30s"
        )
        if response.status.state != StatementState.SUCCEEDED:
            return jsonify({"error": "Heatmap query failed"}), 500

        rows = []
        if response.result and response.result.data_array:
            for r in response.result.data_array:
                rows.append({
                    "cycle_index": int(r[0]) if r[0] else 0,
                    "quality": r[1],
                    "category": r[2],
                    "z": [float(r[i]) if r[i] else 0 for i in range(3, 9)]
                })
        return jsonify({
            "data": rows,
            "joints": ["Shoulder Pan","Shoulder Lift","Elbow Flex","Wrist Flex","Wrist Roll","Gripper"]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/cycle_overlay')
def get_cycle_overlay():
    """Return last N cycles' joint data aligned by cycle start for overlay chart."""
    try:
        num_cycles = int(request.args.get('cycles', '8'))
        stmt = f"""
            WITH recent_cycles AS (
                SELECT cycle_index, cycle_start, cycle_end,
                       CAST(cycle_time_sec AS DOUBLE) as cycle_time_sec,
                       cycle_quality, ai_failure_category
                FROM {HEALTH_TABLE}
                WHERE CAST(cycle_time_sec AS DOUBLE) BETWEEN 10 AND 60
                ORDER BY cycle_start DESC
                LIMIT {num_cycles}
            )
            SELECT
                c.cycle_index,
                c.cycle_quality,
                c.ai_failure_category,
                c.cycle_time_sec,
                ROUND((unix_millis(t.event_time) - unix_millis(c.cycle_start)) / 1000.0, 2) as elapsed_sec,
                ROUND(t.payload:arms[0].joints[0].position::DOUBLE, 1) as shoulder_pan,
                ROUND(t.payload:arms[0].joints[2].position::DOUBLE, 1) as elbow_flex,
                ROUND(t.payload:arms[0].joints[5].position::DOUBLE, 1) as gripper
            FROM recent_cycles c
            JOIN {TELEMETRY_TABLE} t
                ON t.event_time BETWEEN c.cycle_start AND c.cycle_end
                AND t.event_time >= (SELECT MIN(cycle_start) FROM recent_cycles)
            ORDER BY c.cycle_start DESC, t.event_time
        """
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=stmt,
            wait_timeout="30s"
        )
        if response.status.state != StatementState.SUCCEEDED:
            return jsonify({"error": "Cycle overlay query failed"}), 500

        cycles = {}
        if response.result and response.result.data_array:
            for r in response.result.data_array:
                idx = int(r[0])
                if idx not in cycles:
                    cycles[idx] = {
                        "cycle_index": idx,
                        "quality": r[1],
                        "category": r[2],
                        "cycle_time": float(r[3]) if r[3] else 0,
                        "points": []
                    }
                cycles[idx]["points"].append({
                    "t": float(r[4]) if r[4] else 0,
                    "shoulder_pan": float(r[5]) if r[5] else None,
                    "elbow_flex": float(r[6]) if r[6] else None,
                    "gripper": float(r[7]) if r[7] else None
                })
        return jsonify({"data": list(cycles.values())})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/health_score')
def get_health_score():
    """Return real-time health score (0-100) based on cycles in a time window.
    Score = throughput_ratio * avg_cycle_quality * 100.
    Naturally decays to 0 when robot is idle (no cycles in window)."""
    try:
        window_min = int(request.args.get('window', '5'))
        ideal_cycle_sec = 30.0
        expected_cycles = window_min * 60.0 / ideal_cycle_sec

        stmt = f"""
            SELECT
                COUNT(*) as actual_cycles,
                AVG(
                    CASE WHEN ai_failure_category = 'normal_operation' THEN 0.5 ELSE 0.0 END
                    + CASE WHEN cycle_quality = 'Good' THEN 0.2 ELSE 0.0 END
                    + CASE WHEN distinct_phases >= 11 THEN 0.15 ELSE 0.0 END
                    + CASE WHEN CAST(cycle_time_sec AS DOUBLE) BETWEEN 25 AND 35 THEN 0.15 ELSE 0.0 END
                ) as avg_quality,
                SUM(CASE WHEN ai_failure_category = 'normal_operation' THEN 1 ELSE 0 END) as normal_cycles,
                SUM(CASE WHEN ai_failure_category = 'calibration_error' THEN 1 ELSE 0 END) as calibration_errors,
                SUM(CASE WHEN ai_failure_category = 'incomplete_cycle' THEN 1 ELSE 0 END) as incomplete_cycles,
                SUM(CASE WHEN ai_failure_category = 'mechanical_drift' THEN 1 ELSE 0 END) as mechanical_drifts,
                MAX(cycle_start) as last_cycle_time
            FROM {HEALTH_TABLE}
            WHERE cycle_start >= current_timestamp() - INTERVAL {window_min} MINUTES

        """
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=stmt,
            wait_timeout="30s"
        )
        if response.status.state != StatementState.SUCCEEDED:
            return jsonify({"error": "Health score query failed"}), 500

        results = []
        if response.result and response.result.data_array:
            for r in response.result.data_array:
                actual = int(r[0]) if r[0] else 0
                avg_q = float(r[1]) if r[1] else 0
                throughput = min(actual / expected_cycles, 1.0)
                score = round(throughput * avg_q * 100, 1)
                status = "HEALTHY" if score >= 80 else "DEGRADED" if score >= 50 else "CRITICAL"
                results.append({
                    "health_score": score,
                    "health_status": status,
                    "actual_cycles": actual,
                    "expected_cycles": int(expected_cycles),
                    "throughput_pct": round(throughput * 100, 1),
                    "avg_quality_pct": round(avg_q * 100, 1),
                    "normal_cycles": int(r[2]) if r[2] else 0,
                    "calibration_errors": int(r[3]) if r[3] else 0,
                    "incomplete_cycles": int(r[4]) if r[4] else 0,
                    "mechanical_drifts": int(r[5]) if r[5] else 0,
                    "last_cycle_time": r[6],
                    "window_minutes": window_min
                })
        if not results:
            results.append({
                "health_score": 0,
                "health_status": "CRITICAL",
                "actual_cycles": 0,
                "expected_cycles": int(expected_cycles),
                "throughput_pct": 0,
                "avg_quality_pct": 0,
                "normal_cycles": 0,
                "calibration_errors": 0,
                "incomplete_cycles": 0,
                "mechanical_drifts": 0,
                "last_cycle_time": None,
                "window_minutes": window_min
            })
        return jsonify({"data": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    init_schedule_table()
    port = int(os.environ.get("PORT", 8000))
    app.run(host='0.0.0.0', port=port, debug=False)
