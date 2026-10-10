# FE Bar — Robot Gripper Monitoring Platform

End-to-end robotics monitoring solution for SO101 robot arm gripper operations,
built on Databricks Lakehouse.

## Repository Structure

```
febar/
├── app/                          # Databricks App — Gripper Maintenance Monitor
│   ├── app.py                    # Flask backend (APIs for telemetry, OEE, schedules)
│   ├── app.yaml                  # App deployment config
│   ├── requirements.txt          # Python dependencies
│   └── static/
│       ├── dashboard.html        # Light-theme React dashboard (production)
│       └── index.html            # Dark-theme React dashboard (legacy)
│
├── pipelines/
│   ├── gripper_cycle_count/      # Pipeline 1: Gripper Cycle Count Pipeline
│   │   └── transformations/
│   │       ├── bronze_gripper_events.sql
│   │       ├── bronze_gripper_all_events.sql
│   │       ├── silver_gripper_cycle_counts.sql
│   │       └── gold_gripper_operational_metrics.sql
│   │
│   └── cycle_ai_classification/  # Pipeline 2: Cycle AI Classification Pipeline
│       └── transformations/
│           ├── bronze_cycle_events.sql       # (deprecated)
│           ├── bronze_all_events.sql         # (deprecated)
│           ├── silver_cycle_metrics.sql      # Deterministic cycle metrics + shift tagging
│           ├── silver_cycle_classified.sql   # AI classification via ai_classify
│           └── gold_cycle_classifications.sql # OEE KPIs with AI quality scoring
│
├── notebooks/
│   └── SO101_Drop_Quality_ML_Model.ipynb  # ML model: Random Forest drop quality prediction
│
└── dashboards/
    └── Gripper_OEE_Dashboard.lvdash.json  # AI/BI Dashboard: OEE metrics visualization
```

## Components

### Gripper Maintenance Alert App
Flask-based monitoring dashboard with real-time telemetry, OEE gauges,
AI defect classification, and Lakebase-backed shift scheduling.

### Lakeflow Spark Declarative Pipelines
- **Gripper Cycle Count**: Bronze→Silver→Gold pipeline computing per-cycle
  performance metrics and OEE from `so101_joints` streaming data.
- **Cycle AI Classification**: Reads `so101_snapshots`, applies `ai_classify`
  for failure categorization, and produces per-arm per-shift OEE KPIs.

### ML Model
Random Forest classifier for drop quality prediction (96.8% accuracy, AUC 0.997).
Trained on 66 aggregate features from 6 joint sensors.

### Dashboard
Lakeview dashboard with OEE gauges, cycle quality breakdowns, and hourly trend charts.

## Data Sources
- `robot_demo_catalog.robot_demo.so101_joints` — IoT joint telemetry
- `robot_demo_catalog.robot_demo.so101_snapshots` — Full payload snapshots
- `robot_demo_catalog.robot_demo.lb_shift_schedule_history` — Lakebase CDF shift data

## Execution Evidence

Every artifact in this repo includes proof of execution for FE bar AI evaluation:

### Notebook (`notebooks/SO101_Drop_Quality_ML_Model.ipynb`)
- Cell outputs embedded: dataset shape, model comparison metrics, feature importance chart (PNG)
- MLflow experiment links to logged models
- Results: Random Forest 96.8% accuracy, AUC 0.997, perfect precision

### Pipeline SQL (`pipelines/`)
- Each `.sql` file has an `EXECUTION EVIDENCE` header with:
  - Pipeline ID and update status
  - Output table name, row count, and column count
  - Key data statistics (e.g., AI classification distribution, OEE ranges)
  - Deprecated files explicitly marked with reason

### App (`app/`)
- 20 successful deployments, latest: 2026-10-08T23:45:03Z
- Live at: https://gripper-maintenance-alert-7474645654545438.aws.databricksapps.com
- Compute status: ACTIVE

### Dashboard (`dashboards/`)
- Full Lakeview JSON with 2 datasets, 5 widgets, 11 measures
- Widget definitions include query specs, encodings, and layout positions

### Verification Report (`evidence/execution_verification.ipynb`)
- Standalone notebook with embedded query outputs proving:
  - Pipeline tables exist and contain data (1,034 cycles)
  - Sample data from silver and gold tables
  - AI classification distribution across all 4 categories
  - App deployment history and status
  - Dashboard widget inventory
