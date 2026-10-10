# Genie Space: Robotics Operational Performance

AI/BI Genie agent for querying robotic arm OEE and cycle performance data
via natural language.

## Configuration

- **Space ID:** `01f1c491058d10f1a5fdbb95f67c1fdf`
- **Run-as:** VIEWER
- **Warehouse:** Serverless Starter Warehouse

## Tables

| Table | Purpose |
| --- | --- |
| `silver_cycle_metrics_v2` | Cycle-level detail: times, AI classification, quality |
| `gold_operational_kpis` | Aggregate OEE per arm/shift/day |

## Entity Matching (Value Indexing)

Enabled on categorical columns for accurate filtering:
- `ai_failure_category`, `cycle_quality`, `rotation_type`, `shift_name`, `arm_serial`

## SQL Examples (4)

1. **OEE for each shift today** — gold table, filtered to CURRENT_DATE()
2. **Top failure categories** — silver table, grouped by ai_failure_category
3. **Average cycle time by shift** — silver table, grouped by shift_name
4. **Defective cycles with AI rationale** — silver table, filtered to cycle_quality='Bad'

## Instructions Summary

Detailed natural language instructions guide the agent on:
- OEE formula (Availability × Performance × Quality)
- Table selection guidance (gold for aggregates, silver for drill-downs)
- Column semantics (shift values, failure categories, rotation types)
- Join keys (arm_serial + shift_name)

See `space_config.json` for the complete configuration.
