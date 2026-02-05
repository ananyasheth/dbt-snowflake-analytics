# dbt + Snowflake Analytics Engineering Project (TPCH)

This repository contains an end-to-end **analytics engineering** project built with **dbt (data build tool)** on **Snowflake**.  
The goal of the project is to demonstrate how raw source tables can be transformed into **analytics-ready marts** using a structured modeling approach (**staging → marts**) with **automated lineage (DAG)**.

---

## What I Built

### 1) Snowflake setup (compute + permissions)
- Created a dedicated Snowflake **warehouse** to run transformations (compute).
- Configured role-based access so dbt can materialize models into a target database/schema.

**Key objects**
- Warehouse: `ANALYTICS_WH`
- Target database: `ANALYTICS_DB`
- Target schemas:
  - `STAGING` (stg_ models)
  - `MARTS` (dim_/fct_ models)

### 2) dbt transformation layer (modular SQL)
- Referenced Snowflake’s TPCH sample data as **dbt sources** (read-only).
- Built standardized **staging models** (`stg_`) to:
  - normalize naming conventions
  - standardize data types/columns
  - create clean, reusable building blocks

### 3) Dimensional modeling (star schema marts)
- Applied **fact and dimensional modeling** to create analytics-ready marts:
  - **Dimensions** (e.g., `dim_customer`)
  - **Facts** (e.g., `fct_orders`)
- This approach produces datasets optimized for downstream reporting and BI.

### 4) Automated DAG & lineage (dbt docs)
- Dependencies were declared using `source()` and `ref()`.
- dbt automatically generated a **DAG / lineage graph** showing how marts trace back to raw sources.

---

## Why dbt (instead of standalone SQL scripts)?

You can absolutely write transformations as SQL scripts directly in a database. dbt adds structure that makes transformations easier to scale and maintain:

- **Modularity**: break logic into reusable models
- **Dependency management**: `ref()` creates explicit upstream/downstream relationships
- **Reproducibility**: transformations are version-controlled and re-runnable
- **Testing**: built-in data quality checks (e.g., `not_null`, `unique`, relationships)
- **Documentation + lineage**: auto-generated docs and DAG from the project metadata

In short: dbt turns “SQL that works” into a maintainable, production-style analytics codebase.

---

## Data Source

This project uses Snowflake’s built-in TPCH sample dataset:

- Database: `SNOWFLAKE_SAMPLE_DATA`
- Schema: `TPCH_SF100` (or another TPCH scale factor)
- Example tables: `CUSTOMER`, `ORDERS`, `LINEITEM`, `NATION`, `REGION`

These are referenced as **dbt sources** (no raw data is modified).

---

## Project Structure

```text
tpch_analytics/
├── dbt_project.yml
├── models/
│   ├── sources/
│   │   └── tpch_sources.yml
│   ├── staging/
│   │   └── tpch/
│   │       ├── stg_customer.sql
│   │       ├── stg_orders.sql
│   │       ├── stg_lineitem.sql
│   │       ├── stg_nation.sql
│   │       └── stg_region.sql
│   └── marts/
│       ├── dim_customer.sql
│       └── fct_orders.sql
└── README.md
