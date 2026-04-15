# BatayanPH
### Philippine Basic Education Enrollment Pipeline | Medallion Architecture · SQLite · Python

BatayanPH is a collaborative, end-to-end data engineering pipeline modeled 
after DepEd's Learner Information System (LIS) — the dataset that tracks 
whether millions of Filipino students are still in school or not. The data 
is synthetic, but the structure, the mess, and the questions are real.

The project was built to demonstrate core data engineering fundamentals 
with purpose: taking raw, dirty enrollment data and transforming it into 
a structured analytical foundation that surfaces equity gaps in Philippine 
basic education.

---

## What Was Built

A 5-stage Medallion ETL pipeline in Python and SQL that ingests raw 
enrollment CSVs, cleans them through a Bronze-Silver-Gold architecture, 
loads them into a normalized star schema, and surfaces KPIs through 
analytical SQL views — all reproducible from a single command.

No BI tool did the heavy lifting here. Every transformation, every schema 
decision, and every view was written by hand.

---

## The Numbers That Come Out

| What | Result |
|---|---|
| JHS Gender Parity Index | **0.906** - boys are quietly dropping out at Grades 7-10 |
| National JHS survival rate | **74.5%** - 1 in 4 students never reach Grade 10 |
| Worst vs. best region survival gap | **7.5 percentage points** |
| NCR enrollment drop during COVID | **-11.6%** in SY 2020-2021 |
| Pipeline data quality pass rate | **94.9%** across 4,193 ingested rows |

---

## The Engineering

```
Bronze → Silver → Gold
```

**Bronze** - Raw CSVs loaded as-is into SQLite. Every row gets an MD5 hash, 
source file, and ingestion timestamp. Nothing is touched. This is the audit trail.

**Silver** - The raw data contains 33 different ways of writing region names 
(typos, abbreviations, inconsistent casing). A custom lookup dictionary resolves 
all of them down to the 17 official Philippine administrative regions. Duplicates 
are caught by row hash. Nulls are imputed. Enrollment totals that do not match 
male and female breakdowns are recalculated and flagged in a quality log.

**Gold** - A normalized star schema: 1 fact table, 4 dimension tables, and 6 
analytical SQL views written specifically to answer SDG 4 questions — gender 
parity by grade level, JHS cohort survival by region, COVID enrollment impact, 
and education level share across all school years.

---

## Project Structure

```
BatayanPH/
├── run_pipeline.py              ← Master runner (all 5 stages in sequence)
├── pipeline/
│   ├── 01_generate_raw_data.py  ← Synthetic CSV generation (mirrors LIS structure)
│   ├── 02_bronze_ingest.py      ← Raw ingestion with row hash + lineage tracking
│   ├── 03_silver_transform.py   ← Cleaning, deduplication, standardization
│   ├── 04_gold_build.py         ← Star schema + 6 analytical SQL views
│   └── 05_analytics_report.py  ← Terminal KPI report
```

The pipeline generates `batayan.db` (SQLite) automatically on run. 
Raw CSVs and the database are excluded from version control — 
everything is fully reproducible from `run_pipeline.py`.

---

## Where This Can Go

The Gold layer is built to be extended. The analytical views and star schema 
are ready to plug directly into:

- **Tableau or Power BI** - regional enrollment maps, GPI trend lines, 
  survival rate comparisons by island group
- **Streamlit or Plotly Dash** - a lightweight public-facing dashboard 
  requiring no BI license
- **Python notebooks** - deeper statistical analysis, cohort modeling, 
  or predictive dropout risk scoring

The database schema follows dimensional modeling conventions, so any 
standard BI tool can connect to `batayan.db` and begin building views 
without additional transformation.

---

## On the Data

The pipeline runs on synthetic data that mirrors real LIS structure and 
its known quality problems - dirty region names, duplicate rows, null 
enrollments, and total mismatches. Real LIS microdata requires institutional 
access. Everything this pipeline does to the data, and every question it 
asks, is exactly what it would do on the real thing.

---

## Why SDG 4

1 in 4 Filipino students who enter Grade 7 never finish Grade 10. That 
number is not fabricated - it is what this pipeline surfaces, and it is 
the kind of insight that gets buried in a raw CSV somewhere in a government 
repository. BatayanPH is an attempt to build the infrastructure that makes 
that visible, and to demonstrate that data engineering is most meaningful 
when it is pointed at something that matters.

---

## Contributing

This project is open for collaboration. Whether it is extending the 
pipeline to additional DepEd datasets, connecting the Gold layer to a 
visualization layer, or improving the synthetic data generation to 
better reflect real LIS edge cases - contributions and discussions 
are welcome. Open an issue or submit a pull request.

---

## Stack

Python · SQL · SQLite · Pandas · Medallion Architecture (Bronze/Silver/Gold) · Star Schema

---

## Run It

```bash
pip install pandas numpy
python run_pipeline.py
```

Generates `batayan.db` with the full Bronze, Silver, and Gold layers 
plus a terminal analytics report. Everything rebuilds from scratch on each run.
