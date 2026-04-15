# BatayanPH - Medallion ETL Pipeline for Philippine Education Analytics

## Project Structure
```
BatayanPH/
├── run_pipeline.py              ← Master runner (runs all stages)
├── batayan.db                   ← SQLite database (Bronze + Silver + Gold)
├── pipeline/
│   ├── 01_generate_raw_data.py  ← Synthetic raw CSV generation
│   ├── 02_bronze_ingest.py      ← Bronze: raw ingestion + lineage tracking
│   ├── 03_silver_transform.py   ← Silver: cleaning, deduplication, standardization
│   ├── 04_gold_build.py         ← Gold: star schema + 6 analytical SQL views
│   └── 05_analytics_report.py  ← KPI report output
```

## Medallion Architecture
- **Bronze**: Raw CSV data ingested as-is with row hash, source file, timestamp
- **Silver**: Cleaned - region names standardized (33→17 variants), deduped, nulls imputed, totals validated
- **Gold**: Star schema (1 fact + 4 dims) + 6 analytical views for SDG 4 KPIs

## Key Findings
- JHS Gender Parity Index: 0.906 (male under-enrollment, Grades 7–10)
- National JHS survival rate: 74.5% | Regional gap: 7.5 pp
- COVID dip: NCR enrollment dropped 11.6% in SY 2020-2021
- Data quality pass rate: 94.9% (215 duplicates removed, 37 totals recalculated)

## SDG Alignment
- SDG 4.1 - Completion: JHS cohort survival monitoring
- SDG 4.5 - Gender parity: GPI tracking by grade level
- SDG 4.b - Recovery: COVID impact vs enrollment rebound

## To Run
```bash
pip install pandas numpy sqlalchemy
python run_pipeline.py
```
