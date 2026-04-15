"""
run_pipeline.py — Master runner for BatayanPH ETL pipeline
Runs all 5 stages in order.
"""
import os
import sys
import importlib.util

# ── Path setup ────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

DB_PATH = os.path.join(BASE_DIR, "batayan.db")
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print("Cleared existing database.\n")

def load(filename):
    path = os.path.join(BASE_DIR, "pipeline", filename)
    spec = importlib.util.spec_from_file_location(filename, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

# ── Stage 1: Generate raw CSVs ────────────────────────────────────────────────
print("STAGE 1/5: Generating raw data...")
load("01_generate_raw_data.py")          # no guard — runs on import

# ── Stage 2: Bronze ingestion ─────────────────────────────────────────────────
print("\nSTAGE 2/5: Bronze ingestion...")
load("02_bronze_ingest.py").ingest_bronze()

# ── Stage 3: Silver transformation ───────────────────────────────────────────
print("\nSTAGE 3/5: Silver transformation...")
load("03_silver_transform.py").transform_silver()

# ── Stage 4: Gold layer + views ───────────────────────────────────────────────
print("\nSTAGE 4/5: Gold layer + analytical views...")
load("04_gold_build.py").build_gold()

# ── Stage 5: Analytics report ─────────────────────────────────────────────────
print("\n" + "=" * 50)
print("STAGE 5/5: Analytics report...")
load("05_analytics_report.py").run_report()