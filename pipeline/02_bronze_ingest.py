"""
02_bronze_ingest.py — BRONZE LAYER
Raw ingestion: load all CSVs as-is into bronze schema.
No transformations. Schema enforced, nothing else.
Tracks source file, ingestion timestamp, row hash for lineage.
"""

import pandas as pd
import sqlite3
import hashlib
import os
import glob
from datetime import datetime

DB_PATH = "batayan.db"
RAW_DIR = "data/raw"

def row_hash(row_dict):
    s = str(sorted(row_dict.items()))
    return hashlib.md5(s.encode()).hexdigest()

def ingest_bronze():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS bronze_enrollment_raw (
            bronze_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file       TEXT    NOT NULL,
            ingested_at       TEXT    NOT NULL,
            row_hash          TEXT    NOT NULL,
            sy                TEXT,
            ay_start          INTEGER,
            ay_end            INTEGER,
            region            TEXT,
            school_type       TEXT,
            grade_level       TEXT,
            male_enrollment   REAL,
            female_enrollment REAL,
            total_enrollment  REAL
        );
        CREATE TABLE IF NOT EXISTS bronze_ingestion_log (
            log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file     TEXT    NOT NULL,
            ingested_at     TEXT    NOT NULL,
            rows_ingested   INTEGER,
            status          TEXT
        );
    """)
    con.commit()

    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))
    print(f"Found {len(files)} raw files to ingest\n")

    total_rows = 0
    for fpath in files:
        fname = os.path.basename(fpath)
        ingested_at = datetime.utcnow().isoformat()
        df = pd.read_csv(fpath, dtype=str)
        df.columns = [c.strip().lower() for c in df.columns]

        rows_to_insert = []
        for _, row in df.iterrows():
            d = row.to_dict()
            h = row_hash(d)
            rows_to_insert.append((
                fname, ingested_at, h,
                d.get("sy"), d.get("ay_start"), d.get("ay_end"),
                d.get("region"), d.get("school_type"), d.get("grade_level"),
                d.get("male_enrollment"), d.get("female_enrollment"), d.get("total_enrollment")
            ))

        cur.executemany("""
            INSERT INTO bronze_enrollment_raw
            (source_file, ingested_at, row_hash, sy, ay_start, ay_end,
             region, school_type, grade_level, male_enrollment, female_enrollment, total_enrollment)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows_to_insert)

        cur.execute("""
            INSERT INTO bronze_ingestion_log (source_file, ingested_at, rows_ingested, status)
            VALUES (?, ?, ?, 'SUCCESS')
        """, (fname, ingested_at, len(rows_to_insert)))

        con.commit()
        total_rows += len(rows_to_insert)
        print(f"  [BRONZE] {fname}: {len(rows_to_insert):,} rows ingested")

    print(f"\nTotal bronze rows: {total_rows:,}")
    cur.execute("SELECT COUNT(*) FROM bronze_enrollment_raw")
    print(f"Verified in DB:    {cur.fetchone()[0]:,} rows in bronze_enrollment_raw")
    cur.execute("SELECT COUNT(*) FROM bronze_enrollment_raw WHERE region IS NULL OR total_enrollment IS NULL")
    print(f"Null check:        {cur.fetchone()[0]:,} rows with NULL region or total_enrollment (expected pre-Silver)")
    con.close()

if __name__ == "__main__":
    ingest_bronze()
