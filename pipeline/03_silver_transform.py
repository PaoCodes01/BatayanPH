"""
03_silver_transform.py — SILVER LAYER
Cleans and standardizes bronze data:
- Standardize dirty region names → 17 official regions
- Normalize school_type casing
- Impute NULL sy from ay_start/ay_end
- Cast numeric columns
- Recalculate total where male+female don't match
- Deduplicate using row_hash
- Log all rejected/fixed rows with reason
"""

import sqlite3
import pandas as pd

DB_PATH = "batayan.db"

REGION_LOOKUP = {
    # Region I
    "Region I": "Region I - Ilocos Region",
    "Reg. I - Ilocos": "Region I - Ilocos Region",
    "REGION I - ILOCOS REGION": "Region I - Ilocos Region",
    "Region 1 - Ilocos Region": "Region I - Ilocos Region",
    # Region II
    "Region II": "Region II - Cagayan Valley",
    "Reg. II - Cagayan Valley": "Region II - Cagayan Valley",
    "REGION II": "Region II - Cagayan Valley",
    "Region 2": "Region II - Cagayan Valley",
    # Region III
    "Region III": "Region III - Central Luzon",
    "Reg. III": "Region III - Central Luzon",
    "REGION III - CENTRAL LUZON": "Region III - Central Luzon",
    "Region 3": "Region III - Central Luzon",
    # Region IV-A
    "Region IV-A": "Region IV-A - CALABARZON",
    "Reg. IV-A": "Region IV-A - CALABARZON",
    "REGION IVA": "Region IV-A - CALABARZON",
    "Region 4A - CALABARZON": "Region IV-A - CALABARZON",
    "IVA": "Region IV-A - CALABARZON",
    # Region IV-B
    "Region IV-B": "Region IV-B - MIMAROPA",
    "Reg. IV-B": "Region IV-B - MIMAROPA",
    "MIMAROPA": "Region IV-B - MIMAROPA",
    "Region 4B": "Region IV-B - MIMAROPA",
    # Region V
    "Region V": "Region V - Bicol Region",
    "Bicol": "Region V - Bicol Region",
    "REGION V": "Region V - Bicol Region",
    "Region 5": "Region V - Bicol Region",
    # Region VI
    "Region VI": "Region VI - Western Visayas",
    "Western Visayas": "Region VI - Western Visayas",
    "REGION VI": "Region VI - Western Visayas",
    "Region 6": "Region VI - Western Visayas",
    # Region VII
    "Region VII": "Region VII - Central Visayas",
    "Central Visayas": "Region VII - Central Visayas",
    "REGION VII": "Region VII - Central Visayas",
    "Region 7": "Region VII - Central Visayas",
    # Region VIII
    "Region VIII": "Region VIII - Eastern Visayas",
    "Eastern Visayas": "Region VIII - Eastern Visayas",
    "REGION VIII": "Region VIII - Eastern Visayas",
    "Region 8": "Region VIII - Eastern Visayas",
    # Region IX
    "Region IX": "Region IX - Zamboanga Peninsula",
    "Zamboanga": "Region IX - Zamboanga Peninsula",
    "REGION IX": "Region IX - Zamboanga Peninsula",
    "Region 9": "Region IX - Zamboanga Peninsula",
    # Region X
    "Region X": "Region X - Northern Mindanao",
    "Northern Mindanao": "Region X - Northern Mindanao",
    "REGION X": "Region X - Northern Mindanao",
    "Region 10": "Region X - Northern Mindanao",
    # Region XI
    "Region XI": "Region XI - Davao Region",
    "Davao": "Region XI - Davao Region",
    "REGION XI": "Region XI - Davao Region",
    "Region 11": "Region XI - Davao Region",
    # Region XII
    "Region XII": "Region XII - SOCCSKSARGEN",
    "SOCC": "Region XII - SOCCSKSARGEN",
    "REGION XII": "Region XII - SOCCSKSARGEN",
    "Region 12": "Region XII - SOCCSKSARGEN",
    # Region XIII
    "Region XIII": "Region XIII - Caraga",
    "Caraga": "Region XIII - Caraga",
    "REGION XIII": "Region XIII - Caraga",
    "Region 13": "Region XIII - Caraga",
    # NCR
    "NCR": "NCR - National Capital Region",
    "National Capital Region": "NCR - National Capital Region",
    "Metro Manila": "NCR - National Capital Region",
    "NCR - Metro Manila": "NCR - National Capital Region",
    # CAR
    "CAR": "CAR - Cordillera Administrative Region",
    "Cordillera": "CAR - Cordillera Administrative Region",
    "CAR Region": "CAR - Cordillera Administrative Region",
    "Cordillera Administrative Region": "CAR - Cordillera Administrative Region",
    # BARMM
    "BARMM": "BARMM - Bangsamoro Autonomous Region",
    "ARMM": "BARMM - Bangsamoro Autonomous Region",
    "Bangsamoro": "BARMM - Bangsamoro Autonomous Region",
    "BARMM Region": "BARMM - Bangsamoro Autonomous Region",
}

SCHOOL_TYPE_LOOKUP = {
    "public": "Public",
    "private": "Private",
    "sucs/lucs": "SUCs/LUCs",
    "Public": "Public",
    "Private": "Private",
    "SUCs/LUCs": "SUCs/LUCs",
}

def transform_silver():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Create silver tables
    cur.executescript("""
        DROP TABLE IF EXISTS silver_enrollment;
        DROP TABLE IF EXISTS silver_quality_log;

        CREATE TABLE silver_enrollment (
            silver_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            bronze_id         INTEGER NOT NULL,
            sy                TEXT    NOT NULL,
            ay_start          INTEGER NOT NULL,
            ay_end            INTEGER NOT NULL,
            region_clean      TEXT    NOT NULL,
            school_type_clean TEXT    NOT NULL,
            grade_level       TEXT    NOT NULL,
            male_enrollment   INTEGER NOT NULL,
            female_enrollment INTEGER NOT NULL,
            total_enrollment  INTEGER NOT NULL,
            total_recalculated INTEGER DEFAULT 0,
            processed_at      TEXT    NOT NULL
        );

        CREATE TABLE silver_quality_log (
            log_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            bronze_id   INTEGER,
            issue_type  TEXT,
            original_value TEXT,
            resolved_value TEXT,
            action      TEXT
        );
    """)
    con.commit()

    df = pd.read_sql("SELECT * FROM bronze_enrollment_raw", con)
    print(f"Bronze rows loaded: {len(df):,}")

    stats = {"null_sy_imputed": 0, "region_standardized": 0,
             "school_type_fixed": 0, "total_recalculated": 0,
             "duplicates_dropped": 0, "rows_rejected": 0, "rows_clean": 0}

    from datetime import datetime
    now = datetime.utcnow().isoformat()

    # --- Deduplication ---
    before = len(df)
    df = df.drop_duplicates(subset="row_hash", keep="first")
    stats["duplicates_dropped"] = before - len(df)
    print(f"Duplicates dropped: {stats['duplicates_dropped']:,}")

    quality_log_rows = []
    silver_rows = []
    rejected = 0

    for _, row in df.iterrows():
        bronze_id = row["bronze_id"]
        issues = []

        # --- SY imputation ---
        sy = row["sy"]
        ay_start = row["ay_start"]
        ay_end = row["ay_end"]
        if pd.isna(sy) and not pd.isna(ay_start) and not pd.isna(ay_end):
            sy = f"{int(float(ay_start))}-{int(float(ay_end))}"
            issues.append(("NULL_SY_IMPUTED", None, sy, "IMPUTED"))
            stats["null_sy_imputed"] += 1
        if pd.isna(sy) or pd.isna(ay_start) or pd.isna(ay_end):
            quality_log_rows.append((bronze_id, "MISSING_YEAR", str(sy), None, "REJECTED"))
            rejected += 1
            continue

        ay_start = int(float(ay_start))
        ay_end = int(float(ay_end))

        # --- Region standardization ---
        region_raw = str(row["region"]).strip() if not pd.isna(row["region"]) else None
        if not region_raw:
            quality_log_rows.append((bronze_id, "NULL_REGION", None, None, "REJECTED"))
            rejected += 1
            continue
        region_clean = REGION_LOOKUP.get(region_raw)
        if not region_clean:
            quality_log_rows.append((bronze_id, "UNKNOWN_REGION", region_raw, None, "REJECTED"))
            rejected += 1
            continue
        if region_raw != region_clean:
            issues.append(("REGION_STANDARDIZED", region_raw, region_clean, "FIXED"))
            stats["region_standardized"] += 1

        # --- School type normalization ---
        st_raw = str(row["school_type"]).strip() if not pd.isna(row["school_type"]) else None
        school_type_clean = SCHOOL_TYPE_LOOKUP.get(st_raw)
        if not school_type_clean:
            quality_log_rows.append((bronze_id, "UNKNOWN_SCHOOL_TYPE", st_raw, None, "REJECTED"))
            rejected += 1
            continue
        if st_raw != school_type_clean:
            issues.append(("SCHOOL_TYPE_FIXED", st_raw, school_type_clean, "FIXED"))
            stats["school_type_fixed"] += 1

        # --- Grade level ---
        grade_level = str(row["grade_level"]).strip() if not pd.isna(row["grade_level"]) else None
        if not grade_level:
            quality_log_rows.append((bronze_id, "NULL_GRADE", None, None, "REJECTED"))
            rejected += 1
            continue

        # --- Numeric casting ---
        def safe_int(val):
            try:
                return int(float(val)) if not pd.isna(val) else None
            except:
                return None

        male = safe_int(row["male_enrollment"])
        female = safe_int(row["female_enrollment"])
        total = safe_int(row["total_enrollment"])

        # Impute nulls
        if male is None and female is not None and total is not None:
            male = total - female
        if female is None and male is not None and total is not None:
            female = total - male
        if total is None and male is not None and female is not None:
            total = male + female

        if male is None or female is None or total is None:
            quality_log_rows.append((bronze_id, "NULL_ENROLLMENT", None, None, "REJECTED"))
            rejected += 1
            continue

        # Reject negatives
        if male < 0 or female < 0 or total < 0:
            quality_log_rows.append((bronze_id, "NEGATIVE_ENROLLMENT", str(total), None, "REJECTED"))
            rejected += 1
            continue

        # Recalculate total if mismatch > 5
        total_recalculated = 0
        if abs((male + female) - total) > 5:
            orig = total
            total = male + female
            issues.append(("TOTAL_MISMATCH", str(orig), str(total), "RECALCULATED"))
            stats["total_recalculated"] += 1
            total_recalculated = 1

        for issue in issues:
            quality_log_rows.append((bronze_id,) + issue)

        silver_rows.append((
            bronze_id, str(sy), ay_start, ay_end,
            region_clean, school_type_clean, grade_level,
            male, female, total, total_recalculated, now
        ))
        stats["rows_clean"] += 1

    stats["rows_rejected"] = rejected

    # Bulk insert
    cur.executemany("""
        INSERT INTO silver_enrollment
        (bronze_id, sy, ay_start, ay_end, region_clean, school_type_clean,
         grade_level, male_enrollment, female_enrollment, total_enrollment,
         total_recalculated, processed_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, silver_rows)

    cur.executemany("""
        INSERT INTO silver_quality_log (bronze_id, issue_type, original_value, resolved_value, action)
        VALUES (?,?,?,?,?)
    """, quality_log_rows)

    con.commit()
    con.close()

    print(f"\n=== SILVER LAYER QUALITY REPORT ===")
    print(f"  Duplicates dropped:      {stats['duplicates_dropped']:,}")
    print(f"  NULL SY imputed:         {stats['null_sy_imputed']:,}")
    print(f"  Regions standardized:    {stats['region_standardized']:,}")
    print(f"  School type fixed:       {stats['school_type_fixed']:,}")
    print(f"  Totals recalculated:     {stats['total_recalculated']:,}")
    print(f"  Rows rejected:           {stats['rows_rejected']:,}")
    print(f"  Rows passed to Silver:   {stats['rows_clean']:,}")
    total_in = stats['rows_clean'] + stats['rows_rejected'] + stats['duplicates_dropped']
    pct_clean = stats['rows_clean'] / (total_in) * 100 if total_in else 0
    print(f"  Data quality pass rate:  {pct_clean:.1f}%")

if __name__ == "__main__":
    transform_silver()
