"""
04_gold_build.py — GOLD LAYER
Builds a star schema from silver data:
  Dimensions: dim_region, dim_school_type, dim_grade_level, dim_academic_year
  Fact table:  fact_enrollment
Then creates analytical SQL views for KPI reporting.
"""

import sqlite3
import pandas as pd

DB_PATH = "batayan.db"

EDUCATION_LEVEL_MAP = {
    "Kindergarten": "Elementary",
    "Grade 1": "Elementary", "Grade 2": "Elementary", "Grade 3": "Elementary",
    "Grade 4": "Elementary", "Grade 5": "Elementary", "Grade 6": "Elementary",
    "Grade 7": "Junior High School", "Grade 8": "Junior High School",
    "Grade 9": "Junior High School", "Grade 10": "Junior High School",
    "Grade 11": "Senior High School", "Grade 12": "Senior High School",
}

GRADE_ORDER = {
    "Kindergarten": 0, "Grade 1": 1, "Grade 2": 2, "Grade 3": 3,
    "Grade 4": 4, "Grade 5": 5, "Grade 6": 6, "Grade 7": 7,
    "Grade 8": 8, "Grade 9": 9, "Grade 10": 10,
    "Grade 11": 11, "Grade 12": 12,
}

REGION_ISLAND_GROUP = {
    "Region I - Ilocos Region": "Luzon",
    "Region II - Cagayan Valley": "Luzon",
    "Region III - Central Luzon": "Luzon",
    "Region IV-A - CALABARZON": "Luzon",
    "Region IV-B - MIMAROPA": "Luzon",
    "Region V - Bicol Region": "Luzon",
    "NCR - National Capital Region": "Luzon",
    "CAR - Cordillera Administrative Region": "Luzon",
    "Region VI - Western Visayas": "Visayas",
    "Region VII - Central Visayas": "Visayas",
    "Region VIII - Eastern Visayas": "Visayas",
    "Region IX - Zamboanga Peninsula": "Mindanao",
    "Region X - Northern Mindanao": "Mindanao",
    "Region XI - Davao Region": "Mindanao",
    "Region XII - SOCCSKSARGEN": "Mindanao",
    "Region XIII - Caraga": "Mindanao",
    "BARMM - Bangsamoro Autonomous Region": "Mindanao",
}

def build_gold():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.executescript("""
        DROP TABLE IF EXISTS dim_region;
        DROP TABLE IF EXISTS dim_school_type;
        DROP TABLE IF EXISTS dim_grade_level;
        DROP TABLE IF EXISTS dim_academic_year;
        DROP TABLE IF EXISTS fact_enrollment;
        DROP VIEW IF EXISTS vw_national_enrollment_trend;
        DROP VIEW IF EXISTS vw_regional_enrollment;
        DROP VIEW IF EXISTS vw_gender_parity;
        DROP VIEW IF EXISTS vw_education_level_summary;
        DROP VIEW IF EXISTS vw_dropout_proxy;
        DROP VIEW IF EXISTS vw_covid_impact;
    """)
    con.commit()

    # ─── DIMENSION: dim_region ───────────────────────────────────────────────
    df_silver = pd.read_sql("SELECT DISTINCT region_clean FROM silver_enrollment", con)
    regions = df_silver["region_clean"].unique()
    dim_region_rows = [(i+1, r, REGION_ISLAND_GROUP.get(r, "Unknown")) for i, r in enumerate(sorted(regions))]
    cur.execute("CREATE TABLE dim_region (region_id INTEGER PRIMARY KEY, region_name TEXT NOT NULL, island_group TEXT NOT NULL)")
    cur.executemany("INSERT INTO dim_region VALUES (?,?,?)", dim_region_rows)

    # ─── DIMENSION: dim_school_type ──────────────────────────────────────────
    cur.execute("CREATE TABLE dim_school_type (school_type_id INTEGER PRIMARY KEY, school_type TEXT NOT NULL, sector TEXT NOT NULL)")
    cur.executemany("INSERT INTO dim_school_type VALUES (?,?,?)", [
        (1, "Public", "Government"),
        (2, "Private", "Private"),
        (3, "SUCs/LUCs", "Government"),
    ])

    # ─── DIMENSION: dim_grade_level ──────────────────────────────────────────
    grades = list(GRADE_ORDER.keys())
    dim_grade_rows = [(i+1, g, EDUCATION_LEVEL_MAP[g], GRADE_ORDER[g]) for i, g in enumerate(grades)]
    cur.execute("CREATE TABLE dim_grade_level (grade_id INTEGER PRIMARY KEY, grade_name TEXT NOT NULL, education_level TEXT NOT NULL, grade_order INTEGER NOT NULL)")
    cur.executemany("INSERT INTO dim_grade_level VALUES (?,?,?,?)", dim_grade_rows)

    # ─── DIMENSION: dim_academic_year ────────────────────────────────────────
    df_years = pd.read_sql("SELECT DISTINCT sy, ay_start, ay_end FROM silver_enrollment ORDER BY ay_start", con)
    cur.execute("CREATE TABLE dim_academic_year (year_id INTEGER PRIMARY KEY, sy TEXT NOT NULL, ay_start INTEGER NOT NULL, ay_end INTEGER NOT NULL, is_pandemic_year INTEGER NOT NULL)")
    dim_year_rows = []
    for i, row in df_years.iterrows():
        is_pandemic = 1 if row["ay_start"] in [2020, 2021] else 0
        dim_year_rows.append((i+1, row["sy"], int(row["ay_start"]), int(row["ay_end"]), is_pandemic))
    cur.executemany("INSERT INTO dim_academic_year VALUES (?,?,?,?,?)", dim_year_rows)

    # Build lookup dicts
    region_lookup = {r: i+1 for i, r in enumerate(sorted(regions))}
    st_lookup = {"Public": 1, "Private": 2, "SUCs/LUCs": 3}
    grade_lookup = {g: i+1 for i, g in enumerate(grades)}
    year_lookup = {(int(r["ay_start"]), int(r["ay_end"])): i+1 for i, r in df_years.iterrows()}

    # ─── FACT TABLE ──────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE fact_enrollment (
            fact_id           INTEGER PRIMARY KEY AUTOINCREMENT,
            region_id         INTEGER NOT NULL REFERENCES dim_region(region_id),
            school_type_id    INTEGER NOT NULL REFERENCES dim_school_type(school_type_id),
            grade_id          INTEGER NOT NULL REFERENCES dim_grade_level(grade_id),
            year_id           INTEGER NOT NULL REFERENCES dim_academic_year(year_id),
            male_enrollment   INTEGER NOT NULL,
            female_enrollment INTEGER NOT NULL,
            total_enrollment  INTEGER NOT NULL,
            gender_parity_index REAL
        )
    """)

    df_silver_full = pd.read_sql("SELECT * FROM silver_enrollment", con)
    fact_rows = []
    for _, row in df_silver_full.iterrows():
        rid = region_lookup.get(row["region_clean"])
        sid = st_lookup.get(row["school_type_clean"])
        gid = grade_lookup.get(row["grade_level"])
        yid = year_lookup.get((int(row["ay_start"]), int(row["ay_end"])))
        if not all([rid, sid, gid, yid]):
            continue
        male = int(row["male_enrollment"])
        female = int(row["female_enrollment"])
        total = int(row["total_enrollment"])
        gpi = round(female / male, 4) if male > 0 else None
        fact_rows.append((rid, sid, gid, yid, male, female, total, gpi))

    cur.executemany("""
        INSERT INTO fact_enrollment
        (region_id, school_type_id, grade_id, year_id, male_enrollment, female_enrollment, total_enrollment, gender_parity_index)
        VALUES (?,?,?,?,?,?,?,?)
    """, fact_rows)

    con.commit()
    print(f"[GOLD] fact_enrollment: {len(fact_rows):,} rows loaded")

    # ─── ANALYTICAL VIEWS ────────────────────────────────────────────────────
    cur.executescript("""
        -- View 1: National enrollment trend by year
        CREATE VIEW vw_national_enrollment_trend AS
        SELECT
            ay.sy,
            ay.ay_start,
            ay.is_pandemic_year,
            SUM(f.total_enrollment)  AS total_learners,
            SUM(f.male_enrollment)   AS total_male,
            SUM(f.female_enrollment) AS total_female,
            ROUND(CAST(SUM(f.female_enrollment) AS REAL) / SUM(f.male_enrollment), 4) AS national_gpi
        FROM fact_enrollment f
        JOIN dim_academic_year ay ON f.year_id = ay.year_id
        GROUP BY ay.sy, ay.ay_start, ay.is_pandemic_year
        ORDER BY ay.ay_start;

        -- View 2: Regional enrollment by year
        CREATE VIEW vw_regional_enrollment AS
        SELECT
            ay.sy,
            r.region_name,
            r.island_group,
            SUM(f.total_enrollment)  AS total_learners,
            SUM(f.male_enrollment)   AS male_learners,
            SUM(f.female_enrollment) AS female_learners,
            ROUND(CAST(SUM(f.female_enrollment) AS REAL) / SUM(f.male_enrollment), 4) AS regional_gpi
        FROM fact_enrollment f
        JOIN dim_academic_year ay ON f.year_id = ay.year_id
        JOIN dim_region r         ON f.region_id = r.region_id
        GROUP BY ay.sy, r.region_name, r.island_group
        ORDER BY ay.ay_start, total_learners DESC;

        -- View 3: Gender Parity Index by grade level (JHS focus — SDG 4.5)
        CREATE VIEW vw_gender_parity AS
        SELECT
            g.grade_name,
            g.education_level,
            g.grade_order,
            ay.sy,
            SUM(f.male_enrollment)   AS male_learners,
            SUM(f.female_enrollment) AS female_learners,
            ROUND(CAST(SUM(f.female_enrollment) AS REAL) / SUM(f.male_enrollment), 4) AS gpi,
            CASE WHEN CAST(SUM(f.female_enrollment) AS REAL) / SUM(f.male_enrollment) < 0.97
                 THEN 'Male-dominant gap'
                 WHEN CAST(SUM(f.female_enrollment) AS REAL) / SUM(f.male_enrollment) > 1.03
                 THEN 'Female-dominant gap'
                 ELSE 'At parity'
            END AS parity_status
        FROM fact_enrollment f
        JOIN dim_grade_level g    ON f.grade_id = g.grade_id
        JOIN dim_academic_year ay ON f.year_id = ay.year_id
        GROUP BY g.grade_name, g.education_level, g.grade_order, ay.sy
        ORDER BY ay.ay_start, g.grade_order;

        -- View 4: Education level summary
        CREATE VIEW vw_education_level_summary AS
        SELECT
            ay.sy,
            g.education_level,
            SUM(f.total_enrollment)  AS total_learners,
            SUM(f.male_enrollment)   AS male_learners,
            SUM(f.female_enrollment) AS female_learners,
            ROUND(100.0 * SUM(f.total_enrollment) /
                  SUM(SUM(f.total_enrollment)) OVER (PARTITION BY ay.sy), 2) AS pct_of_year
        FROM fact_enrollment f
        JOIN dim_academic_year ay ON f.year_id = ay.year_id
        JOIN dim_grade_level g    ON f.grade_id = g.grade_id
        GROUP BY ay.sy, g.education_level
        ORDER BY ay.ay_start;

        -- View 5: Dropout proxy (Grade 7 vs Grade 10 cohort survival)
        CREATE VIEW vw_dropout_proxy AS
        WITH g7 AS (
            SELECT ay.ay_start, r.region_name, SUM(f.total_enrollment) AS grade7_enrollment
            FROM fact_enrollment f
            JOIN dim_grade_level g    ON f.grade_id = g.grade_id
            JOIN dim_academic_year ay ON f.year_id = ay.year_id
            JOIN dim_region r         ON f.region_id = r.region_id
            WHERE g.grade_name = 'Grade 7'
            GROUP BY ay.ay_start, r.region_name
        ),
        g10 AS (
            SELECT ay.ay_start, r.region_name, SUM(f.total_enrollment) AS grade10_enrollment
            FROM fact_enrollment f
            JOIN dim_grade_level g    ON f.grade_id = g.grade_id
            JOIN dim_academic_year ay ON f.year_id = ay.year_id
            JOIN dim_region r         ON f.region_id = r.region_id
            WHERE g.grade_name = 'Grade 10'
            GROUP BY ay.ay_start, r.region_name
        )
        SELECT
            g7.ay_start, g7.region_name,
            g7.grade7_enrollment,
            g10.grade10_enrollment,
            ROUND(100.0 * g10.grade10_enrollment / g7.grade7_enrollment, 2) AS jhs_survival_rate_pct
        FROM g7 JOIN g10 ON g7.ay_start = g10.ay_start AND g7.region_name = g10.region_name
        ORDER BY g7.ay_start, jhs_survival_rate_pct;

        -- View 6: COVID impact — enrollment drop SY2019-2020 vs SY2020-2021
        -- FIX: uses SUM (total regional enrollment) not MAX (single row value)
        CREATE VIEW vw_covid_impact AS
        SELECT
            r.region_name,
            SUM(CASE WHEN ay.ay_start = 2019 THEN f.total_enrollment END) AS enroll_2019,
            SUM(CASE WHEN ay.ay_start = 2020 THEN f.total_enrollment END) AS enroll_2020,
            ROUND(100.0 *
                (SUM(CASE WHEN ay.ay_start = 2020 THEN f.total_enrollment END) -
                 SUM(CASE WHEN ay.ay_start = 2019 THEN f.total_enrollment END)) /
                 NULLIF(SUM(CASE WHEN ay.ay_start = 2019 THEN f.total_enrollment END), 0), 2
            ) AS pct_change
        FROM fact_enrollment f
        JOIN dim_academic_year ay ON f.year_id = ay.year_id
        JOIN dim_region r         ON f.region_id = r.region_id
        GROUP BY r.region_name
        ORDER BY pct_change;
    """)
    con.commit()
    print("[GOLD] 6 analytical views created")
    con.close()

if __name__ == "__main__":
    build_gold()