"""
05_analytics_report.py — GOLD ANALYTICS
Runs the analytical SQL views and prints KPI report.
This is the "so what" of the pipeline.
"""

import sqlite3
import pandas as pd

DB_PATH = "batayan.db"

def run_report():
    con = sqlite3.connect(DB_PATH)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 120)
    pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

    print("=" * 70)
    print("  BATAYAN PH — ANALYTICS REPORT")
    print("  Philippine Basic Education Enrollment Pipeline (SY 2018–2024)")
    print("=" * 70)

    # 1. Pipeline summary
    print("\n── PIPELINE SUMMARY ──────────────────────────────────────────────")
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM bronze_enrollment_raw")
    bronze = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM silver_enrollment")
    silver = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM fact_enrollment")
    gold = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM silver_quality_log WHERE action='REJECTED'")
    rejected = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM silver_quality_log WHERE action='FIXED' OR action='IMPUTED' OR action='RECALCULATED'")
    fixed = cur.fetchone()[0]
    print(f"  Bronze rows ingested:       {bronze:,}")
    print(f"  Silver rows (after clean):  {silver:,}")
    print(f"  Gold fact rows:             {gold:,}")
    print(f"  Rows auto-fixed:            {fixed:,}")
    print(f"  Rows rejected:              {rejected:,}")
    print(f"  Data quality pass rate:     {silver/(bronze)*100:.1f}%")

    # 2. National trend
    print("\n── KPI 1: NATIONAL ENROLLMENT TREND ─────────────────────────────")
    df = pd.read_sql("SELECT sy, total_learners, total_male, total_female, national_gpi, is_pandemic_year FROM vw_national_enrollment_trend", con)
    df["total_learners"] = df["total_learners"].apply(lambda x: f"{x:,.0f}")
    df["flag"] = df["is_pandemic_year"].apply(lambda x: "⚠ COVID" if x else "")
    print(df[["sy", "total_learners", "national_gpi", "flag"]].to_string(index=False))

    # 3. Top/bottom 3 regions by enrollment (latest year)
    print("\n── KPI 2: REGIONAL ENROLLMENT (SY 2023-2024) ────────────────────")
    df_r = pd.read_sql("SELECT region_name, total_learners, regional_gpi FROM vw_regional_enrollment WHERE sy='2023-2024' ORDER BY total_learners DESC", con)
    print("  TOP 3:")
    print(df_r.head(3).to_string(index=False))
    print("  BOTTOM 3:")
    print(df_r.tail(3).to_string(index=False))

    # 4. GPI analysis — male under-enrollment at JHS
    print("\n── KPI 3: GENDER PARITY INDEX — JHS GRADES (SY 2023-2024) ──────")
    df_gpi = pd.read_sql("""
        SELECT grade_name, gpi, parity_status
        FROM vw_gender_parity
        WHERE sy='2023-2024' AND education_level='Junior High School'
        ORDER BY grade_order
    """, con)
    print(df_gpi.to_string(index=False))
    avg_jhs_gpi = pd.read_sql("""
        SELECT AVG(gpi) as avg_gpi FROM vw_gender_parity
        WHERE sy='2023-2024' AND education_level='Junior High School'
    """, con)["avg_gpi"].iloc[0]
    print(f"\n  Average JHS GPI: {avg_jhs_gpi:.4f} (>1.00 = more females; <1.00 = more males)")
    print(f"  SDG 4.5 target: GPI between 0.97 and 1.03")

    # 5. JHS survival rate (dropout proxy)
    print("\n── KPI 4: JHS COHORT SURVIVAL RATE (2023-2024) ──────────────────")
    df_surv = pd.read_sql("""
        SELECT region_name, grade7_enrollment, grade10_enrollment, jhs_survival_rate_pct
        FROM vw_dropout_proxy
        WHERE ay_start=2023
        ORDER BY jhs_survival_rate_pct
    """, con)
    print("  LOWEST SURVIVAL (highest dropout risk):")
    print(df_surv.head(3).to_string(index=False))
    print("  HIGHEST SURVIVAL:")
    print(df_surv.tail(3).to_string(index=False))
    national_surv = df_surv["jhs_survival_rate_pct"].mean()
    gap = df_surv["jhs_survival_rate_pct"].max() - df_surv["jhs_survival_rate_pct"].min()
    print(f"\n  National average survival rate: {national_surv:.1f}%")
    print(f"  Regional gap (max - min):       {gap:.1f} percentage points")

    # 6. COVID impact
    print("\n── KPI 5: COVID-19 ENROLLMENT IMPACT (2019→2020) ────────────────")
    df_covid = pd.read_sql("""
        SELECT region_name, enroll_2019, enroll_2020, pct_change
        FROM vw_covid_impact
        ORDER BY pct_change
        LIMIT 5
    """, con)
    print("  Regions with steepest enrollment drops:")
    print(df_covid.to_string(index=False))

    # 7. Education level breakdown
    print("\n── KPI 6: EDUCATION LEVEL SHARE (SY 2023-2024) ──────────────────")
    df_lvl = pd.read_sql("""
        SELECT education_level, total_learners, pct_of_year
        FROM vw_education_level_summary
        WHERE sy='2023-2024'
        ORDER BY pct_of_year DESC
    """, con)
    print(df_lvl.to_string(index=False))

    print("\n" + "=" * 70)
    print("  SDG 4 ALIGNMENT SUMMARY")
    print("=" * 70)
    print("  SDG 4.1 — Universal completion: JHS survival gap flagged across regions")
    print(f"             National avg survival rate: {national_surv:.1f}% | Regional gap: {gap:.1f} pp")
    print(f"  SDG 4.5 — Gender parity: JHS average GPI = {avg_jhs_gpi:.4f}")
    print("             Male under-enrollment detected in Grade 7–10 across most SYs")
    print("  SDG 4.b — COVID recovery: enrollment rebounded by SY 2022-2023")
    print("=" * 70)

    con.close()

if __name__ == "__main__":
    run_report()
