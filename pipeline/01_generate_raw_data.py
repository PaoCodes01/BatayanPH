"""
01_generate_raw_data.py
Generates synthetic raw CSV files that mirror the DepEd LIS enrollment
data structure, using real region names, real enrollment magnitudes,
and intentional data quality issues for the ETL pipeline to handle.
"""

import pandas as pd
import numpy as np
import os
import random

np.random.seed(42)
random.seed(42)

REGIONS = [
    "Region I - Ilocos Region",
    "Region II - Cagayan Valley",
    "Region III - Central Luzon",
    "Region IV-A - CALABARZON",
    "Region IV-B - MIMAROPA",
    "Region V - Bicol Region",
    "Region VI - Western Visayas",
    "Region VII - Central Visayas",
    "Region VIII - Eastern Visayas",
    "Region IX - Zamboanga Peninsula",
    "Region X - Northern Mindanao",
    "Region XI - Davao Region",
    "Region XII - SOCCSKSARGEN",
    "Region XIII - Caraga",
    "NCR - National Capital Region",
    "CAR - Cordillera Administrative Region",
    "BARMM - Bangsamoro Autonomous Region",
]

# Intentionally dirty region name variants (simulate real-world messy data)
DIRTY_REGION_MAP = {
    "Region I - Ilocos Region": ["Region I", "Reg. I - Ilocos", "REGION I - ILOCOS REGION", "Region 1 - Ilocos Region"],
    "Region II - Cagayan Valley": ["Region II", "Reg. II - Cagayan Valley", "REGION II", "Region 2"],
    "Region III - Central Luzon": ["Region III", "Reg. III", "REGION III - CENTRAL LUZON", "Region 3"],
    "Region IV-A - CALABARZON": ["Region IV-A", "Reg. IV-A", "REGION IVA", "Region 4A - CALABARZON", "IVA"],
    "Region IV-B - MIMAROPA": ["Region IV-B", "Reg. IV-B", "MIMAROPA", "Region 4B"],
    "Region V - Bicol Region": ["Region V", "Bicol", "REGION V", "Region 5"],
    "Region VI - Western Visayas": ["Region VI", "Western Visayas", "REGION VI", "Region 6"],
    "Region VII - Central Visayas": ["Region VII", "Central Visayas", "REGION VII", "Region 7"],
    "Region VIII - Eastern Visayas": ["Region VIII", "Eastern Visayas", "REGION VIII", "Region 8"],
    "Region IX - Zamboanga Peninsula": ["Region IX", "Zamboanga", "REGION IX", "Region 9"],
    "Region X - Northern Mindanao": ["Region X", "Northern Mindanao", "REGION X", "Region 10"],
    "Region XI - Davao Region": ["Region XI", "Davao", "REGION XI", "Region 11"],
    "Region XII - SOCCSKSARGEN": ["Region XII", "SOCC", "REGION XII", "Region 12"],
    "Region XIII - Caraga": ["Region XIII", "Caraga", "REGION XIII", "Region 13"],
    "NCR - National Capital Region": ["NCR", "National Capital Region", "Metro Manila", "NCR - Metro Manila"],
    "CAR - Cordillera Administrative Region": ["CAR", "Cordillera", "CAR Region", "Cordillera Administrative Region"],
    "BARMM - Bangsamoro Autonomous Region": ["BARMM", "ARMM", "Bangsamoro", "BARMM Region"],
}

SCHOOL_TYPES = ["Public", "Private", "SUCs/LUCs"]
SCHOOL_TYPE_WEIGHTS = [0.75, 0.20, 0.05]

GRADE_LEVELS = ["Kindergarten", "Grade 1", "Grade 2", "Grade 3", "Grade 4", "Grade 5", "Grade 6",
                "Grade 7", "Grade 8", "Grade 9", "Grade 10", "Grade 11", "Grade 12"]

SY_YEARS = [
    ("2018-2019", 2018, 2019),
    ("2019-2020", 2019, 2020),
    ("2020-2021", 2020, 2021),
    ("2021-2022", 2021, 2022),
    ("2022-2023", 2022, 2023),
    ("2023-2024", 2023, 2024),
]

# Base enrollment per region (public, approximated from DepEd Data Bits reports)
REGION_BASE_ENROLLMENT = {
    "Region I - Ilocos Region": 900000,
    "Region II - Cagayan Valley": 600000,
    "Region III - Central Luzon": 2000000,
    "Region IV-A - CALABARZON": 3900000,
    "Region IV-B - MIMAROPA": 550000,
    "Region V - Bicol Region": 1100000,
    "Region VI - Western Visayas": 1500000,
    "Region VII - Central Visayas": 1600000,
    "Region VIII - Eastern Visayas": 800000,
    "Region IX - Zamboanga Peninsula": 750000,
    "Region X - Northern Mindanao": 950000,
    "Region XI - Davao Region": 1200000,
    "Region XII - SOCCSKSARGEN": 900000,
    "Region XIII - Caraga": 500000,
    "NCR - National Capital Region": 2500000,
    "CAR - Cordillera Administrative Region": 450000,
    "BARMM - Bangsamoro Autonomous Region": 850000,
}

GRADE_SHARE = {
    "Kindergarten": 0.095,
    "Grade 1": 0.105,
    "Grade 2": 0.098,
    "Grade 3": 0.095,
    "Grade 4": 0.088,
    "Grade 5": 0.082,
    "Grade 6": 0.078,
    "Grade 7": 0.075,
    "Grade 8": 0.068,
    "Grade 9": 0.060,
    "Grade 10": 0.055,
    "Grade 11": 0.055,
    "Grade 12": 0.046,
}

def get_dirty_region(clean_region):
    variants = DIRTY_REGION_MAP[clean_region]
    return random.choice(variants)

def generate_enrollment_csv(sy_label, ay_start, ay_end, noise_level=0.05):
    """Generate one year's raw enrollment CSV with intentional messiness."""
    rows = []
    year_growth = 1 + (ay_start - 2018) * 0.012  # slight YoY growth trend

    # COVID dip in 2020-2021
    if ay_start == 2020:
        year_growth *= 0.93
    if ay_start == 2021:
        year_growth *= 0.97

    for region in REGIONS:
        base = REGION_BASE_ENROLLMENT[region] * year_growth
        dirty_region = get_dirty_region(region)

        for school_type in SCHOOL_TYPES:
            type_mult = {"Public": 1.0, "Private": 0.18, "SUCs/LUCs": 0.04}[school_type]

            for grade in GRADE_LEVELS:
                grade_share = GRADE_SHARE[grade]
                total_enrollment = int(base * type_mult * grade_share * np.random.uniform(1 - noise_level, 1 + noise_level))

                # Gender split — slight male bias at JHS level (real pattern)
                if grade in ["Grade 7", "Grade 8", "Grade 9", "Grade 10"]:
                    male_pct = np.random.uniform(0.51, 0.54)
                else:
                    male_pct = np.random.uniform(0.48, 0.52)

                male = int(total_enrollment * male_pct)
                female = total_enrollment - male

                # Inject data quality issues
                row = {
                    "sy": sy_label if random.random() > 0.02 else None,  # 2% null SY
                    "ay_start": ay_start,
                    "ay_end": ay_end,
                    "region": dirty_region,
                    "school_type": school_type if random.random() > 0.01 else school_type.lower(),  # case inconsistency
                    "grade_level": grade,
                    "male_enrollment": male if random.random() > 0.005 else None,  # 0.5% nulls
                    "female_enrollment": female if random.random() > 0.005 else None,
                    "total_enrollment": total_enrollment if random.random() > 0.008 else total_enrollment + random.randint(-500, 500),  # totals mismatch
                }

                # Duplicate rows (5% of rows duplicated — common in real exports)
                rows.append(row)
                if random.random() < 0.05:
                    rows.append(row.copy())

    df = pd.DataFrame(rows)
    # Shuffle rows
    df = df.sample(frac=1).reset_index(drop=True)
    return df

OUT = "data/raw"
os.makedirs(OUT, exist_ok=True)

print("Generating raw CSVs...")
for sy_label, ay_start, ay_end in SY_YEARS:
    df = generate_enrollment_csv(sy_label, ay_start, ay_end)
    fname = f"enrollment_raw_{sy_label.replace('-','_')}.csv"
    fpath = os.path.join(OUT, fname)
    df.to_csv(fpath, index=False)
    print(f"  {fname}: {len(df):,} rows, {df['total_enrollment'].sum():,.0f} total learners")

print("\nDone. Raw data ready in data/raw/")
