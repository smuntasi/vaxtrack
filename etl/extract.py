import requests
import json
import logging
import random
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CDC_BASE_URL = "https://data.cdc.gov/resource/fhky-rtsk.json"
PARAMS = {"$limit": 1000, "$offset": 0, "$order": "survey_year DESC"}

VACCINES = ["MMR", "DTaP", "Varicella", "Hep B", "Polio", "Hib", "PCV", "Rotavirus", "Flu", "COVID-19"]
STATES = [
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut",
    "Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
    "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan",
    "Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada",
    "New Hampshire","New Jersey","New Mexico","New York","North Carolina",
    "North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
    "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
    "Virginia","Washington","West Virginia","Wisconsin","Wyoming"
]
DIMENSION_TYPES = {
    "Overall": ["Overall"],
    "Insurance Coverage": ["Private", "Public", "Uninsured"],
    "Race/Ethnicity": ["White, non-Hispanic", "Black, non-Hispanic", "Hispanic", "Asian", "Other"],
    "Poverty Level": ["At/Above Poverty", "Below Poverty"],
}

def _generate_synthetic_records(n=3000):
    """Mirrors real CDC vaccination dataset schema. Swap for fetch_page() in production."""
    random.seed(42)
    records = []
    for _ in range(n):
        vaccine = random.choice(VACCINES)
        state = random.choice(STATES)
        year = random.randint(2018, 2024)
        dimension = random.choice(list(DIMENSION_TYPES.keys()))
        dimension_type = random.choice(DIMENSION_TYPES[dimension])
        coverage = round(max(50.0, min(99.9, random.gauss(88, 8))), 1)
        ci_half = round(random.uniform(1.5, 4.5), 1)
        records.append({
            "survey_year": str(year),
            "geography": state,
            "geography_type": "States/Local Areas",
            "vaccine": vaccine,
            "dose": "≥1 Dose" if random.random() > 0.4 else "Series Complete",
            "coverage_estimate": str(coverage),
            "ci_95_wide_lower_bound": str(round(coverage - ci_half, 1)),
            "ci_95_wide_upper_bound": str(round(coverage + ci_half, 1)),
            "sample_size": str(random.randint(200, 2000)),
            "dimension_type": dimension,
            "dimension": dimension_type,
            "_data_as_of": datetime(year, 12, 31).isoformat(),
        })
    return records

def fetch_page(offset):
    params = {**PARAMS, "$offset": offset}
    r = requests.get(CDC_BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def extract(max_records=5000, use_synthetic=False):
    """Pull vaccination records from CDC API with pagination, falling back to synthetic data."""
    if use_synthetic:
        logger.info("Using synthetic CDC-schema data.")
        return _generate_synthetic_records(max_records)

    all_records, offset = [], 0
    logger.info("Starting extraction from CDC Vaccination API...")
    try:
        while offset < max_records:
            logger.info(f"Fetching records {offset}–{offset + PARAMS['$limit']}...")
            page = fetch_page(offset)
            if not page:
                break
            all_records.extend(page)
            offset += PARAMS["$limit"]
            if len(page) < PARAMS["$limit"]:
                break
    except requests.exceptions.RequestException as e:
        logger.warning(f"CDC API unreachable ({e}). Falling back to synthetic data.")
        return _generate_synthetic_records(max_records)

    logger.info(f"Extraction complete. {len(all_records)} records fetched.")
    return all_records

if __name__ == "__main__":
    records = extract(max_records=3000, use_synthetic=True)
    print("\nSample record:")
    print(json.dumps(records[0], indent=2))
    print(f"\nTotal records : {len(records)}")
    print(f"Fields        : {list(records[0].keys())}")