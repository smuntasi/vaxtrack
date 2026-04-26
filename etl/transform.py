import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

EXPECTED_FIELDS = [
    "survey_year", "geography", "vaccine", "dose",
    "coverage_estimate", "ci_95_wide_lower_bound",
    "ci_95_wide_upper_bound", "sample_size",
    "dimension_type", "dimension", "_data_as_of"
]

def transform(records: list[dict]) -> pd.DataFrame:
    """
    Clean and normalize raw CDC vaccination records.
    - Casts types (strings -> numbers, dates)
    - Drops duplicates and invalid rows
    - Renames columns to snake_case
    - Adds a derived coverage_category column
    """
    logger.info(f"Starting transform on {len(records)} records...")

    df = pd.DataFrame(records)

    # Drop columns we don't need
    df = df[[col for col in EXPECTED_FIELDS if col in df.columns]]

    # Rename _data_as_of to data_as_of
    df = df.rename(columns={"_data_as_of": "data_as_of"})

    # Cast numeric columns
    for col in ["coverage_estimate", "ci_95_wide_lower_bound", "ci_95_wide_upper_bound"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["sample_size"] = pd.to_numeric(df["sample_size"], errors="coerce").astype("Int64")
    df["survey_year"] = pd.to_numeric(df["survey_year"], errors="coerce").astype("Int64")

    # Parse dates
    df["data_as_of"] = pd.to_datetime(df["data_as_of"], errors="coerce")

    # Drop rows missing critical fields
    before = len(df)
    df = df.dropna(subset=["coverage_estimate", "geography", "vaccine", "survey_year"])
    dropped = before - len(df)
    if dropped:
        logger.warning(f"Dropped {dropped} rows with missing critical fields.")

    # Drop duplicates
    df = df.drop_duplicates()

    # Derived column: bucket coverage into Low / Moderate / High
    df["coverage_category"] = pd.cut(
        df["coverage_estimate"],
        bins=[0, 75, 90, 100],
        labels=["Low", "Moderate", "High"]
    )

    logger.info(f"Transform complete. {len(df)} clean records ready to load.")
    return df


if __name__ == "__main__":
    from extract import extract

    records = extract(max_records=3000, use_synthetic=True)
    df = transform(records)

    print("\nDataFrame info:")
    print(df.dtypes)
    print(f"\nShape: {df.shape}")
    print(f"\nCoverage category breakdown:")
    print(df["coverage_category"].value_counts())
    print(f"\nSample rows:")
    print(df.head(3).to_string())