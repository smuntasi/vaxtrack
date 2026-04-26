import logging
from sqlalchemy import create_engine, text
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_URL = "postgresql://postgres:postgres@localhost:5432/vaxtrack"


def get_engine():
    return create_engine(DB_URL)


def create_table(engine):
    """Create the vaccination_records table if it doesn't exist."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS vaccination_records (
                id                  SERIAL PRIMARY KEY,
                survey_year         INTEGER,
                geography           VARCHAR(100),
                vaccine             VARCHAR(100),
                dose                VARCHAR(50),
                coverage_estimate   NUMERIC(5,2),
                ci_lower            NUMERIC(5,2),
                ci_upper            NUMERIC(5,2),
                sample_size         INTEGER,
                dimension_type      VARCHAR(100),
                dimension           VARCHAR(100),
                data_as_of          DATE,
                coverage_category   VARCHAR(20),
                inserted_at         TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_vaccine     ON vaccination_records(vaccine);
            CREATE INDEX IF NOT EXISTS idx_geography   ON vaccination_records(geography);
            CREATE INDEX IF NOT EXISTS idx_survey_year ON vaccination_records(survey_year);
        """))
        conn.commit()
    logger.info("Table ready.")


def load(df: pd.DataFrame):
    """Load cleaned DataFrame into PostgreSQL."""
    engine = get_engine()
    create_table(engine)

    # Rename columns to match DB schema
    df = df.rename(columns={
        "ci_95_wide_lower_bound": "ci_lower",
        "ci_95_wide_upper_bound": "ci_upper",
    })

    # Convert coverage_category to string for DB
    df["coverage_category"] = df["coverage_category"].astype(str)

    df.to_sql(
        name="vaccination_records",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )

    logger.info(f"Loaded {len(df)} records into vaccination_records.")


if __name__ == "__main__":
    from extract import extract
    from transform import transform

    records = extract(max_records=3000, use_synthetic=True)
    df = transform(records)
    load(df)
    print("Load complete. Check your database!")