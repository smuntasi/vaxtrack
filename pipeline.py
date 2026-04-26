import logging
from etl.extract import extract
from etl.transform import transform
from etl.load import load

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("=== VaxTrack Pipeline Starting ===")
    records = extract(max_records=3000, use_synthetic=True)
    df = transform(records)
    load(df)
    logger.info("=== Pipeline Complete ===")