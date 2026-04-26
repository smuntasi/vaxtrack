# VaxTrack

An ETL pipeline and analytics dashboard for CDC childhood vaccination coverage data.

## Overview

VaxTrack extracts vaccination rate data from the CDC Open Data API, transforms and
cleans it using pandas, loads it into a PostgreSQL database, and surfaces it through
a Flask analytics dashboard.

## Architecture

CDC API → Extract → Transform → PostgreSQL → Flask Dashboard

## Tech Stack

- **Python** — ETL pipeline and backend
- **pandas** — data cleaning and normalization
- **PostgreSQL** — relational data store
- **SQLAlchemy** — database ORM and connection management
- **Flask** — web dashboard
- **Chart.js** — data visualization

## Setup

### 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/vaxtrack.git
cd vaxtrack

### 2. Create virtual environment
python -m virtualenv venv
source venv/Scripts/activate  # Windows
source venv/bin/activate       # Mac/Linux

### 3. Install dependencies
pip install -r requirements.txt

### 4. Configure database
Create a PostgreSQL database called `vaxtrack` and update the DB_URL
in `etl/load.py` and `app/app.py` with your credentials.

### 5. Run the pipeline
python pipeline.py

### 6. Start the dashboard
python app/app.py

Then open http://localhost:5000

## Features

- Paginated CDC API extraction with graceful fallback
- Data cleaning: type casting, null handling, derived coverage categories
- Indexed PostgreSQL storage for fast queries
- Interactive dashboard with vaccine and year filters
- Coverage breakdown by vaccine, state, and category