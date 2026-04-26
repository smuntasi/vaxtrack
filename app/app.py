from flask import Flask, render_template, request
from sqlalchemy import create_engine, text
import json
from decimal import Decimal
from dotenv import load_dotenv
import os

app = Flask(__name__)

load_dotenv()
DB_URL = os.getenv("DB_URL")

engine = create_engine(DB_URL)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def query(sql, params=None):
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        return [dict(row._mapping) for row in result]


@app.route("/")
def index():
    by_vaccine = query("""
        SELECT vaccine, ROUND(AVG(coverage_estimate), 2) AS avg_coverage
        FROM vaccination_records
        GROUP BY vaccine
        ORDER BY avg_coverage DESC
    """)

    by_state = query("""
        SELECT geography, ROUND(AVG(coverage_estimate), 2) AS avg_coverage
        FROM vaccination_records
        GROUP BY geography
        ORDER BY geography
    """)

    by_category = query("""
        SELECT coverage_category, COUNT(*) AS count
        FROM vaccination_records
        GROUP BY coverage_category
        ORDER BY coverage_category
    """)

    vaccine_filter = request.args.get("vaccine", "All")
    year_filter = request.args.get("year", "All")

    filter_sql = "WHERE 1=1"
    filter_params = {}
    if vaccine_filter != "All":
        filter_sql += " AND vaccine = :vaccine"
        filter_params["vaccine"] = vaccine_filter
    if year_filter != "All":
        filter_sql += " AND survey_year = :year"
        filter_params["year"] = int(year_filter)

    records = query(f"""
        SELECT survey_year, geography, vaccine, dose,
               coverage_estimate, coverage_category, dimension_type, dimension
        FROM vaccination_records
        {filter_sql}
        ORDER BY survey_year DESC, geography
        LIMIT 100
    """, filter_params)

    vaccines = [r["vaccine"] for r in query("SELECT DISTINCT vaccine FROM vaccination_records ORDER BY vaccine")]
    years = [r["survey_year"] for r in query("SELECT DISTINCT survey_year FROM vaccination_records ORDER BY survey_year DESC")]

    return render_template("index.html",
        by_vaccine=json.dumps(by_vaccine, cls=DecimalEncoder),
        by_state=json.dumps(by_state, cls=DecimalEncoder),
        by_category=json.dumps(by_category, cls=DecimalEncoder),
        records=records,
        vaccines=vaccines,
        years=years,
        selected_vaccine=vaccine_filter,
        selected_year=year_filter,
    )


if __name__ == "__main__":
    app.run(debug=True)