import datetime
import pandas as pd
import threading
from supabase import create_client, Client
from jobspy_enhanced.indeed import Indeed
from jobspy_enhanced.model import Country, Site, ScraperInput, DescriptionFormat


SUPABASE_URL = "https://tbfcxawbygftalalhvlf.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRiZmN4YXdieWdmdGFsYWxodmxmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzYyNTA1NjcsImV4cCI6MjA5MTgyNjU2N30.REhIuOmOHJHxXowe0z754lYYXU539t-JsQ2Ymbnw1VI"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==============================
# ⚙️ SETTINGS
# ==============================
RESULTS_PER_COUNTRY = 100
HOURS_OLD = 24
BATCH_SIZE = 500

# ==============================
# 🔒 SCRAPER LOCK
# ==============================
SCRAPE_LOCK = threading.Lock()

# ==============================
# 🌍 GET COUNTRIES
# ==============================
def get_countries():
    return [c for c in Country if c.name not in ["US_CANADA", "WORLDWIDE"]]

# ==============================
# 🔄 CONVERT JOB → ROW
# ==============================
def job_to_row(job, country_name, role_id, role_name):
    return {
        "role_id": role_id,
        "role_name": role_name,
        "indeed_search_country": country_name,
        "title": job.title,
        "company_name": job.company_name,
        "country": str(job.location.country) if job.location and job.location.country else None,
        "location": str(job.location) if job.location else None,
        "job_url": job.job_url,
        "job_url_direct": job.job_url_direct,
        "date_posted": str(job.date_posted) if job.date_posted else None,
        "is_remote": job.is_remote,
        "description": (job.description or "")[:30000],
    }

# ==============================
# 🚀 MAIN LOGIC
# ==============================
def main():
    print("🚀 Starting job scraper...")

    # 📂 Load roles
    df_roles = pd.read_csv("karmafy_jobrole.csv")

    scraper = Indeed()
    all_jobs = []

    countries = get_countries()

    # 🔁 Loop roles
    for _, row in df_roles.iterrows():
        role_id = int(row["id"])
        role_name = str(row["name"]).strip()

        print(f"🔍 Scraping: {role_name}")

        for country in countries:
            try:
                scraper_input = ScraperInput(
                    site_type=[Site.INDEED],
                    search_term=role_name,
                    country=country,
                    results_wanted=RESULTS_PER_COUNTRY,
                    hours_old=HOURS_OLD,
                    description_format=DescriptionFormat.MARKDOWN,
                )

                with SCRAPE_LOCK:
                    scraper.seen_urls.clear()
                    response = scraper.scrape(scraper_input)

                for job in response.jobs:
                    all_jobs.append(
                        job_to_row(job, country.name, role_id, role_name)
                    )

            except Exception as e:
                print(f"⚠️ Error: {role_name} - {country.name} - {e}")

    if not all_jobs:
        print("❌ No jobs found")
        return

    # ==============================
    # 🧹 CLEAN DATA
    # ==============================
    df = pd.DataFrame(all_jobs)
    df = df.where(pd.notnull(df), None)

    # 🔥 REMOVE DUPLICATES (VERY IMPORTANT)
    df = df.drop_duplicates(subset=["job_url_direct"])

    print(f"✅ Total jobs after dedup: {len(df)}")

    # ==============================
    # 📦 UPLOAD TO SUPABASE
    # ==============================
    data = df.to_dict(orient="records")

    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]

        try:
            supabase.table("jobs_all_roles") \
                .upsert(batch, on_conflict="job_url_direct") \
                .execute()

            print(f"✅ Uploaded {i} to {i+len(batch)}")

        except Exception as e:
            print(f"❌ Upload error: {e}")

    print("🎉 DONE: All jobs uploaded!")

# ==============================
if __name__ == "__main__":
    main()
