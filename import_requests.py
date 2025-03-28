import os
import time
import logging
import json
import re
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ---------- Setup ----------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("JobScraper")

WEBHOOK_URL = os.getenv("WEBHOOK_URL")
AIRTABLE_URL = os.getenv("AIRTABLE_URL")

if not WEBHOOK_URL or not AIRTABLE_URL:
    logger.error("Missing required environment variables: WEBHOOK_URL or AIRTABLE_URL")
    exit(1)

BASE_DIR = Path("job_data")
CSV_DIR = BASE_DIR / "csv_files"
HISTORY_FILE = BASE_DIR / "job_history.json"
FILTERED_EXCEL = BASE_DIR / "filtered_jobs.xlsx"

TARGET_COMPANIES = ["Google", "Microsoft", "Amazon", "Meta", "Apple", "TikTok", "Draper", "Mayo Clinic"]

BASE_DIR.mkdir(parents=True, exist_ok=True)
CSV_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Helper Functions ----------

def load_history():
    if HISTORY_FILE.exists():
        with open(HISTORY_FILE, 'r') as f:
            return json.load(f)
    return {"seen_jobs": []}

def save_history(history):
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f)

def is_new_job(job, history):
    key = f"{job['Company']}_{job['Position Title']}"
    if key not in history["seen_jobs"]:
        history["seen_jobs"].append(key)
        return True
    return False

def setup_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    prefs = {
        "download.default_directory": str(CSV_DIR.resolve()),
        "download.prompt_for_download": False
    }
    options.add_experimental_option("prefs", prefs)
    service = Service("/usr/bin/chromedriver")  # Adjust path if needed
    return webdriver.Chrome(service=service, options=options)

def cleanup_old_csvs():
    now = datetime.now()
    for file in CSV_DIR.glob("*.csv"):
        if now - datetime.fromtimestamp(file.stat().st_ctime) > timedelta(hours=1):
            file.unlink()

def download_csv(driver):
    driver.get(AIRTABLE_URL)
    time.sleep(5)
    WebDriverWait(driver, 15).until(
        EC.element_to_be_clickable((By.CLASS_NAME, "viewMenuButton"))
    ).click()
    time.sleep(1)
    WebDriverWait(driver, 15).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-tutorial-selector-id='viewMenuItem-viewExportCsv']"))
    ).click()
    time.sleep(10)

    files = list(CSV_DIR.glob("*.csv"))
    if not files:
        logger.error("No CSV downloaded.")
        return None

    latest = max(files, key=lambda f: f.stat().st_ctime)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    renamed = CSV_DIR / f"jobs_{timestamp}.csv"
    latest.rename(renamed)
    return renamed

def filter_jobs(csv_path):
    df = pd.read_csv(csv_path)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df[df["Date"].notna()]
    today = datetime.now().date()

    mask = (
        df["Company"].str.contains('|'.join(TARGET_COMPANIES), case=False, na=False)
        & (df["Date"].dt.date == today)
    )
    filtered = df[mask]

    logger.info(f"Total jobs before filtering: {len(df)}")
    logger.info(f"Jobs from target companies: {len(df[df['Company'].str.contains('|'.join(TARGET_COMPANIES), case=False, na=False)])}")
    logger.info(f"Jobs from today: {len(df[df['Date'].notna() & (df['Date'].dt.date == today)])}")
    logger.info(f"Final filtered jobs: {len(filtered)}")

    if not filtered.empty:
        logger.info("\nFiltered Jobs:")
        logger.info(filtered[['Company', 'Position Title', 'Date']].to_string())
        filtered.to_csv(str(csv_path).replace(".csv", "_filtered.csv"), index=False)
        filtered.to_excel(FILTERED_EXCEL, index=False)
        return filtered

    return pd.DataFrame()

def send_to_discord(jobs_df):
    history = load_history()
    new_jobs = [job for _, job in jobs_df.iterrows() if is_new_job(job, history)]
    save_history(history)

    if not new_jobs:
        logger.info("No new jobs found.")
        return

    message = "**🎯 New Job Openings from Target Companies**\n\n"
    for job in new_jobs:
        message += f"**Company:** {job['Company']}\n**Position:** {job['Position Title']}\n**Apply:** {job['Apply']}\n---\n"

    requests.post(WEBHOOK_URL, json={"content": message})

# ---------- Main ----------

def main():
    logger.info("🔍 Starting job scraper...")

    cleanup_old_csvs()
    driver = setup_driver()
    try:
        csv_path = download_csv(driver)
    finally:
        driver.quit()

    if not csv_path:
        return

    filtered_df = filter_jobs(csv_path)
    if not filtered_df.empty:
        send_to_discord(filtered_df)

    logger.info("✅ Job scraping finished.")

if __name__ == "__main__":
    main()
