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
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
WEBHOOK_URL = os.getenv('WEBHOOK_URL')
AIRTABLE_URL = os.getenv('AIRTABLE_URL')

if not WEBHOOK_URL or not AIRTABLE_URL:
    logger.error("Missing required environment variables: WEBHOOK_URL or AIRTABLE_URL")
    sys.exit(1)

# Directories
BASE_DIR = os.path.join(os.getcwd(), "job_data")
CSV_DIR = os.path.join(BASE_DIR, "csv_files")
HISTORY_FILE = os.path.join(BASE_DIR, "job_history.json")
FILTERED_EXCEL = os.path.join(BASE_DIR, "filtered_jobs.xlsx")
LOGGED_JOBS_FILE = os.path.join(BASE_DIR, "jobs_sent_to_discord.txt")

os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

# Target companies
TARGET_COMPANIES = [...]  # Keep your full list here

def load_job_history():
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                data = json.load(f)
                data["seen_jobs"] = set(data.get("seen_jobs", []))
                return data
        return {"seen_jobs": set()}
    except:
        return {"seen_jobs": set()}

def save_job_history(history):
    history["seen_jobs"] = list(history.get("seen_jobs", set()))
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f)

def is_new_job(job, history):
    key = f"{job['Company']}_{job['Position Title']}"
    if key not in history['seen_jobs']:
        history['seen_jobs'].add(key)
        return True
    return False

def setup_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--user-agent=Mozilla/5.0")
    prefs = {"download.default_directory": CSV_DIR, "download.prompt_for_download": False}
    options.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(service=Service('/usr/bin/chromedriver'), options=options)

def download_airtable_csv(driver):
    driver.get(AIRTABLE_URL)
    time.sleep(5)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "viewMenuButton"))).click()
    time.sleep(1)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-tutorial-selector-id='viewMenuItem-viewExportCsv']"))).click()
    time.sleep(10)
    csv_files = [f for f in os.listdir(CSV_DIR) if f.endswith('.csv')]
    if not csv_files:
        return None
    latest = max(csv_files, key=lambda f: os.path.getctime(os.path.join(CSV_DIR, f)))
    new_path = os.path.join(CSV_DIR, f"jobs_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
    os.rename(os.path.join(CSV_DIR, latest), new_path)
    return new_path

def filter_jobs(csv_path):
    df = pd.read_csv(csv_path)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df[df['Date'].notna()]
    df['OnlyDate'] = df['Date'].dt.normalize()
    today = pd.Timestamp(datetime.now().date())
    pattern = '|'.join(map(re.escape, TARGET_COMPANIES))
    df = df[df['Company'].str.contains(pattern, case=False, na=False) & (df['OnlyDate'] == today)]
    if not df.empty:
        df.to_excel(FILTERED_EXCEL, index=False)
        df.to_csv(csv_path, index=False)
        return csv_path
    return None

def log_sent_jobs(jobs):
    with open(LOGGED_JOBS_FILE, "a") as f:
        for job in jobs:
            date = pd.to_datetime(job['Date'], errors='coerce')
            date_str = date.strftime('%Y-%m-%d') if not pd.isna(date) else "Unknown"
            f.write(f"{job['Position Title']} | {job['Company']} | {date_str}\n")

def send_csv_to_discord(csv_path, webhook_url, label):
    if not webhook_url:
        return False
    history = load_job_history()
    df = pd.read_csv(csv_path)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    new_jobs = [job for _, job in df.iterrows() if is_new_job(job, history)]
    if not new_jobs:
        return True

    messages, msg = [], f"🎯 **{label}** ({datetime.now().strftime('%Y-%m-%d %H:%M')})\n\n"
    for job in new_jobs:
        txt = f"**Company:** {job['Company']}\n**Position:** {job['Position Title']}\n**Apply:** {job['Apply']}\n-------------------\n\n"
        if len(msg) + len(txt) > 1900:
            messages.append(msg)
            msg = ""
        msg += txt
    if msg:
        messages.append(msg)

    for m in messages:
        res = requests.post(webhook_url, json={"content": m, "username": "Job Scraper Bot", "avatar_url": "https://i.imgur.com/4M34hi2.png"})
        if res.status_code not in [200, 204]:
            return False
        time.sleep(1)

    save_job_history(history)
    log_sent_jobs(new_jobs)
    return True

def cleanup_old_csvs():
    now = datetime.now()
    for f in os.listdir(CSV_DIR):
        path = os.path.join(CSV_DIR, f)
        if f.endswith(".csv") and now - datetime.fromtimestamp(os.path.getctime(path)) > timedelta(hours=1):
            os.remove(path)

def main():
    cleanup_old_csvs()
    driver = setup_driver()
    csv_path = download_airtable_csv(driver)
    driver.quit()

    if not csv_path:
        return

    filtered_csv = filter_jobs(csv_path)
    if filtered_csv:
        send_csv_to_discord(filtered_csv, WEBHOOK_URL, "Target Company Jobs")

    os.remove(csv_path)

if __name__ == "__main__":
    main()
