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
from selenium.common.exceptions import TimeoutException, NoSuchElementException
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

# Set up directories (adjust as needed)
BASE_DIR = os.path.join(os.getcwd(), "job_data")
CSV_DIR = os.path.join(BASE_DIR, "csv_files")
HISTORY_FILE = os.path.join(BASE_DIR, "job_history.json")
FILTERED_EXCEL = os.path.join(BASE_DIR, "filtered_jobs.xlsx")

os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

# Target companies to filter for (including TikTok)
TARGET_COMPANIES = ["Google", "Microsoft", "Amazon", "Meta", "Apple", "TikTok", "Draper"]

# ---------------- Helper Functions ----------------

def load_job_history():
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                data = json.load(f)
                data["seen_jobs"] = set(data.get("seen_jobs", []))
                return data
        return {"seen_jobs": []}
    except Exception as e:
        logger.error(f"Error loading job history: {e}")
        return {"seen_jobs": []}

def save_job_history(history):
    try:
        history["seen_jobs"] = list(history.get("seen_jobs", set()))
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history, f)
    except Exception as e:
        logger.error(f"Error saving job history: {e}")

def is_new_job(job, history):
    job_key = f"{job['Company']}_{job['Position Title']}"
    if job_key not in history["seen_jobs"]:
        history["seen_jobs"].append(job_key)
        return True
    return False

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--remote-debugging-port=9222")
    prefs = {
        "download.default_directory": CSV_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "browser.helperApps.neverAsk.saveToDisk": "text/csv"
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    service = Service('/usr/bin/chromedriver')
    return webdriver.Chrome(service=service, options=chrome_options)

def cleanup_old_csvs():
    current_time = datetime.now()
    for filename in os.listdir(CSV_DIR):
        if filename.endswith('.csv'):
            file_path = os.path.join(CSV_DIR, filename)
            file_time = datetime.fromtimestamp(os.path.getctime(file_path))
            if current_time - file_time > timedelta(hours=1):
                try:
                    os.remove(file_path)
                    logger.info(f"Deleted old CSV file: {filename}")
                except Exception as e:
                    logger.error(f"Error deleting old CSV file {filename}: {e}")

def save_filtered_jobs_to_excel(df):
    try:
        if os.path.exists(FILTERED_EXCEL):
            os.remove(FILTERED_EXCEL)
            logger.info(f"Deleted previous filtered jobs Excel file: {FILTERED_EXCEL}")
        df.to_excel(FILTERED_EXCEL, index=False)
        logger.info(f"Saved filtered jobs to: {FILTERED_EXCEL}")
        logger.info(f"Total jobs saved to Excel: {len(df)}")
    except Exception as e:
        logger.error(f"Error saving filtered jobs to Excel: {e}")

def filter_jobs(csv_path):
    """Filter jobs based on target companies and today's date, then save filtered data."""
    try:
        df = pd.read_csv(csv_path)
        logger.info("CSV Structure:")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info(f"Number of rows: {len(df)}")
        logger.info("\nSample of data:")
        logger.info(df.head())

        # Convert 'Date' column to datetime safely
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

        # Drop rows where date conversion failed
        df = df[df['Date'].notna()]

        # Normalize dates to remove time for accurate comparison
        df['OnlyDate'] = df['Date'].dt.normalize()
        today = pd.Timestamp(datetime.now().date())

        # Build regex pattern for matching companies
        company_pattern = '|'.join(map(re.escape, TARGET_COMPANIES))

        # Filter for target companies AND today's date
        filtered_df = df[
            (df['Company'].str.contains(company_pattern, case=False, na=False)) &
            (df['OnlyDate'] == today)
        ]

        logger.info("\nFiltering Results:")
        logger.info(f"Total jobs before filtering: {len(df)}")
        logger.info(f"Jobs from target companies: {len(df[df['Company'].str.contains(company_pattern, case=False, na=False)])}")
        logger.info(f"Jobs from today: {len(df[df['OnlyDate'] == today])}")
        logger.info(f"Final filtered jobs: {len(filtered_df)}")

        if not filtered_df.empty:
            logger.info("\nFiltered Jobs:")
            logger.info(filtered_df[['Company', 'Position Title', 'Date']].to_string())
            save_filtered_jobs_to_excel(filtered_df)

        # Save filtered jobs to CSV
        filtered_csv_path = csv_path.replace('.csv', '_filtered.csv')
        filtered_df.to_csv(filtered_csv_path, index=False)
        logger.info(f"Filtered jobs saved to: {filtered_csv_path}")
        return filtered_csv_path

    except Exception as e:
        logger.error(f"Error filtering jobs: {e}")
        return None


def send_csv_to_discord(csv_path):
    try:
        df = pd.read_csv(csv_path)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')  # Ensure datetime
        history = load_job_history()
        new_jobs = [job for _, job in df.iterrows() if is_new_job(job, history)]
        save_job_history(history)

        if not new_jobs:
            logger.info("No new job openings found for today from target companies.")
            return

        message = f"🎯 **New Job Openings from Target Companies** ({datetime.now().strftime('%Y-%m-%d %H:%M')})\n\n"
        for job in new_jobs:
            message += f"**Company:** {job['Company']}\n"
            message += f"**Position:** {job['Position Title']}\n"
            message += f"**Apply:** {job['Apply']}\n"
            message += "-------------------\n\n"
        message += f"\nTotal new jobs found: {len(new_jobs)}"

        logger.info(f"Sending the following message to Discord:\n{message}")
        payload = {
            "content": message,
            "username": "Job Scraper Bot",
            "avatar_url": "https://i.imgur.com/4M34hi2.png"
        }
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code == 200:
            logger.info("Successfully sent job openings to Discord")
            return True
        else:
            logger.error(f"Failed to send to Discord. Status code: {response.status_code}")
            logger.error(f"Response content: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error sending to Discord: {e}")
        return False

def download_airtable_csv(driver):
    try:
        driver.get(AIRTABLE_URL)
        logger.info("Navigated to Airtable URL")
        time.sleep(5)
        wait = WebDriverWait(driver, 10)
        wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "viewMenuButton"))).click()
        logger.info("Clicked view menu button")
        time.sleep(1)
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-tutorial-selector-id='viewMenuItem-viewExportCsv']"))).click()
        logger.info("Clicked Download CSV option")
        time.sleep(10)

        downloaded_files = [f for f in os.listdir(CSV_DIR) if f.endswith('.csv')]
        if not downloaded_files:
            logger.error("No CSV file found in downloads")
            return None

        latest_csv = max(downloaded_files, key=lambda x: os.path.getctime(os.path.join(CSV_DIR, x)))
        csv_path = os.path.join(CSV_DIR, latest_csv)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        new_path = os.path.join(CSV_DIR, f"jobs_{timestamp}.csv")
        os.rename(csv_path, new_path)
        logger.info(f"Saved CSV to: {new_path}")
        return new_path
    except Exception as e:
        logger.error(f"Error downloading CSV: {e}")
        return None

# ---------------- Main Execution ----------------

def main():
    try:
        logger.info("Starting job scraping process...")
        cleanup_old_csvs()
        driver = setup_driver()
        csv_path = download_airtable_csv(driver)
        driver.quit()

        if not csv_path:
            logger.error("No CSV file found after download; aborting.")
            return

        filtered_csv_path = filter_jobs(csv_path)
        if not filtered_csv_path:
            logger.error("Filtering failed; aborting.")
            return

        if send_csv_to_discord(filtered_csv_path):
            logger.info("Notification sent successfully")
        else:
            logger.error("Failed to send notification to Discord")

        try:
            os.remove(csv_path)
            logger.info("Removed downloaded CSV file after processing.")
        except Exception as e:
            logger.error(f"Error removing CSV file: {e}")

        logger.info("Job scraping process completed")
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
