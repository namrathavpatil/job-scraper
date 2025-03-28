import time
import logging
import os
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import pandas as pd
import re
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
WEBHOOK_URL = os.getenv('WEBHOOK_URL', "https://discord.com/api/webhooks/1354915426156675243/KVQEjmIQpw4LaDOb5Sv_AItFu38KDEyWh1wXDN2zQbv2LZ66y55WOQayiHTjiOWzcimh")
AIRTABLE_URL = os.getenv('AIRTABLE_URL', "https://airtable.com/embed/appjSXAWiVF4d1HoZ/shrf04yGbrK3IebAl/tbl7UBhvwqng6GRGZ?viewControls=on")

# Set up directories
BASE_DIR = os.path.join(os.getcwd(), "job_data")
CSV_DIR = os.path.join(BASE_DIR, "csv_files")
HISTORY_FILE = os.path.join(BASE_DIR, "job_history.json")
FILTERED_EXCEL = os.path.join(BASE_DIR, "filtered_jobs.xlsx")

# Create necessary directories
os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

# Target companies to filter for
TARGET_COMPANIES = ["Google", "Microsoft", "Amazon", "Meta", "Apple", "TikTok", "Draper"]

def load_job_history():
    """Load the history of previously seen jobs."""
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                return json.load(f)
        return {"seen_jobs": []}
    except Exception as e:
        logging.error(f"Error loading job history: {e}")
        return {"seen_jobs": []}

def save_job_history(history):
    """Save the job history to file."""
    try:
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history, f)
    except Exception as e:
        logging.error(f"Error saving job history: {e}")

def is_new_job(job, history):
    """Check if a job is new (not seen before)."""
    job_key = f"{job['Company']}_{job['Position Title']}"
    if job_key not in history["seen_jobs"]:
        history["seen_jobs"].append(job_key)
        return True
    return False

def setup_driver():
    """Set up and return a configured Chrome WebDriver with download preferences."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--remote-debugging-port=9222")
    
    # Set download preferences
    prefs = {
        "download.default_directory": CSV_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "browser.helperApps.neverAsk.saveToDisk": "text/csv"
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Use system Chrome in cloud environment
    service = Service('/usr/bin/chromedriver')
    
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=service, options=chrome_options)

def cleanup_old_csvs():
    """Delete CSV files older than 1 hour."""
    current_time = datetime.now()
    for filename in os.listdir(CSV_DIR):
        if filename.endswith('.csv'):
            file_path = os.path.join(CSV_DIR, filename)
            file_time = datetime.fromtimestamp(os.path.getctime(file_path))
            if current_time - file_time > timedelta(hours=1):
                try:
                    os.remove(file_path)
                    logging.info(f"Deleted old CSV file: {filename}")
                except Exception as e:
                    logging.error(f"Error deleting old CSV file {filename}: {e}")

def save_filtered_jobs_to_excel(df):
    """Save filtered jobs to Excel, replacing the previous file."""
    try:
        # Delete previous Excel file if it exists
        if os.path.exists(FILTERED_EXCEL):
            os.remove(FILTERED_EXCEL)
            logging.info(f"Deleted previous filtered jobs Excel file: {FILTERED_EXCEL}")
        
        # Save new filtered jobs to Excel
        df.to_excel(FILTERED_EXCEL, index=False)
        logging.info(f"Saved filtered jobs to: {FILTERED_EXCEL}")
        
        # Log the number of jobs saved
        logging.info(f"Total jobs saved to Excel: {len(df)}")
        
    except Exception as e:
        logging.error(f"Error saving filtered jobs to Excel: {e}")

def filter_jobs(csv_path):
    """Filter jobs based on target companies and today's date."""
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Log the structure of the CSV
        logging.info("CSV Structure:")
        logging.info(f"Columns: {list(df.columns)}")
        logging.info(f"Number of rows: {len(df)}")
        logging.info("\nSample of data:")
        logging.info(df.head())
        
        # Convert date column to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Get today's date
        today = datetime.now().date()
        
        # Create a pattern for company matching (case-insensitive)
        company_pattern = '|'.join(map(re.escape, TARGET_COMPANIES))
        
        # Filter for target companies and today's date
        filtered_df = df[
            (df['Company'].str.contains(company_pattern, case=False, na=False)) &
            (df['Date'].dt.date == today)
        ]
        
        # Log filtering results
        logging.info(f"\nFiltering Results:")
        logging.info(f"Total jobs before filtering: {len(df)}")
        logging.info(f"Jobs from target companies: {len(df[df['Company'].str.contains(company_pattern, case=False, na=False)])}")
        logging.info(f"Jobs from today: {len(df[df['Date'].dt.date == today])}")
        logging.info(f"Final filtered jobs: {len(filtered_df)}")
        
        if len(filtered_df) > 0:
            logging.info("\nFiltered Jobs:")
            logging.info(filtered_df[['Company', 'Position Title', 'Date']].to_string())
            
            # Save filtered jobs to Excel
            save_filtered_jobs_to_excel(filtered_df)
        
        # Save filtered data to CSV as well
        filtered_csv_path = csv_path.replace('.csv', '_filtered.csv')
        filtered_df.to_csv(filtered_csv_path, index=False)
        logging.info(f"Filtered jobs saved to: {filtered_csv_path}")
        
        return filtered_csv_path
        
    except Exception as e:
        logging.error(f"Error filtering jobs: {e}")
        return None

def send_csv_to_discord(csv_path):
    """Send filtered job openings to Discord with formatted message."""
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Load job history
        history = load_job_history()
        
        # Filter for new jobs only
        new_jobs = []
        for _, job in df.iterrows():
            if is_new_job(job, history):
                new_jobs.append(job)
        
        # Save updated history
        save_job_history(history)
        
        if len(new_jobs) == 0:
            message = f"📊 **Job Update** ({datetime.now().strftime('%Y-%m-%d %H:%M')})\n\n"
            message += "No new job openings found for today from target companies.\n"
            message += f"Total jobs checked: {len(df)}\n"
            message += f"Jobs from target companies: {len(df[df['Company'].str.contains('|'.join(map(re.escape, TARGET_COMPANIES)), case=False, na=False)])}\n"
            message += f"Jobs from today: {len(df[df['Date'].dt.date == datetime.now().date()])}"
        else:
            # Create a formatted message
            message = f"🎯 **New Job Openings from Target Companies** ({datetime.now().strftime('%Y-%m-%d %H:%M')})\n\n"
            
            for job in new_jobs:
                message += f"**Company:** {job['Company']}\n"
                message += f"**Position:** {job['Position Title']}\n"
                message += f"**Date:** {job['Date']}\n"
                message += f"**Apply:** {job['Apply']}\n"
                message += "-------------------\n\n"
            
            message += f"\nTotal new jobs found: {len(new_jobs)}"
        
        # Log the message content
        logging.info(f"Preparing to send message to Discord:\n{message}")
        
        # Send the message to Discord
        payload = {
            "content": message,
            "username": "Job Scraper Bot",
            "avatar_url": "https://i.imgur.com/4M34hi2.png"
        }
        
        response = requests.post(WEBHOOK_URL, json=payload)
        
        if response.status_code == 200:
            logging.info("Successfully sent job openings to Discord")
            return True
        else:
            logging.error(f"Failed to send to Discord. Status code: {response.status_code}")
            logging.error(f"Response content: {response.text}")
            return False
            
    except Exception as e:
        logging.error(f"Error sending to Discord: {e}")
        return False

def download_airtable_csv(driver):
    """Download CSV from Airtable."""
    try:
        # Navigate to Airtable URL
        driver.get(AIRTABLE_URL)
        logging.info("Navigated to Airtable URL")
        
        # Wait for the table to load
        time.sleep(5)
        
        # Set up wait
        wait = WebDriverWait(driver, 10)
        
        # Find and click the view menu button
        menu_button = wait.until(EC.element_to_be_clickable((
            By.CLASS_NAME, "viewMenuButton"
        )))
        menu_button.click()
        logging.info("Clicked view menu button")
        time.sleep(1)
        
        # Click the Download CSV option
        csv_button = wait.until(EC.element_to_be_clickable((
            By.CSS_SELECTOR, "[data-tutorial-selector-id='viewMenuItem-viewExportCsv']"
        )))
        csv_button.click()
        logging.info("Clicked Download CSV option")
        
        # Wait longer for download to complete
        time.sleep(10)
        
        # Find the downloaded file in the CSV directory
        downloaded_files = [f for f in os.listdir(CSV_DIR) if f.endswith('.csv')]
        if not downloaded_files:
            logging.error("No CSV file found in downloads")
            return None
            
        # Get the most recently downloaded CSV
        latest_csv = max(downloaded_files, key=lambda x: os.path.getctime(os.path.join(CSV_DIR, x)))
        csv_path = os.path.join(CSV_DIR, latest_csv)
        
        # Rename the file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        new_filename = f"jobs_{timestamp}.csv"
        new_path = os.path.join(CSV_DIR, new_filename)
        
        os.rename(csv_path, new_path)
        logging.info(f"Saved CSV to: {new_path}")
        
        return new_path
        
    except Exception as e:
        logging.error(f"Error downloading CSV: {e}")
        return None

def main():
    while True:
        try:
            logger.info("Starting job scraping process...")
            
            driver = setup_driver()
            try:
                # Download CSV from Airtable
                csv_path = download_airtable_csv(driver)
                
                if csv_path:
                    # Filter the jobs
                    filtered_csv_path = filter_jobs(csv_path)
                    
                    if filtered_csv_path:
                        # Send filtered CSV to Discord
                        if send_csv_to_discord(filtered_csv_path):
                            logging.info("Successfully sent filtered CSV to Discord")
                        else:
                            logging.error("Failed to send filtered CSV to Discord")
                    else:
                        logging.error("Failed to filter jobs")
                    
                    # Clean up old CSV files
                    cleanup_old_csvs()
                else:
                    logging.error("Failed to download CSV")
                    
            finally:
                driver.quit()
            
            logger.info("Job scraping process completed")
            
            # Wait for 1 hour before next run
            logger.info("Waiting for 1 hour before next run...")
            time.sleep(3600)  # 3600 seconds = 1 hour
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            logger.info("Waiting for 5 minutes before retrying...")
            time.sleep(300)  # Wait 5 minutes before retrying on error

if __name__ == "__main__":
    main()