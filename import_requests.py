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
RESEARCH_WEBHOOK_URL = os.getenv('RESEARCH_WEBHOOK_URL')
UNIVERSITY_WEBHOOK_URL = os.getenv('UNIVERSITY_WEBHOOK_URL')

if not WEBHOOK_URL or not AIRTABLE_URL or not RESEARCH_WEBHOOK_URL or not UNIVERSITY_WEBHOOK_URL:
    logger.error("Missing required environment variables: WEBHOOK_URL, AIRTABLE_URL, RESEARCH_WEBHOOK_URL, or UNIVERSITY_WEBHOOK_URL")
    sys.exit(1)
    
# Set up directories (adjust as needed)
BASE_DIR = os.path.join(os.getcwd(), "job_data")
CSV_DIR = os.path.join(BASE_DIR, "csv_files")
HISTORY_FILE = os.path.join(BASE_DIR, "job_history.json")
FILTERED_EXCEL = os.path.join(BASE_DIR, "filtered_jobs.xlsx")
LOGGED_JOBS_FILE = os.path.join(BASE_DIR, "jobs_sent_to_discord.txt")

# Create directories if they don't exist
os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

# Target companies to filter for (including TikTok)
TARGET_COMPANIES = [
    "Google", "Microsoft", "Amazon", "Meta", "Apple", "TikTok", "Draper", "Yahoo", "Tesla", "Nvidia",
    "Hyundai", "Deloitte", "PwC", "EY", "KPMG", "Goldman Sachs", "The Walt Disney Company", "Wells Fargo",
    "McKinsey & Company", "Riot Games", "Tinder", "DISQO", "GumGum", "MySpace", "Telesign", "PeerStreet",
    "Escape Communications", "Push Media", "Quantum Dimension", "Robin Labs", "Southbay", "The White Rabbit Entertainment",
    "Rubicon Project", "TaskUs", "AssetAvenue", "Clutter", "Intel", "Samsung", "Qualcomm", "AMD", "LiveRamp",
    "Red Hat", "Ciena", "Acadaca", "TP-Link", "CoBank", "Intermountain Health", "Hexagon Manufacturing Intelligence",
    "North Carolina State University", "ProbablyMonsters", "Western Digital", "Boise State University", "TabaPay",
    "The New York Times", "Wolters Kluwer", "Siemens Healthineers", "Cboe Global Markets", "Exelon", "Medtronic",
    "Collins Aerospace", "General Dynamics Information Technology", "General Atomics", "Walgreens", "Delmarva Power",
    "CGI", "Midland Credit Management", "Fiserv", "Capital One", "Teledyne Technologies Incorporated", "ByteDance",
    "Haas Automation, Inc.", "SpaceX", "Tatari", "Aspen Technology", "Vertafore", "Mission Technologies",
    "Palantir Technologies", "Adobe", "Medpace", "Mastercard", "Rambus", "The Reynolds and Reynolds Company",
    "Boeing", "Analog Devices", "Northrop Grumman", "Patterson Companies, Inc.", "Piper Companies", "Aperia Technologies",
    "Galaxy", "Costco Wholesale", "Texas A&M Engineering Experiment Station (TEES)", "Moffatt & Nichol", "Quick Quack Car Wash",
    "KLA", "Lockheed Martin", "University of Maryland Medical System", "Belvedere Trading, LLC", "Casey's",
    "The University of Texas at Austin", "Daimler Truck North America", "Texas A&M University", "Coalition, Inc.",
    "Delta Solutions and Strategies", "Ennoble First Inc.", "FloQast", "Spring Health", "American Family Insurance",
    "Resideo", "Freddie Mac", "NetSuite", "Virginia Commonwealth University", "AMEWAS, Inc.", "Esri", "Stanford Health Care",
    "Prime Healthcare", "Leonardo DRS", "Wizards of the Coast", "Ancestry", "General Atomics Aeronautical Systems",
    "Federal Signal Corporation", "Afficiency", "Amazon Web Services (AWS)", "BlackRock", "AppLovin", "Sinch",
    "Catalent Pharma Solutions", "Splunk", "Field Agent", "Kensho Technologies", "Parsons Corporation", "Nature's Bakery",
    "Neuralink", "AIG", "Atlassian", "Odoo", "Ascend Analytics", "Sandia National Laboratories", "Blue Origin",
    "Corpay", "Madiba, Inc.", "TraceGains", "Abbott", "American Electric Power", "Moveworks", "Cognizant", "University of Virginia",
    "California Highway Patrol", "University of Southern California", "Nidec Motor Corporation", "Austin Community College",
    "Diversified Services Network, Inc.", "Plexus Corp.", "State of Nebraska", "Experian", "Infinite Campus", "Affirm",
    "Addepar", "HSA Bank", "Perdue Farms", "CodePath", "Twitch", "Rockstar Games", "HashiCorp", "Peraton", "SquareTrade",
    "Nintendo", "WOOD Consulting Services, Inc.", "Trillium Health Resources", "Target", "Sierra Nevada Corporation",
    "Bectran, Inc.", "Walmart", "DoorDash", "eBay", "Airbnb", "Chewy", "Wayfair", "Expedia Group", "Booking Holdings",
    "Coupang", "Uber Technologies", "Concentrix", "Science Applications International", "Insight Enterprises",
    "Booz Allen Hamilton Holding", "DXC Technology", "Leidos Holdings", "Kyndryl Holdings", "Cognizant Technology Solutions",
    "CDW", "IBM", "Motorola Solutions", "Amphenol", "Cisco Systems", "ON Semiconductor", "Microchip Technology",
    "Sanmina", "KLA", "Lam Research", "Texas Instruments", "Applied Materials", "Micron Technology", "Jabil", "Broadcom",
    "Advanced Micro Devices", "Analog Devices", "HP Inc.", "Lenovo", "Panasonic", "Accenture", "IBM", "Dell Technologies",
    "Sony", "Hitachi", "Tencent", "Huawei", "Deutsche Telekom", "Meta", "AT&T", "Alibaba", "Jingdong", "Foxconn",
    "Samsung Electronics", "Alphabet", "Apple", "Amazon", "Walmart", "UnitedHealth Group", "Berkshire Hathaway",
    "CVS Health", "ExxonMobil", "McKesson Corporation", "Cencora", "Costco", "JPMorgan Chase", "Cardinal Health",
    "Chevron Corporation", "Cigna", "Ford Motor Company", "Bank of America", "General Motors", "Elevance Health","SoundCloud", "SharkNinja", "Juniper Networks", "Cisco ThousandEyes"
]

def load_job_history():
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                data = json.load(f)
                # Convert list to set if it exists, otherwise create empty set
                data["seen_jobs"] = set(data.get("seen_jobs", []))
                logger.info(f"Loaded {len(data['seen_jobs'])} previously seen jobs from history")
                return data
        logger.info("No job history found, starting fresh")
        return {"seen_jobs": set()}
    except Exception as e:
        logger.error(f"Error loading job history: {e}")
        return {"seen_jobs": set()}

def save_job_history(history):
    try:
        # Convert set to list before saving
        history["seen_jobs"] = list(history.get("seen_jobs", set()))
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history, f)
        logger.info(f"Saved {len(history['seen_jobs'])} jobs to history")
        
        # If running in GitHub Actions, commit the changes
        if os.getenv('GITHUB_ACTIONS'):
            try:
                os.system('git config --global user.name "github-actions"')
                os.system('git config --global user.email "actions@github.com"')
                os.system(f'git add {HISTORY_FILE}')
                os.system('git commit -m "Update job history" || echo "No changes to commit"')
                os.system('git push')
                logger.info("Committed job history changes to repository")
            except Exception as e:
                logger.error(f"Error committing job history: {e}")
    except Exception as e:
        logger.error(f"Error saving job history: {e}")

def is_new_job(job, history):
    job_key = f"{job['Company']}_{job['Position Title']}"
    if job_key not in history["seen_jobs"]:
        history["seen_jobs"].add(job_key)
        logger.info(f"Found new job: {job['Company']} - {job['Position Title']}")
        return True
    logger.debug(f"Skipping already seen job: {job['Company']} - {job['Position Title']}")
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

def log_sent_jobs(jobs):
    try:
        os.makedirs(BASE_DIR, exist_ok=True)  # Make sure the folder exists
        if not os.path.exists(LOGGED_JOBS_FILE):
            with open(LOGGED_JOBS_FILE, "w") as f:
                f.write("Position Title | Company | Date\n")  # Header for first-time file

        with open(LOGGED_JOBS_FILE, "a") as f:
            for job in jobs:
                date_str = pd.to_datetime(job['Date'], errors='coerce')
                if pd.isna(date_str):
                    date_str = "Unknown"
                else:
                    date_str = date_str.strftime('%Y-%m-%d')
                f.write(f"{job['Position Title']} | {job['Company']} | {date_str}\n")
    except Exception as e:
        logger.error(f"Error logging sent jobs: {e}")

def send_csv_to_discord(csv_path, webhook_url, label="Job Openings"):
    try:
        if not webhook_url:
            logger.error(f"Webhook URL is empty for {label}")
            return False

        if not webhook_url.startswith('http'):
            logger.error(f"Invalid webhook URL format for {label}")
            return False

        # Load job history at the start
        history = load_job_history()
        logger.info(f"Checking {label} against {len(history['seen_jobs'])} previously seen jobs")

        df = pd.read_csv(csv_path)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        # Filter for new jobs
        new_jobs = [job for _, job in df.iterrows() if is_new_job(job, history)]
        
        if not new_jobs:
            logger.info(f"No new {label.lower()} found.")
            return True

        logger.info(f"Found {len(new_jobs)} new {label.lower()} to send to Discord")

        base_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        messages = []
        current_msg = f"🎯 **{label}** ({base_time})\n\n"

        for job in new_jobs:
            job_text = (
                f"**Company:** {job['Company']}\n"
                f"**Position:** {job['Position Title']}\n"
                f"**Apply:** {job['Apply']}\n"
                "-------------------\n\n"
            )
            if len(current_msg) + len(job_text) > 1900:
                messages.append(current_msg)
                current_msg = ""
            current_msg += job_text

        if current_msg:
            messages.append(current_msg)

        success = True
        for idx, msg in enumerate(messages):
            payload = {
                "content": msg,
                "username": "Job Scraper Bot",
                "avatar_url": "https://i.imgur.com/4M34hi2.png"
            }
            response = requests.post(webhook_url, json=payload)

            if response.status_code in [200, 204]:  # Both are success codes
                logger.info(f"Successfully sent part {idx + 1} to Discord (label: {label})")
            else:
                logger.error(f"Failed to send part {idx + 1} to Discord (label: {label}). Status code: {response.status_code}")
                logger.error(f"Response content: {response.text}")
                success = False

            time.sleep(1)

        if success:
            try:
                # Save to job history
                logger.info(f"Saving {len(new_jobs)} new jobs to history file")
                save_job_history(history)
                
                # Log sent jobs to text file
                logger.info(f"Logging {len(new_jobs)} sent jobs to {LOGGED_JOBS_FILE}")
                log_sent_jobs(new_jobs)
                
                logger.info(f"Successfully sent all {label.lower()} to Discord and updated both history files")
            except Exception as e:
                logger.error(f"Error saving job history or logging sent jobs: {e}")
                # Don't return False here as the Discord messages were sent successfully
        else:
            logger.error(f"Failed to send all {label.lower()} to Discord, not updating history")
            return False

        return True

    except Exception as e:
        logger.error(f"Error sending {label.lower()} to Discord: {e}")
        return False

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
    try:
        df = pd.read_csv(csv_path)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df[df['Date'].notna()]
        
        # Convert to PDT timezone
        import pytz
        pdt_timezone = pytz.timezone('America/Los_Angeles')
        today = datetime.now(pdt_timezone).date()
        
        # Create pattern for company matching
        company_pattern = '|'.join(map(re.escape, TARGET_COMPANIES))

        # Filter for target companies with today's date in PDT
        company_df = df[
            df['Company'].str.contains(company_pattern, case=False, na=False) &
            (df['Date'].dt.date == today)
        ]
        
        # Filter for researcher positions
        researcher_df = df[
            df['Position Title'].str.contains('researcher', case=False, na=False)
        ]
        
        # Filter for university positions
        university_df = df[
            df['Company'].str.contains('university', case=False, na=False)
        ]
        
        # Filter for non-university researcher positions
        non_university_researcher_df = researcher_df[
            ~researcher_df['Company'].str.contains('university', case=False, na=False)
        ]
        
        # Save filtered CSVs and return file paths
        def save_df(df, suffix):
            if df.empty:
                return None
            out_path = csv_path.replace(".csv", f"_{suffix}.csv")
            df.to_csv(out_path, index=False)
            return out_path

        company_path = save_df(company_df, "companies")
        researcher_path = save_df(non_university_researcher_df, "researchers")
        university_path = save_df(university_df, "universities")

        combined_df = pd.concat([company_df, non_university_researcher_df, university_df]).drop_duplicates()
        if not combined_df.empty:
            save_filtered_jobs_to_excel(combined_df)

        return company_path, researcher_path, university_path

    except Exception as e:
        logger.error(f"Error filtering jobs: {e}")
        return None, None, None


def main():
    try:
        logger.info("Starting job scraping process...")
        cleanup_old_csvs()

        # Launch browser and download Airtable CSV
        driver = setup_driver()
        csv_path = download_airtable_csv(driver)
        driver.quit()

        if not csv_path:
            logger.error("No CSV file found after download; aborting.")
            return

        # Filter and save to separate CSVs — returns file paths
        company_csv, researcher_csv, university_csv = filter_jobs(csv_path)

        # If all are None, there's nothing new to send
        if company_csv is None and researcher_csv is None and university_csv is None:
            logger.error("No relevant jobs found; aborting.")
            return

        # Send each non-empty filtered CSV to the appropriate Discord webhook
        if company_csv is not None:
            send_csv_to_discord(company_csv, WEBHOOK_URL, label="Target Company Jobs")

        if researcher_csv is not None:
            send_csv_to_discord(researcher_csv, RESEARCH_WEBHOOK_URL, label="Researcher Jobs")

        if university_csv is not None:
            send_csv_to_discord(university_csv, UNIVERSITY_WEBHOOK_URL, label="University Jobs")

        # Clean up the original downloaded CSV
        try:
            os.remove(csv_path)
            logger.info("Removed downloaded CSV file after processing.")
        except Exception as e:
            logger.error(f"Error removing CSV file: {e}")

        logger.info("Job scraping process completed successfully.")
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise



if __name__ == "__main__":
    main()
