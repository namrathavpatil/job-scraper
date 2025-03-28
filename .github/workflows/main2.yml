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
FILTERED_EXCEL = os.path.join(BASE_DIR, "filtered_jobs.xlsx")
HISTORY_FILE = os.path.join(BASE_DIR, 'job_history.json')

# Create directories if they don't exist
os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

# Load job history from file
def load_job_history():
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                data = json.load(f)
                logger.info(f"Loaded {len(data)} previously seen jobs from history")
                return data
        logger.info("No job history found, starting fresh")
        return {}
    except Exception as e:
        logger.error(f"Error loading job history: {e}")
        return {}

def save_job_history(history):
    try:
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history, f, indent=2)
        logger.info(f"Saved {len(history)} jobs to history")
        
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

# Load initial job history
job_history = load_job_history()

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
    "Chevron Corporation", "Cigna", "Ford Motor Company", "Bank of America", "General Motors", "Elevance Health"
]

# ---------------- Helper Functions ----------------

def is_new_job(job):
    """Check if a job is new using file history."""
    key = f"{job['Company']} - {job['Position Title']}"
    return key not in job_history

def mark_job_seen(job):
    """Mark a job as seen in file history."""
    key = f"{job['Company']} - {job['Position Title']}"
    job_history[key] = str(datetime.utcnow())
    save_job_history(job_history)
    return True

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
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(csv_path)

        # Convert 'Date' column to datetime, coercing errors to NaT
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

        # Filter out rows with invalid dates
        df = df[df['Date'].notna()]

        # Extract only the date part
        df['OnlyDate'] = df['Date'].dt.normalize()

        # Get today's date
        today = pd.Timestamp(datetime.now().date())

        # Compile regex pattern for target companies
        company_pattern = '|'.join(map(re.escape, TARGET_COMPANIES))

        # Filter jobs for target companies posted today
        company_df = df[
            df['Company'].str.contains(company_pattern, case=False, na=False) &
            (df['OnlyDate'] == today)
        ]

        # Filter researcher jobs
        researcher_df = df[
            df['Position Title'].str.contains('researcher', case=False, na=False)
        ]

        # Filter university jobs
        university_df = df[
            df['Company'].str.contains('university', case=False, na=False)
        ]

        # Exclude university jobs from researcher_df
        non_university_researcher_df = researcher_df[
            ~researcher_df['Company'].str.contains('university', case=False, na=False)
        ]

        researcher_combined_df = non_university_researcher_df

        logger.info(f"Target company jobs today: {len(company_df)}")
        logger.info(f"'Researcher' jobs found: {len(researcher_df)}")
        logger.info(f"'Researcher' jobs (non-university): {len(researcher_combined_df)}")
        logger.info(f"University jobs found: {len(university_df)}")

        # Save combined Excel
        combined_df = pd.concat([company_df, researcher_combined_df, university_df]).drop_duplicates()
        if not combined_df.empty:
            save_filtered_jobs_to_excel(combined_df)

        return company_df, researcher_combined_df, university_df

    except Exception as e:
        logger.error(f"Error filtering jobs: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def send_csv_to_discord(csv_path, webhook_url, label="Job Openings"):
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Filter for new jobs only
        new_jobs = df[df.apply(is_new_job, axis=1)]
        
        if new_jobs.empty:
            logger.info(f"No new {label} to send to Discord")
            return
        
        logger.info(f"Found {len(new_jobs)} new {label} to send to Discord")
        
        # Split into chunks of 10 jobs
        chunk_size = 10
        for i in range(0, len(new_jobs), chunk_size):
            chunk = new_jobs.iloc[i:i + chunk_size]
            
            # Create message content
            message = f"**{label}**\n\n"
            for _, job in chunk.iterrows():
                message += f"**{job['Position Title']}** at **{job['Company']}**\n"
                message += f"Posted: {job['Date']}\n"
                if pd.notna(job.get('Apply')):
                    message += f"Apply: {job['Apply']}\n"
                message += "\n"
                
                # Mark job as seen
                mark_job_seen(job)
            
            # Send to Discord
            payload = {"content": message}
            response = requests.post(webhook_url, json=payload)
            
            if response.status_code == 204:
                logger.info(f"Successfully sent part {i//chunk_size + 1} to Discord (label: {label})")
            else:
                logger.error(f"Failed to send to Discord: {response.status_code} - {response.text}")
            
            # Add delay between chunks
            time.sleep(1)
        
        logger.info(f"Successfully sent all {label} to Discord")
        
    except Exception as e:
        logger.error(f"Error sending to Discord: {e}")

def download_airtable_csv(driver):
    try:
        # Navigate to the Airtable URL
        driver.get(AIRTABLE_URL)
        logger.info("Navigated to Airtable URL")
        
        # Wait for and click the view menu button
        view_menu = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label='View menu']"))
        )
        view_menu.click()
        logger.info("Clicked view menu button")
        
        # Wait for and click the Download CSV option
        download_csv = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//span[text()='Download CSV']"))
        )
        download_csv.click()
        logger.info("Clicked Download CSV option")
        
        # Wait for download to complete
        time.sleep(5)
        
        # Find the downloaded CSV file
        csv_files = [f for f in os.listdir(CSV_DIR) if f.endswith('.csv')]
        if not csv_files:
            raise Exception("No CSV file found after download")
        
        latest_csv = max(csv_files, key=lambda x: os.path.getctime(os.path.join(CSV_DIR, x)))
        csv_path = os.path.join(CSV_DIR, latest_csv)
        
        logger.info(f"Saved CSV to: {csv_path}")
        return csv_path
        
    except Exception as e:
        logger.error(f"Error downloading CSV: {e}")
        raise

def cleanup_old_jobs():
    """Remove jobs older than 30 days from history."""
    current_time = datetime.utcnow()
    old_jobs = []
    
    for key, date_str in job_history.items():
        try:
            job_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            if (current_time - job_date).days > 30:
                old_jobs.append(key)
        except ValueError:
            continue
    
    for key in old_jobs:
        del job_history[key]
    
    if old_jobs:
        save_job_history(job_history)
        logger.info(f"Removed {len(old_jobs)} old jobs from history")

def main():
    try:
        logger.info("Starting job scraping process...")
        
        # Clean up old jobs from history
        cleanup_old_jobs()
        
        # Clean up old CSV files
        cleanup_old_csvs()
        
        # Set up and start the driver
        driver = setup_driver()
        
        try:
            # Download the CSV file
            csv_path = download_airtable_csv(driver)
            
            # Filter jobs
            company_df, researcher_df, university_df = filter_jobs(csv_path)
            
            # Send filtered jobs to Discord
            if not company_df.empty:
                send_csv_to_discord(csv_path, WEBHOOK_URL, "Target Company Jobs")
            
            if not researcher_df.empty:
                send_csv_to_discord(csv_path, RESEARCH_WEBHOOK_URL, "Researcher Jobs")
            
            if not university_df.empty:
                send_csv_to_discord(csv_path, UNIVERSITY_WEBHOOK_URL, "University Jobs")
            
            # Remove the downloaded CSV file
            os.remove(csv_path)
            logger.info("Removed downloaded CSV file after processing.")
            
        finally:
            # Always quit the driver
            driver.quit()
        
        logger.info("Job scraping process completed")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    main()
