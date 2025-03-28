from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=chrome_options)

def test_selectors():
    driver = None
    try:
        driver = setup_driver()
        url = "https://airtable.com/embed/appjSXAWiVF4d1HoZ/shrf04yGbrK3IebAl/tbl7UBhvwqng6GRGZ?viewControls=on"
        driver.get(url)
        
        # Wait for page to load
        print("Waiting for page to load...")
        time.sleep(10)  # Increased wait time
        
        # Print page source for debugging
        print("\nPage source preview:")
        print(driver.page_source[:500])
        
        # Try different header selectors
        header_selectors = [
            ".cell.header.read.readonly.primary .nameAndDescription .name",
            ".cell.header.read.readonly .name",
            ".headerRow .cell.header .name",
            "[data-tutorial-selector-id='gridHeaderCell'] .name"
        ]
        
        for selector in header_selectors:
            print(f"\nTrying header selector: {selector}")
            try:
                headers = WebDriverWait(driver, 5).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                )
                print(f"Found {len(headers)} headers with selector '{selector}'")
                for header in headers:
                    print(f"Header text: {header.text.strip()}")
                break
            except Exception as e:
                print(f"Selector '{selector}' failed: {e}")
        
        # Try different cell selectors
        cell_selectors = [
            "[data-testid^='gridCell-'] .truncate",
            ".dataRow .cell.read .truncate",
            ".dataRow .cell.primary.read .truncate",
            "[data-columntype='multilineText'] .truncate"
        ]
        
        for selector in cell_selectors:
            print(f"\nTrying cell selector: {selector}")
            try:
                cells = WebDriverWait(driver, 5).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                )
                print(f"Found {len(cells)} cells with selector '{selector}'")
                if len(cells) > 0:
                    print("\nFirst few cells:")
                    for cell in cells[:5]:
                        print(f"Cell text: {cell.text.strip()}")
                break
            except Exception as e:
                print(f"Selector '{selector}' failed: {e}")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    test_selectors() 