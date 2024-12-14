import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def fetch_imdb_reviews(imdb_id):
    url = f"https://www.imdb.com/title/tt{imdb_id}/reviews?ref_=tt_ql_3"

    # Configure Selenium options
    options = Options()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(url)

    # Wait for a specific element to confirm the page is loaded
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

    try:
        # Find all buttons with specific classes
        buttons = driver.find_elements(By.CLASS_NAME, "ipc-btn")

        # Filter buttons by the text content "All"
        show_all_button = None
        for button in buttons:
            if "All" in button.text:
                show_all_button = button
                break

        if show_all_button:
            # Click the button
            driver.execute_script("arguments[0].click();", show_all_button)
            time.sleep(2)  # Wait for the reviews to load
        else:
            print("No 'All' button found on the page.")

        # Parse the page source after loading all reviews
        soup = BeautifulSoup(driver.page_source, "html.parser")

    finally:
        driver.quit()

    # Extract reviews
    reviews = []
    review_divs = soup.find_all(class_="review-container")

    for review in review_divs:
        review_info = {
            "rating": None,
            "title": None,
            "text": None
        }

        # Extract rating
        rating_obj = review.find(class_="rating-other-user-rating")
        if rating_obj:
            review_info['rating'] = rating_obj.get_text(strip=True)

        # Extract title
        title_obj = review.find(class_="title")
        if title_obj:
            review_info['title'] = title_obj.get_text(strip=True)

        # Extract text
        text_obj = review.find(class_="text show-more__control")
        if text_obj:
            review_info['text'] = text_obj.get_text(strip=True)
        
        reviews.append(review_info)

    return reviews

if __name__ == "__main__":
    imdb_id = "0298148"  # Example: Shrek 2
    reviews = fetch_imdb_reviews(imdb_id)
    df = pd.DataFrame(reviews)
    df.to_csv("imdb_reviews.csv", index=False)
    print(df.head())
