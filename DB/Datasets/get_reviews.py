"""
File: get_reviews.py
Date: 09-12-2024
Description: Get movie reviews from IMDb or rotten tomatoes
"""

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




# # ## IMDb User Reviews Scraper

# import pandas as pd #Using panda to create our dataframe
# # Import Selenium and its sub libraries
# import selenium
# from selenium import webdriver
# # Import BS4
# import requests #needed to load the page for BS4
# from bs4 import BeautifulSoup

# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.common.by import By
# import re
# import time

# # ### Retrieve URLs using IMDbPY package


# #### Scraping Movie Reviews

# def get_review(url_df):
#     # First step, we will need to import all the necessary libraries:

#     # As we chose Chrome as our main web browser, we will need to download Chrome driver and tell Selenium where to find it:

#     PATH = r"C:/chromedriver/chromedriver.exe"  # path to the webdriver file

#     '''
#     Get the review from input as url for IMDB movies list.
#     The function takes 2 input the url of the movies and the name of the folder to store the data
#     For each folder, the function will grab the review for each movies and store into respective file.
#     '''

#     #Set initial empty list for each element:
#     title = []
#     link = url_df['imdb_url']
#     year = []      

#     # After that, we can use BeautifulSoup to extract the user reviews link 
#     #Set an empty list to store user review link
#     user_review_links = []
#     for i in range(len(url_df)):
#         review_link = url_df['imdb_url'][i]+'reviews/?ref_=tt_ql_2'
            
#         #Append the newly grabed link into its list
#         user_review_links.append(review_link)

#     url_df['review_link'] = user_review_links
    
#     # Step 2, we will grab the data from each user review page
#     # Use Selenium to go to each user review page
#     for i in range(0,len(url_df['review_link'])): 
            
#         service = Service(PATH)
#         options = webdriver.ChromeOptions()
#         driver = webdriver.Chrome(service=service, options=options)
#         print(url_df['review_link'][i])
#         driver.get(url_df['review_link'][i])
#         driver.implicitly_wait(2) # tell the webdriver to wait for 1 second for the page to load to prevent blocked by anti spam software


#         # Set up action to click on 'load more' button
#         # note that each page on imdb has 25 reviews
#         page = 1 #Set initial variable for while loop
#         #We want at least 1000 review, so get 50 at a safe number
#         while page<50:  
#             try:
#                 #find the load more button on the webpage
#                 load_more = driver.find_element(By.ID,'load-more-trigger')
#                 #click on that button
#                 load_more.click()
#                 page+=1 #move on to next loadmore button
#             except:
#                 #If couldnt find any button to click, stop
#                 print("No button to click! Page ", page)
#                 break
#         # After fully expand the page, we will grab data from whole website
#         review = driver.find_elements(By.CLASS_NAME,'review-container')
#         #Set list for each element:
#         title = []
#         content = []
#         rating = []
#         date = []
#         user_name = []
        
#         #run for loop to get
#         if len(review) > 0:
#             for n in range(0,1100):
#                 try:
#                     #Some reviewers only give review text or rating without the other, 
#                     #so we use try/except here to make sure each block of content must has all the element before 
#                     #append them to the list

#                     #Check if each review has all the elements
#                     ftitle = review[n].find_element(By.CLASS_NAME,'title').text
#                     #For the review content, some of them are hidden as spoiler, 
#                     #so we use the attribute 'textContent' here after extracting the 'content' tag
#                     fcontent = review[n].find_element(By.CLASS_NAME,'content').get_attribute("textContent").strip()
#                     frating = review[n].find_element(By.CLASS_NAME,'rating-other-user-rating').text
#                     fdate = review[n].find_element(By.CLASS_NAME,'review-date').text
#                     fname = review[n].find_element(By.CLASS_NAME,'display-name-link').text


#                     #Then add them to the respective list
#                     title.append(ftitle)
#                     content.append(fcontent)
#                     rating.append(frating)
#                     date.append(fdate)
#                     user_name.append(fname)
#                 except:
#                     continue
#         #Build data dictionary for dataframe
#         data = {'User_name': user_name, 
#             'Review title': title, 
#             'Review Rating': rating,
#             'Review date' : date,
#             'Review_body' : content
#            }
#         #Build dataframe for each movie to export
#         review = pd.DataFrame(data = data)
#         movie = url_df['stripped_title'][i] #grab the movie name from the top50 list    
#         review['Movie_name'] = movie #create new column with the same movie name column    
#         mid = url_df['movie_id'][i]
#         review['movie_id'] = mid
#         review.to_csv(f'data.csv') #store them into individual file for each movies, so we can combine or check them later
#         driver.quit()


#     if __name__ == "__main__":
#         imdb_id = "0298148" # Shrek 2
#         reviews = fetch_imdb_reviews(imdb_id)
#         df = pd.DataFrame(reviews)
#         df.to_csv("df.csv")
#         print(reviews)
#         print("Done processing")