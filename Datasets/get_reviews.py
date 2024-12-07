import requests
from bs4 import BeautifulSoup


if __name__ == "__main__":
    movie_id = "time_cut"
    url = f"https://www.rottentomatoes.com/m/{movie_id}#critics-reviews"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all review text elements
        reviews = soup.find_all('div', attrs={"data-qa": "review-text"})
        
        # Print each review
        for idx, review in enumerate(reviews, start=1):
            print(f"Review {idx}: {review.text.strip()}")
    else:
        print(f"Failed to fetch the page: {response.status_code}")

