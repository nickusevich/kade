import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time

headers = {
    'Referer': 'https://www.rottentomatoes.com/m/notebook/reviews?type=user',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
}

s = requests.Session()

def get_reviews(url):
    r = requests.get(url)
    movie_id_s = re.findall(r'(?<=movieId":")(.*)(?=","type)',r.text)
    if len(movie_id_s) > 0:
     movie_id = movie_id_s[0]
    else:
        print("Movie ID not found")
        return []

    api_url = f"https://www.rottentomatoes.com/napi/movie/{movie_id}/criticsReviews/all" #use reviews/userfor user reviews
    
    payload = {
        'direction': 'next',
        'endCursor': '',
        'startCursor': '',
    }
    
    review_data = []
    
    while True:
        r = s.get(api_url, headers=headers, params=payload)
        data = r.json()

        if not data['pageInfo']['hasNextPage']:
            break

        payload['endCursor'] = data['pageInfo']['endCursor']
        payload['startCursor'] = data['pageInfo']['startCursor'] if data['pageInfo'].get('startCursor') else ''

        review_data.extend(data['reviews'])
        time.sleep(1)
    
    return review_data

data = get_reviews('https://www.rottentomatoes.com/m/interstellar_2014/reviews')
df = pd.json_normalize(data)


if __name__ == "__main__":
    data = get_reviews('https://www.rottentomatoes.com/m/interstellar_2014/reviews')
    df = pd.json_normalize(data)

