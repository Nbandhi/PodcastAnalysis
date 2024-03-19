from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
from datetime import date
from datetime import timedelta

# List of podcast from Podchaser.

best_podcasts = {'liberal': ['Pod Save America', 'The Daily', 'The Weeds', 'In The Thick',
                             'The NPR Politics Podcast', 'Free Talk Live',
                             'Slate News', '1619', 'The Bernie Sanders Show', 'Still Processing', 'The Bob Cesca Show',
                             'FiveThirtyEight Politics'],
                 'conservative': ['The Ben Shapiro Show', 'Verdict with Ted Cruz', 'The Dan Bongino Show',
                                  'Federalist Radio Hour',
                                  'The Michael Knowles Show', 'Guy Benson Show', 'Mark Levin Podcast',
                                  'Fireside Chat with Dennis Prager',
                                  'Conservative Review with Daniel Horowitz', 'The Candace Owens Show',
                                  'Rush Limbaugh - Timeless Wisdom', 'The Laura Ingraham Podcast']}
podcast = []

# URL for searching
host_url = 'https://podcasts.google.com/'
proxies = {
    'http': os.getenv('HTTP_PROXY')
}
headers = {
    'User-agent':
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 "
        "Safari/537.36 Edge/18.19582"
}


def web_scrapper():
    session = requests.Session()
    for key in best_podcasts:
        for item in best_podcasts[key]:
            params = {
                "q": item,
                "hl": "en",
                "start": ""
            }
            response = session.get(host_url, headers=headers, proxies=proxies, params=params)
            soup = BeautifulSoup(response.text, 'lxml')
            req_content = soup.find_all('a', {'class': 'pyK9td'})[0]['href']  # link to get all pods
            search_url = host_url + req_content
            soup = BeautifulSoup(requests.get(search_url).text, 'lxml')
            req_content = soup.find_all('a', {'role': 'listitem'})
            for content in req_content:
                try:
                    pub_date = content.find('div', {'class': 'OTz6ee'}).text
                except AttributeError:
                    pub_date = 'None'
                title = content.find('div', {'class': 'e3ZUqe'}).text
                try:
                    abstract = content.find('div', {'class': 'LrApYe'}).text
                except AttributeError:
                    abstract = 'None'
                link = content.find('div', {'jsname': 'fvi9Ef'})['jsdata'].split(';')[1]
                podcast.append({
                    'type': key,
                    'podcaster': item,
                    'title': title,
                    'pub_date': pub_date,
                    'abstract': abstract,
                    'pod_link': link
                })

    pods_df = pd.DataFrame.from_dict(podcast)

    today = date.today()

    # Convert pub date from Hours ago and Days ago to a date
    for i in range(pods_df.shape[0]):
        pub_date_str = pods_df.loc[i, 'pub_date']

        if pub_date_str.endswith('hours ago'):
            pods_df.loc[i, 'pub_date'] = today.strftime("%b %d, %Y")
        elif pub_date_str[0].isdigit():
            days_to_subtract = int(pub_date_str[0])
            pub_date = today - timedelta(days=days_to_subtract)
            pods_df.loc[i, 'pub_date'] = pub_date.strftime("%b %d, %Y")

    dir = os.getcwd()
    pods_df.to_csv(os.path.join(dir, 'politicalpodcasts.csv'), index=True)


if __name__ == "__main__":
    web_scrapper()
