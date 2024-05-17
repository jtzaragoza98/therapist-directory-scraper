from selenium import webdriver
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import time


class TherapistURLScraper:
    """
    Parameters:
    - state (str): State for which therapists' URLs need to be scraped.
        - if state is two words, use a dash to separate (north-carolina); keep lowercase
    - binary_pages (list): List of binary pages.
        - number of pages to pull for male and female therapists (20 therapists per page). this will iterate
                binary_pages * 2 times
    - non_binary_pages (list): List of non-binary pages.
        - number of pages to pull for non-binary therapists. default setting is 100, given there are significantly
            less non-binary therapists than male and female therapists

    Takes a while to run if trying to get a BIG list. 3-second timer between API call to avoid request overload
    """
    def __init__(self, state, binary_pages, non_binary_pages=20):
        # usually a lot less non-binary therapists
        self.state, self.binary_pages, self.non_binary_pages = state, binary_pages, non_binary_pages
        self.url_df = self.get_therapist_page_urls()  # save urls as CSV and return df

    def get_therapist_page_urls(self):
        # get therapist URLs by gender (male, female, non-binary)
        # we scrape first pages only b/c each webpage instance generates a new random unsorted list of therapists
        all_urls = []
        for page in range(1, self.binary_pages+1):
            try:
                self.get_urls('male', all_urls)
                time.sleep(10)  # api server limit
            except:  # usually an API request / handshake failure issue...not sure why
                pass

            try:
                self.get_urls('female', all_urls)
                time.sleep(10)  # api server limit
            except:
                pass

        for page in range(1, self.non_binary_pages+1):
            try:
                self.get_urls('non-binary', all_urls)
                time.sleep(10)  # api server limit
            except:
                pass

        # save as df, convert to csv, return df
        url_df = pd.DataFrame(all_urls, columns=['Gender', 'URL']).drop_duplicates(subset=['URL'])
        url_df.to_csv(f'../scraped_data/{self.state}_therapist_urls_{datetime.datetime.now().date()}.csv',
                      index=False, header=True)
        return url_df

    def get_urls(self, gender, url_list):
        urls = self.return_urls(f"https://www.psychologytoday.com/us/therapists/{self.state}?category={gender}")
        for url in urls:
            url_list.append((gender, url))
        return url_list

    @staticmethod
    def return_urls(url):
        # use this to get 20 URLs per page
        driver = webdriver.Chrome()
        time.sleep(1)
        driver.get(url)

        urls = []
        all_data = BeautifulSoup(driver.page_source, 'html.parser')
        info_divs = all_data.find_all("div", class_="results-row-info")
        for divs in info_divs:
            link = divs.find("a", class_="profile-title")  # get the actual URLs from the tags
            if link:
                href = link.get("href")
                urls.append(href)
        print(urls)
        return urls
