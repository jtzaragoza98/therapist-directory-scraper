import logging
import pandas as pd
import time
import datetime
import hashlib
from utility import DirectoryBuilder
from get_therapist_profile import TherapistPageScraper


# log results / performance across different levels of the program
logging.basicConfig(filename='../check_performance.log', filemode='w', encoding='utf-8', level=logging.INFO,
                    format='%(asctime)s - %(name)s- %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


class TherapistDirectory:
    def __init__(self, state, url_df, rescrape=True):
        """
        Parameters:
        - url_df (DataFrame): DataFrame w/ therapist URLs; columns should be ['Gender', 'URLs]
        - rescrape (boolean): used to determine if the re-scraping is needed. set to False in rescrape method to
            avoid infinite loop
        """
        self.unique_id = hashlib.sha256(datetime.datetime.now().strftime("%Y%m%d%H%M%S").encode()).hexdigest()[:10]

        # use the url_df to get therapist pages, scrape them, and store in therapist_profiles_df
        self.state = state  # for sake of passing as parameter when rescraping
        self.url_df = url_df
        self.therapist_profiles_df = pd.DataFrame(columns=DirectoryBuilder.get_therapist_profile_cols())
        self.populate_therapist_df(rescrape)

        # clean the therapist profiles dataframe
        self.therapist_profiles_df = DirectoryBuilder.clean_therapist_profile_dataframe(self.therapist_profiles_df,
                                                                                        self.state)
        # save the clean therapist profile df
        if rescrape:
            self.therapist_profiles_df.to_csv(f'../scraped_data/{state}_therapist_directory_'
                                              f'{datetime.datetime.now().date()}_{self.unique_id}.csv',
                                              index=False, header=True)

    def populate_therapist_df(self, rescrape):
        # let's go row by row and get therapist profile data to add to the master dataframe (therapist_profiles_df)
        program_start = time.time()

        for index, row in self.url_df.iterrows():
            try:
                current_therapist = TherapistPageScraper(row)  # a row contains row['Gender'] and row['URL']
                self.therapist_profiles_df = pd.concat([self.therapist_profiles_df,
                                                        current_therapist.therapist_data], ignore_index=True)
            except:
                self.therapist_profiles_df = pd.concat([self.therapist_profiles_df,
                                                        DirectoryBuilder.failed_scrape_output(row['URL'])],
                                                       ignore_index=True)
            time.sleep(10)  # avoid overloading psychology today's server

        if rescrape:
            self.rescrape_program_failures()

        self.therapist_profiles_df.fillna('N/A', inplace=True)

        program_end = time.time()

        logging.info(f'$|$ Function: Program Efficiency / Time | Time: {program_end - program_start} | '
                     f'Therapists Scraped: {self.url_df.shape[0]}')

    def rescrape_program_failures(self):
        # normally a web request handshake issue or the therapist profile was removed
        failed_scrapes = self.therapist_profiles_df[self.therapist_profiles_df['therapist_name'].str.contains(
            'program failure', case=False, na=False, regex=True)][['therapist_url', 'therapist_gender']].copy()
        failed_scrapes = failed_scrapes.rename(columns={'therapist_url': 'URL', 'therapist_gender': 'Gender'})
        if failed_scrapes.empty:
            return

        # try rescraping the affected profiles
        rescrape = TherapistDirectory(self.state, url_df=failed_scrapes, rescrape=False)
        rescraped_profiles = rescrape.therapist_profiles_df

        # overwrite the successful rescrape...otherwise will redundantly overwrite program failure again
        if not rescraped_profiles.empty:
            for index, row in self.therapist_profiles_df.iterrows():
                therapist_url = row['therapist_url']
                if therapist_url in set(rescraped_profiles['therapist_url']):
                    try:
                        rescraped_row = rescraped_profiles[rescraped_profiles['therapist_url'] == therapist_url].iloc[0]
                        for col in self.therapist_profiles_df.columns:
                            self.therapist_profiles_df.at[index, col] = rescraped_row[col]
                    except IndexError:  # profile removed from website since scraping URL
                        pass
