from get_therapist_directory import TherapistDirectory
from get_therapist_urls import TherapistURLScraper

# get URLs, build Therapist Directory
if __name__ == "__main__":
    therapist_urls = TherapistURLScraper('north-carolina', 250, 100)
    url_df = therapist_urls.url_df
    TherapistDirectory('north-carolina', url_df)
