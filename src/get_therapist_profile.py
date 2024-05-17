from fuzzywuzzy import fuzz
from langchain_community.document_loaders import AsyncHtmlLoader
from langchain_community.document_transformers import Html2TextTransformer
from utility import TextProcessing
import logging
import pandas as pd
import re


class TherapistPageScraper:
    """
    Parameters:
    - therapist_gender_and_url (DataFrame): DataFrame w/ columns ['Gender', 'URL']. URl used to scrape therapist page
    - show_text is used to show the literal page text whenever I need to debug a failed scrape. Usually this leads
        to me fixing the regex text mining pattern or field extraction logic for the corresponding field in the
        utility file methods
    """
    def __init__(self, therapist_gender_and_url, show_text=False):
        # initialize url and gender
        try:  # if going E2E from url_scraper csv->df
            self.therapist_url = therapist_gender_and_url['URL']
            self.therapist_gender = therapist_gender_and_url['Gender']  # easiest to pull is from url_scraper directly
        except:  # manually feeding to debug. just use a tuple
            self.therapist_url = therapist_gender_and_url[0]
            self.therapist_gender = therapist_gender_and_url[1]

        # get page data
        self.page_text = self.get_page_data(show_text)

        # scrape fields
        self.available, self.in_person, self.online = self.get_availability('availability')
        self.street_city, self.zipcode = self.get_address('address')
        self.credentials = self.get_simple_field('credentials')
        self.description = self.get_simple_field('description')
        self.ethnicities_served = self.get_fuzz_fields('ethnicities')
        self.faiths_served = self.get_fuzz_fields('faith')
        self.insurance = self.get_fuzz_fields('insurance')
        self.ages_covered = self.get_age('age')
        self.issues_covered = self.get_issues('issues')
        self.languages_spoken = self.get_fuzz_fields('languages')
        self.lgbtq_status = self.get_direct_match_field('lgbtq_status')
        self.name = self.get_simple_field('name')
        self.phone_number = self.get_simple_field('phone_number')
        self.session_cost = self.get_session_cost('session_cost')
        self.therapy_types = self.get_fuzz_fields('therapy_types')
        self.veteran_status = self.get_direct_match_field('veteran_status')

        # compile into a dataframe, which will be added as a row to the therapist directory
        scraped_data_dict = self.compile_data()
        self.therapist_data = pd.DataFrame([scraped_data_dict])

        # log profiles that had any failed field scrapes
        if (self.therapist_data == 'failed scrape').any().any():
            logging.info(f'$|$ URL: {self.therapist_url} | Failure Type: Failed Scrape \n'
                         f'Data: ({ {k: v for k, v in scraped_data_dict.items() if k != "description"} }) \n ________')

    def compile_data(self):
        scraped_data = {
            'therapist_url': self.therapist_url,
            'therapist_name': self.name,
            'therapist_credentials': self.credentials,
            'therapist_gender': self.therapist_gender,
            'description': self.description,
            'ages_covered': self.ages_covered,
            'issues_covered': self.issues_covered,
            'therapy_types': self.therapy_types,
            'available': self.available,
            'in_person': self.in_person,
            'online': self.online,
            'phone_number': self.phone_number,
            'address': self.street_city,
            'zipcode': self.zipcode,
            'insurance': self.insurance,
            'languages_spoken': self.languages_spoken,
            'session_cost': self.session_cost,
            'ethnicities_served': self.ethnicities_served,
            'faiths_served': self.faiths_served,
            'lgbtq_status': self.lgbtq_status,
            'veteran_status': self.veteran_status
        }
        return scraped_data

    """all data scraper methods down under"""

    def get_page_data(self, show_page_text=False):
        # HTMl2Text Transformer is used to get ALL the page text, which we then initialize to scrape
        page_transformed = Html2TextTransformer().transform_documents(AsyncHtmlLoader(self.therapist_url).load())
        page_text = page_transformed[0].page_content[0:-540]  # get rid of stuff at the end
        if show_page_text:
            print(f'ALL TEXT: \n\n\n {page_text} \n\n\n _________DONE___________\n\n\n')
        return page_text

    @staticmethod  # wrapper function around scraper methods (below). helpful to log performance
    def processor(func):
        def wrapper(self, *args, **kwargs):
            pattern = TextProcessing.get_field_regex_pattern(*args)  # get regex pattern to find match in text
            if 'age' in args:
                match = re.findall(pattern, self.page_text)
            else:
                match = re.search(pattern, self.page_text, re.DOTALL)  # see if there is a match
            data = func(self, *args, match, **kwargs)  # call the original function to try to scrape data
            return data  # return scraping results to og function, which routes to scrape_data_fields method/initializer
        return wrapper

    @processor
    def get_address(self, field, match=None):
        # street, city, zipcode
        if match:
            try:
                relevant_vals = TextProcessing.process_regex_match_text(match, field)()
                joined_address = ', '.join(relevant_vals)
                street_city = joined_address[:-6].strip()
                zipcode = joined_address[-5:].strip()
                return street_city, zipcode
            except:  # catch all
                return 'failed scrape', 'failed scrape'
        return 'N/A', 'N/A'

    @processor
    def get_age(self, field, match=None):
        # use a different regex pattern. just look for exact matches from small list. convert to set
        if match:
            try:
                if len(set(match)) != 0:
                    return set(match)
            except:
                return 'failed scrape'
        return 'N/A'


    @processor
    def get_direct_match_field(self, field, match=None):
        # we are looking for an exact match on text (LGBTQ, LGBTQ+, Veteran)
        if match:
            return 'Y'
        return 'N/A'

    @processor
    def get_availability(self, field, match=None):
        if match:
            try:  # in order: availability, in-person accessibility, online accessibility
                relevant_vals = TextProcessing.process_regex_match_text(match, field)()
                if relevant_vals.startswith('Waitlist') or relevant_vals.startswith('Currently unable'):
                    return 'N', 'N', 'N'
                elif relevant_vals.startswith('Available both'):
                    return 'Y', 'Y', 'Y'
                elif relevant_vals.startswith('Available online only'):
                    return 'Y', 'N', 'Y'
                elif relevant_vals.startswith('Available in-person'):
                    return 'Y', 'Y', 'N'
            except:  # catch all
                return 'failed scrape', 'failed scrape', 'failed scrape'
        return 'N/A', 'N/A', 'N/A'  # could not find pertinent info

    @processor
    def get_fuzz_fields(self, field, match=None):
        # a couple fields require fuzzy string matching with a reference file
        ref_file = TextProcessing.get_reference_data(f'../reference_data/{field}.txt')
        val_matches = set()
        if match:
            try:
                relevant_vals = TextProcessing.process_regex_match_text(match, field)()
                for val in relevant_vals:  # fuzzy string matching - find top ratio for vals matching ref val above 85
                    matches = [ref for ref in ref_file if fuzz.partial_ratio(val, ref) >= 85]
                    highest_ratio = max(matches, key=lambda ref: fuzz.partial_ratio(val, ref), default=None)
                    if highest_ratio:
                        val_matches.add(highest_ratio)
            except:  # catch all
                return 'failed scrape'
        if not val_matches:
            return 'N/A'
        return val_matches

    @processor
    def get_session_cost(self, field, match=None):
        if match:
            try:
                relevant_vals = TextProcessing.process_regex_match_text(match, field)()
                for val in relevant_vals:
                    if val.strip().startswith('Individual Sessions'):
                        return val[val.index('$'):].strip()
            except:  # catch all
                return 'failed scrape'
        return 'N/A'

    def get_issues(self, field):
        """we treat this value differently. since issues can appear in many places, we do all the mining
        and manipulation directly here to pull the key values"""
        ref_file = TextProcessing.get_reference_data(f'../reference_data/{field}.txt')
        issues = set()
        for val in ref_file:
            try:
                match = re.search(rf'\b{val}\b', self.page_text, re.DOTALL)
                if match:
                    issues.add(val)
            except:  # catch all
                return 'failed scrape'
        return issues

    @processor
    def get_simple_field(self, field, match=None):
        # a couple fields just require isolating one snippet of the text
        if match:
            try:
                val = TextProcessing.process_regex_match_text(match, field)()
                if field == 'phone_number':  # sometimes the phone number is missing
                    if not val.startswith('('):
                        return 'N/A'
                return val
            except:  # catch all
                return 'failed scrape'
        return 'N/A'
