import datetime
import pandas as pd


class DirectoryBuilder:
    """methods for constructing/cleaning therapist profile dataframe and handling program-wide scraping failures"""

    @staticmethod
    def clean_therapist_profile_dataframe(therapist_profile_df, state):
        therapist_profile_df.fillna('N/A', inplace=True)
        failures = pd.DataFrame()

        # check for duplicates, program failure rows/failed scrapes, profiles that don't exist, buggy zipcodes
        mask1 = therapist_profile_df.duplicated(subset='therapist_url', keep=False)
        mask2 = ~therapist_profile_df.apply(lambda row: 'program failure' in row.values, axis=1)
        mask3 = therapist_profile_df['therapist_name'].notna()
        mask4 = therapist_profile_df['zipcode'].str.isdigit() | therapist_profile_df['zipcode'].isna()
        mask5 = ~therapist_profile_df.apply(lambda row: 'failed scrape' in row.values, axis=1)
        # compile list of failures
        failures = pd.concat([failures, therapist_profile_df[mask1]])
        failures = pd.concat([failures, therapist_profile_df[~mask2]])
        failures = pd.concat([failures, therapist_profile_df[~mask3]])
        failures = pd.concat([failures, therapist_profile_df[~mask4]])
        failures = pd.concat([failures, therapist_profile_df[~mask5]])
        # filter for clean rows, remove leading/trailing spaces
        therapist_profile_df = therapist_profile_df[~mask1 & mask2 & mask3 & mask4 & mask5]
        therapist_profile_df = therapist_profile_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        # save the defect rows / failed scrapes to a CSV file for review
        failures.to_csv(f'../scraped_data/{state}_therapist_directory_removed_'
                        f'{datetime.datetime.now().date()}.csv', index=False, header=True)

        return therapist_profile_df

    @staticmethod
    def get_therapist_profile_cols():
        # this is the columns used to create the therapist profile df
        columns = ['therapist_url',
                   'therapist_name',
                   'therapist_credentials',
                   'therapist_gender',
                   'description',
                   'ages_covered',
                   'issues_covered',
                   'therapy_types',
                   'available',
                   'in_person',
                   'online',
                   'phone_number',
                   'address',
                   'zipcode',
                   'insurance',
                   'languages_spoken',
                   'session_cost',
                   'ethnicities_served',
                   'faiths_served',
                   'lgbtq_status',
                   'veteran_status']
        return columns

    @staticmethod
    def failed_scrape_output(therapist_url):
        # when the whole darn scraper fails...we can investigate later. most the time, the profile was removed
        failed_scrape_data = {
            'therapist_url': therapist_url,
            'therapist_name': 'program failure',
            'therapist_credentials': 'program failure',
            'therapist_gender': 'program failure',
            'description': 'program failure',
            'ages_covered': 'program failure',
            'issues_covered': 'program failure',
            'therapy_types': 'program failure',
            'available': 'program failure',
            'in_person': 'program failure',
            'online': 'program failure',
            'phone_number': 'program failure',
            'address': 'program failure',
            'zipcode': 'program failure',
            'insurance': 'program failure',
            'languages_spoken': 'program failure',
            'session_cost': 'program failure',
            'ethnicities_served': 'program failure',
            'faiths_served': 'program failure',
            'lgbtq_status': 'program failure',
            'veteran_status': 'program failure'
        }
        return pd.DataFrame([failed_scrape_data])


class TextProcessing:
    """reference methods for text processing and data extraction"""

    @staticmethod
    def get_field_regex_pattern(field):
        # get the pertinent regex pattern to extract data from the text
        regex_patterns = {
            'address': r'### Primary Location\n(.*? \d{5})',
            'age': r'(?:Toddler|Children \(6 to 10\)|Preteen|Teen|Adults|Elders \(65\+\))',
            'availability': r'Practice at a Glance\n\n(.*?)\n\n#',
            'credentials': r'Next(.*?)#',
            'description': r'Verified by Psychology Today(.*?)##',
            'ethnicities': r'### Ethnicity(.*?)#',
            'faith': r'Religion\n\n(.*?)\n#',
            'insurance': r'## Finances(.*?)## Qualifications',
            'languages': r'I also speak\n\n(.*?)\n#',
            'lgbtq_status': r'\b[Ll][Gg][Bb][Tt][Qq]\+?\b',
            'name': r'Next(.*?)#',
            'phone_number': r'### Primary Location\n\n(.*?)\n\n(Email|My web|Website|#)',
            'session_cost': r'## Finances(.*?)## Qualifications',
            'therapy_types': r'Types of Therapy\n\n(.*?)\n\n(Ask|#)',
            'veteran_status': r'\b[Vv]eterans?\b'
        }
        return regex_patterns[field]

    @staticmethod
    def process_regex_match_text(match, field):
        # scrape the text using applicable field extraction logic
        data = match.group(1).strip()
        field_extractors = {
            'address': lambda: data.split('\n\n'),
            'availability': lambda: data,
            'credentials': lambda: (data.split('\n\n'))[1],
            'description': lambda: data[0:],  # TODO: convert to embeddings
            'ethnicities': lambda: [val.strip() for val in [val.strip(',') for val in data.split('\n')] if val.strip()],
            'faith': lambda: [val.strip() for val in data.replace('\n', '').split(',') if val.strip()],
            'insurance': lambda: [val.strip('* ').strip() for val in data.split('\n') if val.strip().startswith('*')],
            'languages': lambda: [val.strip() for val in data.replace('\n', '').split(',') if val.strip()],
            'lgbtq_status': lambda: data.split(','),
            'name': lambda: (data.split('\n\n'))[0],
            'phone_number': lambda: (data.split('\n\n'))[-1],
            'session_cost': lambda: [val.strip('* ').strip() for val in data.split('\n') if '$' in val],
            'therapy_types': lambda: [val.strip() for val in data.split('*') if val.strip()]
        }
        return field_extractors[field]

    @staticmethod
    def get_reference_data(ref_file):
        # get a list of valid values across ethnicities, faith, insurance, languages, and therapy types
        # reference tables will help limit the cardinality at the sake of excluding vals not listed by psych today
        with open(ref_file, 'r') as file:
            return {line.strip() for line in file}  # set is faster to search than list, remove duplicates by default
