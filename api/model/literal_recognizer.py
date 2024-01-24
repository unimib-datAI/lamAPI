import re
class LiteralRecognizer():
    
    # PATTERN TO MATCH DATES
    # dates like: '145 bc', '145.bc', '145,bc'
    # dates like: '1997-08-26', '1997.08.26', '1997/08/26'
    # dates like: '26/08/1997', '26.08.1997', '26-08-1997'
    # dates like: '26/08/97', '26.08.97', '26-08-97'
    # dates like: 'august 26 1997', 'august.26.1997', 'august,26,1997'
    # dates like: '26 august 1997', '26.august.1997', '26,august,1997'
    # dates like: '1997 august 26', '1997,august,26', '1997.august.26'
    # dates like: '1997 26 august', '1997,26,august', '1997.26.august'
    # dates like: 'august 1997', 'august.1997', 'august,1997'
    # dates like: '1997 august', '1997.august', '1997,august'
    # numbers like: '2,797,800,564', '2.797.800.564'
    # numbers like: '200,797,800', '200.797.800'
    # numbers like: '2,8', '2.8'
    # date's year: 1997
    # any pure number: 1345, 26, 1, 1990

    DATE_PATTERN = r'^\d{1,4}[\,\.\s\t\n]+bc$|' \
                r'^\d{4}[-.\/]\d{1,2}[-.\/]\d{1,2}$|' \
                r'^\d{1,2}[-.\/]\d{1,2}[-.\/]\d{4}$|' \
                r'^\d{1,2}[-.\/]\d{1,2}[-.\/]\d{2}$|' \
                r'^(january|february|march|april|may|june|july|august|september|october|november|dicember)[\.\,\s\t\n\/]+\d{1,2}[\.\,\s\t\n\/]+\d{4}$|' \
                r'^\d{1,2}[\.\,\s\t\n\/]+(january|february|march|april|may|june|july|august|september|october|november|dicember)[\.\,\s\t\n\/]+\d{4}$|' \
                r'^\d{4}[\.\,\s\t\n\/]+(january|february|march|april|may|june|july|august|september|october|november|dicember)[\.\,\s\t\n\/]+\d{1,2}$|' \
                r'^\d{4}[\.\,\s\t\n\/]+\d{1,2}[\.\,\s\t\n\/]+(january|february|march|april|may|june|july|august|september|october|november|dicember)$|' \
                r'^(january|february|march|april|may|june|july|august|september|october|november|dicember)[\.\,\s\n\t\/]+\d{4}$|' \
                r'^\d{4}[\.\,\s\n\t\/]+(january|february|march|april|may|june|july|august|september|october|november|dicember)$|' \
                r'^\d+[\.\,]\d+[\.\,]\d+[\.\,]\d+$|' \
                r'^\d+[\.\,]\d+[\.\,]\d+$|' \
                r'^\d+[\.\,]\d+$|' \
                r'^\d{4}[–-]\d{4}$|' \
                r'^\d{4}[–-](present|now)$|' \
                r'^\d{4}$|' \
                r'^\d+$'
    
    # PATTERN TO MATCH URLS
    URL_PATTERN = r'^((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])$'
    
    EMAIL_PATTERN = r'^\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b$'
    
    # PATTERN TO MATCH DATETIME
    DATETIME_PATTERN = r'^\d{4}[-.\/]\d{1,2}[-.\/]\d{1,2}T(24:00|2[0-3]:[0-5][0-9]|[0-1][0-9]:[0-5][0-9])$'
    
    
    # PATTERN TO MATCH TIME
    TIME_PATTERN = r'^(24:00|2[0-3]:[0-5][0-9]|[0-1][0-9]:[0-5][0-9])$'
    
    # PATTERN TO MATCH FLOAT NUMBERS
    FLOAT_NUMBER = r'^[-+]?\d+[\,\.]\d+$|' \
                r'^[-+]?\d+[\,\.]\d+[eE][-+]\d+$|' \
                r'^[-+]?\d+[\,\.]\d+[ ]+(km|km2|m|sq|mi|cm|cm2|mm|dm|ft)|' \
                r'^\d+[ ]+(km|km2|m|sq|mi|cm|cm2|mm|dm|ft)' \
    
    # PATTERN TO MATCH INTEGER NUMBERS
    INTEGER_NUMBER = r'^[-+]?\d+$|' \
                    r'^\d+[ –-](thousand|million|billion|trillion)$|' \
                    r'^\d+[\,\.]\d+[ –-](thousand|million|billion|trillion)$'
    
    datetime_pattern_to_match = re.compile(DATETIME_PATTERN, re.IGNORECASE)
    time_pattern_to_match = re.compile(TIME_PATTERN, re.IGNORECASE)
    date_pattern_to_match = re.compile(DATE_PATTERN, re.IGNORECASE)
    url_pattern_to_match = re.compile(URL_PATTERN, re.IGNORECASE)
    email_pattern_to_match = re.compile(EMAIL_PATTERN, re.IGNORECASE)
    integer_pattern_to_match = re.compile(INTEGER_NUMBER, re.IGNORECASE)
    float_pattern_to_match = re.compile(FLOAT_NUMBER, re.IGNORECASE)
    
    literal_types = {'datetime': datetime_pattern_to_match, 'time': time_pattern_to_match, 'url': url_pattern_to_match, 'email': email_pattern_to_match, 'float': float_pattern_to_match, 'integer': integer_pattern_to_match, 'date': date_pattern_to_match}
    
    @classmethod
    # check literals
    def check_literal(self, token):
        for key in self.literal_types:
            matches = self.literal_types[key].finditer(token)
            list_of_matches = list(matches)
            if len(list_of_matches) > 0:
                return key.upper()
        return 'STRING'