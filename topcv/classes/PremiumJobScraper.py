from bs4 import BeautifulSoup
import uuid
import requests
import time
from datetime import datetime, timedelta
import random
from classes.utils import url_to_id_short


class PremiumJobScraper:
    """
    input: url of brand job detail page
    output: dict contains company info, job info, job description, categories
    flow: fetch url => load job in html fetched => parsing to extract data
    """

    def __init__(self, url):
        self.url = url.split('?')[0]
        self.soup = None
        self.job = None

    def fetch(self):
        headers = {
            'User-Agent': ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0',
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1 Safari/605.1.15',
                        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36']
            }
        response = requests.get(self.url, headers={'User-Agent': random.choice(headers['User-Agent'])})

        # handle request failure
        if response.status_code != 200:
            for i in range(5):
                print('Failed to retrieve: ' + self.url)
                print('Will retry after 60s...')
                time.sleep(60)
                response = requests.get(self.url)
                if response.status_code == 200:
                    break

            if response.status_code != 200:
                print('Failed to retrieve after 5 retries: ' + self.url)
                return None
            
        return response.content
    
    def load(self):
        response_content = self.fetch()
        if response_content is None:
            print('Failed to load page content: ' + self.url)
            return None
        
        self.soup = BeautifulSoup(response_content, "html.parser")
        self.job = self.soup.find('div', class_='premium-job')

        if self.job is None:
            print('Job detail not found: ' + self.url)
            return False
        
        return True
    
    def extract_jd(self):
        jd = {}
        boxes = self.job.find_all('div', class_='premium-job-description__box')
        for box in boxes:
            label = box.find('h2').get_text(strip=True)
            if box.find_all('li'):
                content = "\n".join([c.get_text(strip=True) for c in box.find_all('li')])
            else:
                content = box.find('div').get_text(strip=True, separator='\n')

            jd[label] = content
        return jd
    
    def extract_general_info(self):
        general_info = {}

        job_title = self.job.find('h2', class_='premium-job-basic-information__content--title').get_text(strip=True)
        general_info['job_title'] = job_title

        info_sections = self.job.find('div', class_='premium-job-basic-information__content--sections').find_all('div', class_="basic-information-item")
        salary = info_sections[0].find('div', class_='basic-information-item__data--value').get_text(strip=True)
        location = info_sections[1].find('div', class_='basic-information-item__data--value').get_text(strip=True)
        exp = info_sections[2].find('div', class_='basic-information-item__data--value').get_text(strip=True)
        general_info['salary'] = salary
        general_info['location'] = location
        general_info['experience'] = exp

        # other general info in div class 'general-information-data'
        general_info_data = self.job.find_all('div', class_='general-information-data')
        for data in general_info_data:
            label = data.find('div', class_='general-information-data__label').get_text(strip=True)
            value = data.find('div', class_='general-information-data__value').get_text(strip=True)
            general_info[label] = value
        
        return general_info
    
    def extract_tags(self):
        tags = {}

        job_tags = [tag.get_text(strip=True) for tag in self.job.find('div', class_='job-tags').find_all('a')]
        tags['job_tags'] = job_tags
        
        related_tags = {}
        divs = self.job.find_all('div', class_=["premium-job-related-tags__section", "premium-job-related-tags__section box-category collapsed"])
        for section in divs:
            title = section.find('h2', class_='premium-job-box__title').get_text(strip=True)
            items = [tag.get_text(strip=True) for tag in section.find_all(class_='tag-item')]
            related_tags[title] = items
        tags['related_tags'] = related_tags

        return tags
    
    def scrape(self):
        if not self.load():
            return None
        
        general_info = self.extract_general_info()
        jd = self.extract_jd()
        tags = self.extract_tags()

        return {
            '_id': url_to_id_short(self.url),
            'date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
            'url': self.url,
            'general_info': general_info,
            'job_description': jd,
            'tags': tags
        }

# # Example usage:
# if __name__ == "__main__":
#     url = "https://www.topcv.vn/brand/topcv/tuyen-dung/product-owner-j1873033.html?ta_source=JobSuggestInSearchListNoResult_LinkDetail&u_sr_id=XlqkQDmwyKqXNcyWe9j79alm2eDASBKsA1dgaHTF_1761583047"
#     scraper = PremiumJobScraper(url)
#     job_data = scraper.scrape()
#     print(job_data)