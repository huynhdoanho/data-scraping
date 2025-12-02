from bs4 import BeautifulSoup
import requests
import time
from datetime import datetime, timedelta
import random
import sys
import os

# ensure repository root is on path so we can import main and classes
BASE_DIR = os.path.dirname(__file__)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from utils import url_to_id_short


class NormalJobScraper:
    """
    input: url of normal job detail page
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
        self.job = self.soup.find('div', class_='job-detail__body')

        if self.job is None:
            print('Job detail not found: ' + self.url)
            return False
        
        return True
    
    def extract_company_info(self):
        company_name = self.job.find('div', class_='company-name-label').find('a', class_='name').get_text(strip=True)
        company_scale = self.job.find('div', class_='job-detail__company--information-item company-scale').find('div', class_='company-value').get_text(strip=True)
        company_address = self.job.find('div', class_='job-detail__company--information-item company-address').find('div', class_='company-value').get_text(strip=True)
        company_field = self.job.find('div', class_='job-detail__company--information-item company-field').find('div', class_='company-value').get_text(strip=True)
        return {
            'company_name': company_name,
            'company_scale': company_scale,
            'company_address': company_address,
            'company_field': company_field
        }
    
    def extract_job_info(self):
        job_title = self.job.find('h1', class_='job-detail__info--title').get_text(strip=True, separator=' ')

        infs = self.job.find_all('div', class_='job-detail__info--section-content-value')
        salary = infs[0].get_text(strip=True)
        location = infs[1].get_text(strip=True)
        exp = infs[2].get_text(strip=True)

        # others general info
        general_inf = {}
        all_general_inf = self.job.find('div', class_='job-detail__box--right job-detail__body-right--item job-detail__body-right--box-general').find_all('div', class_='box-general-group-info')
        
        for inf in all_general_inf:
            title = inf.find('div', class_='box-general-group-info-title').get_text(strip=True)
            value = inf.find('div', class_='box-general-group-info-value').get_text(strip=True)
            general_inf[title] = value

        return {
            'job_title': job_title,
            'salary': salary,
            'location': location,
            'experience': exp,
            'general_info': general_inf
        }
    
    def extract_jd(self):
        jd = {}
        all_sections = self.job.find('div', class_='job-description').find_all('div', class_='job-description__item') # sections in jd part of html
        for section in all_sections:
            title = section.find('h3').get_text(strip=True)
            if section.find_all('li'):
                content = "\n".join([c.get_text(strip=True) for c in section.find_all('li')])
            else:
                content = section.find('div').get_text(strip=True, separator='\n')
                
            jd[title] = content
            
        # custom form job (job co job khong)
        cfj = self.job.find_all('div', class_='custom-form-job__item')
        if len(cfj) > 0:    
            for form in cfj:
                title = form.find('h3').get_text(strip=True)
                content = form.find('div', class_='custom-form-job__item--content').get_text(strip=True)

                jd[title] = content
            
        return jd
    
    def extract_categories(self):
        categories = {}
        
        categories_box = self.job.find('div', class_="job-detail__box--right job-detail__body-right--item job-detail__body-right--box-category")\
                            .find_all('div', class_=['box-category', 'box-category-collapsed'])
        for category in categories_box:
            title = category.find('div', class_='box-title').get_text(strip=True)
            a = category.find('div', class_='box-category-tags').find_all('a')
            if a:
                tags = [tag.get_text(strip=True) for tag in a]
            else:
                tags = [tag.get_text(strip=True) for tag in category.find('div', class_='box-category-tags').find_all('span')]
            categories[title] = tags

        return categories
    
    def scrape(self):
        if not self.load():
            return None
        
        company_info = self.extract_company_info()
        job_info = self.extract_job_info()
        jd = self.extract_jd()
        categories = self.extract_categories()

        self.job.clear()
        
        return {
            '_id': url_to_id_short(self.url),
            'date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
            'url': self.url,
            'company_info': company_info,
            'job_info': job_info,
            'job_description': jd,
            'categories': categories
        }
    

# if __name__ == "__main__":
#     url = "https://www.topcv.vn/viec-lam/technical-leader-net-tu-2-nam-kinh-nghiem/1909066.html?ta_source=JobSearchList_LinkDetail&u_sr_id=rMz6eDbuOjNwaeom2JX7rwDkYteS1TuOLwzyeU9V_1761462474"
#     scraper = NormalJobScraper(url)
#     result = scraper.scrape()
#     print(result)
