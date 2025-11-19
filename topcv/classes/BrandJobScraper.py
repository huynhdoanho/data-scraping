from bs4 import BeautifulSoup
import requests
import time
from datetime import datetime, timedelta
import random
from classes.utils import url_to_id_short


class BrandJobScraper:
    """
    input: url of premium job detail page
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
        self.job = self.soup.find('div', class_='block-left')

        if self.job is None:
            print('Job detail not found: ' + self.url)
            return False
        
        return True
    
    def extract_job_details(self):
        jd = {}
        general_info = {}
        job_tags = []
    
        job_title = self.job.find('h2', class_='title').get_text(strip=True)
        general_info['Job Title'] = job_title   

        # address 
        address_div = self.job.find('div', class_='box-job-info').find('div', class_='box-address')
        address = "\n".join([a.get_text(strip=True) for a in address_div.find_all()])
        general_info['Location'] = address

        details = self.job.find('div', class_='box-job-info').find_all('div', class_='box-info')
        # theo nhu exploration thi cac class 'box-info' trong phan job details gom: general info, job tags va job description
        for section in details:
            # general info
            if section.find('div', class_='box-main'):
                items = section.find('div', class_='box-main').find_all('div', class_='box-item')
                for item in items:
                    label = item.find('strong').get_text(strip=True)
                    value = item.find('span').get_text(strip=True)
                    # print(f"{label}: {value}")
                    general_info[label] = value

            # jd    
            else:
                title = section.find('h2').get_text(strip=True)
                content_div = section.find('div', class_='content-tab').find_all()
                content = ""
                for part in content_div:
                    if part.name == 'ul':
                        lis = part.find_all('li')
                        for li in lis:
                            content += "- " + li.get_text(strip=True) + "\n"
                    elif part.name == 'div' or part.name == 'p':
                        content += part.get_text(strip=True) + "\n"
                jd[title] = content

                # custom form job (job co job khong)
                cfj = section.find_all('div', class_='custom-form-job__item')
                if len(cfj) > 0:    
                    for div in cfj:
                        title = div.find('h3').get_text(strip=True) 
                        content = div.find('div', class_='custom-form-job__item--content').get_text(strip=True)
                        # print(f"{title}: {content}")
                        jd[title] = content

                # job tags (phan nay co trong "mo ta cong viec")
                if section.find('div', class_='job-tags'):
                    tags = section.find('div', class_='job-tags').find_all('a')
                    for tag in tags:
                        # print(tag.get_text(strip=True))
                        job_tags.append(tag.get_text(strip=True))

        return jd, general_info, job_tags
    
    def extract_company_info(self):
        # company (phan nay trong footer)
        company = {}
        footer = self.soup.find('div', class_='footer-info')
        company['name'] = footer.find('div', class_='footer-info-content footer-info-company-name').get_text(strip=True)
        title_divs = footer.find_all('div', class_='footer-info-title')

        for title in title_divs:
            if title.find_next('div').get('class')[0] == 'footer-info-content':
                company[title.get_text(strip=True)] = title.find_next('div').get_text(strip=True)

        return company
    
    def scrape(self):   
        if not self.load():
            return None
        
        jd, general_info, job_tags = self.extract_job_details()
        company = self.extract_company_info()

        return {
            '_id': url_to_id_short(self.url),
            'date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
            'url': self.url,
            'company_info': company,
            'general_info': general_info,
            'job_description': jd,
            'tags': job_tags
        }


# if __name__ == "__main__":
#     url = "https://www.topcv.vn/brand/congtycophanmisa/tuyen-dung/nhan-vien-phat-trien-doi-tac-ho-kinh-doanh-thu-nhap-25-30-trieu-thang-j1928082.html?ta_source=JobSuggestInSearchListNoResult_LinkDetail&u_sr_id=rEbGzfWKeHtGTDVcK5ZYKLlLiykPrS5lfK8WM2kX_1761641151"
#     scraper = PremiumJobScraper(url)
#     data = scraper.scrape()
#     print(data)