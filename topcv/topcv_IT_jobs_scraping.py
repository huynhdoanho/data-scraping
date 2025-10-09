from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import time

def extract_job_inf(job):
    title = job.find('h3', class_='title').find('span')['title']
    link = job.find('h3', class_='title').find('a')['href']
    company = job.find('a', class_='company').get_text(strip=True)
    salary = job.find('label', class_='title-salary').get_text(strip=True)
    location = job.find('label', class_='address truncate').get_text(strip=True)
    exp = job.find('label', class_='exp').get_text(strip=True)
    
    return {
        'title': title,
        'link': link,
        'company': company,
        'salary': salary,
        'location': location,
        'exp': exp
    }
    
def scrape_jobs(max_page):
    jobs_data = []
    for page in range(1, max_page+1):
        url = f"https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?type_keyword=1&page={page}&category_family=r257"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        jobs = soup.find_all('div', attrs={"data-box":"BoxSearchResult"}) # max(len(jobs))=50, 50 is the maximum number of jobs in one single page
        
        for job in jobs:
            jobs_data.append(extract_job_inf(job))
        
        time.sleep(5)
    
    return pd.DataFrame.from_dict(jobs_data)


max_page = 5
jobs_df = scrape_jobs(max_page)
current_time = datetime.now().strftime("%Y%m%d%H%M%S")
jobs_df.to_csv(f"./data/topcv_IT_jobs_{current_time}.csv", index=False)
