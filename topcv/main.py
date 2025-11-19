import requests
import time
from classes.NormalJobScraper import NormalJobScraper
from classes.PremiumJobScraper import PremiumJobScraper
from classes.BrandJobScraper import BrandJobScraper
from bs4 import BeautifulSoup
import pymongo


def crawl_recent_job_urls(max_page):
    job_detail_urls = []

    for page in range(1, max_page+1):
        page_url = f"https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?sort=new&type_keyword=1&page={page}&category_family=r257"
        response = requests.get(page_url)
        soup = BeautifulSoup(response.content, "html.parser")

        jobs = soup.find_all('div', attrs={"data-box":"BoxSearchResult"}) # len(jobs)=50, the maximum number of jobs in one single page
        
        for job in jobs:    

            # get updated at
            label = job.find("label", class_="address mobile-hidden label-update")
            # chỉ lấy text trong label, bỏ qua text trong span
            updated_at = [t for t in label.stripped_strings if t not in label.span.stripped_strings][0]

            if updated_at == '1 ngày trước':
                url = job.find('h3', class_='title').find('a')['href']
                job_detail_urls.append(url)

                # print('url: ', url)
                # print('updated at: ', updated_at)
                # print('-----------------------')

    return job_detail_urls


if __name__ == "__main__":

    # mongodb config
    connection = pymongo.MongoClient("mongodb://admin:passwords@localhost:27017/")
    db = connection["topcv_db"]
    coll = db['jobs']

    # crawl 
    max_page = 1
    data = []
    urls = crawl_recent_job_urls(max_page)

    for url in urls:
        try:    
            if url[21:29] == 'viec-lam':
                scraper = NormalJobScraper(url)
                job_data = scraper.scrape()
                if job_data: 
                    data.append(job_data)
                    coll.insert_one(job_data)
                    print(f"Inserted job with ID: {job_data['_id']}")

            elif url[21:26] == 'brand':
                # detecting normal brand job or premium brand job
                response = requests.get(url)
                soup = BeautifulSoup(response.content, "html.parser")
                premium = soup.find('div', class_='premium-job')

                if premium:
                    scraper = PremiumJobScraper(url)
                else:
                    scraper = BrandJobScraper(url)

                job_data = scraper.scrape()

                if job_data: 
                    data.append(job_data)
                    coll.insert_one(job_data)
                    print(f"Inserted job with ID: {job_data['_id']}")
            
            time.sleep(2)
            print(f"Scraped {len(data)} job postings.")
        except Exception as e:
            print(f"Error processing URL {url}: {e}")
