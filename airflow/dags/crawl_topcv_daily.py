from datetime import datetime, timedelta
import sys
import os
import logging
from typing import List

# ensure repository root is on path so we can import main and classes
BASE_DIR = os.path.dirname(__file__)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import pymongo
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# import functions / scrapers from local project
from topcv.classes.NormalJobScraper import NormalJobScraper
from topcv.classes.PremiumJobScraper import PremiumJobScraper
from topcv.classes.BrandJobScraper import BrandJobScraper
import topcv.scrape as scrape_module
import requests
from bs4 import BeautifulSoup
import time

# Mongo config (reuse same as main.py)
MONGO_URI = "mongodb://admin:passwords@host.docker.internal:27017/?authSource=admin"
MONGO_DB = "topcv_db"
MONGO_COLL = "jobs"

default_args = {
    "owner": "topcv",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="crawl_topcv_daily",
    default_args=default_args,
    description="Daily crawl TopCV recent IT job postings and store into MongoDB",
    schedule_interval="0 1 * * *",  # daily 01:00
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
)


def crawl_recent_job_urls_callable(**kwargs) -> List[str]:
    """Call existing function in main.py to get recent job urls."""
    # use max_page configurable via dag_run conf or default to 1
    max_page = kwargs.get("dag_run").conf.get("max_page", 1) if kwargs.get("dag_run") else 1
    urls = scrape_module.crawl_recent_job_urls(max_page)
    logging.info("Found %d recent job urls", len(urls))
    # push to xcom automatically by return
    return urls


def scrape_jobs_callable(**kwargs) -> int:
    """Scrape each URL returned from crawl_recent_job_urls and insert into MongoDB.

    Returns number of inserted documents.
    """
    ti = kwargs["ti"]
    urls = ti.xcom_pull(task_ids="crawl_recent_job_urls")
    if not urls:
        logging.info("No URLs to scrape.")
        return 0

    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[MONGO_COLL]

    inserted = 0
    for url in urls:
        try:
            # decide scraper type
            # URLs from site use path segment to indicate brand vs normal
            if url and len(url) > 26 and url[21:29] == "viec-lam":
                scraper = NormalJobScraper(url)
            elif url and len(url) > 26 and url[21:26] == "brand":
                # detect premium
                response = requests.get(url)
                soup = BeautifulSoup(response.content, "html.parser")
                premium = soup.find("div", class_="premium-job")
                scraper = PremiumJobScraper(url) if premium else BrandJobScraper(url)
            else:
                # fallback to main scraper if available
                try:
                    # main_module.scrape_job exists in draft notebook; attempt call if present
                    scraper = None
                except Exception:
                    scraper = None

            if scraper is None:
                logging.warning("No scraper chosen for url: %s", url)
                continue

            job_data = scraper.scrape()
            if not job_data:
                logging.info("Scraper returned no data for %s", url)
                continue

            # add metadata for checking later
            job_data["_scraped_at"] = datetime.utcnow()
            # Insert into mongo
            coll.insert_one(job_data)
            inserted += 1
            logging.info("ID: %s, Inserted job for url: %s", job_data['_id'], url.split('?')[0])
        except Exception as exc:
            logging.exception("Error scraping url %s: %s", url, exc)
        finally:
            time.sleep(2)  # be polite

    logging.info("Total inserted: %d", inserted)
    return inserted


def check_db_callable(**kwargs) -> bool:
    """Check if MongoDB has at least the expected number of new records from this run.

    The expected number is taken from previous task xcom (scraped_count).
    """
    ti = kwargs["ti"]
    scraped_count = ti.xcom_pull(task_ids="scrape_jobs")
    try:
        scraped_count = int(scraped_count)
    except Exception:
        scraped_count = 0

    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[MONGO_COLL]

    # count documents added in last 2 days as a safety window
    cutoff = datetime.utcnow() - timedelta(days=2)
    recent_count = coll.count_documents({"_scraped_at": {"$gte": cutoff}})

    logging.info("Scraped_count from task: %s, recent_count in DB (last 2 days): %d", scraped_count, recent_count)

    # decide "sufficient" policy:
    # - if scraped_count > 0 => require recent_count >= scraped_count
    # - else require recent_count > 0 (some data present)
    if scraped_count > 0:
        ok = recent_count >= scraped_count
    else:
        ok = recent_count > 0

    if not ok:
        logging.warning("DB check failed. expected >= %d but found %d", scraped_count, recent_count)
    else:
        logging.info("DB check passed.")

    return ok


crawl_task = PythonOperator(
    task_id="crawl_recent_job_urls",
    python_callable=crawl_recent_job_urls_callable,
    provide_context=True,
    dag=dag,
)

scrape_task = PythonOperator(
    task_id="scrape_jobs",
    python_callable=scrape_jobs_callable,
    provide_context=True,
    dag=dag,
)

check_task = PythonOperator(
    task_id="check_db",
    python_callable=check_db_callable,
    provide_context=True,
    dag=dag,
)

crawl_task >> scrape_task >> check_task