import requests
from bs4 import BeautifulSoup
import time
import pymongo

def crawl_all_tuoitre_news_url() -> list:
    response = requests.get("https://tuoitre.vn")
    soup = BeautifulSoup(response.content, 'html.parser')
    titles = soup.find_all('h3', class_='box-title-text')
    urls = ["https://tuoitre.vn" + link.find('a').attrs['href'] for link in titles]
    return urls

def crawl_single_news(url):
    news = requests.get(url)
    soup = BeautifulSoup(news.content, 'html.parser') 

    title = soup.find('h1', class_='detail-title').text

    abstract = soup.find('h2', class_='detail-sapo').text

    raw_content = soup.find('div', class_='detail-content afcbc-body').find_all('p')    
    body = '\n'.join([p.text for p in raw_content if not p.has_attr("data-placeholder")])

    publish_date = soup.find('div', class_='detail-time').text.strip()

    category = soup.find('div', class_='detail-cate').text

    return {
        "_id": hash(url + str(publish_date)),
        "url": url,
        "content": {
            "title": title,
            "abstract": abstract,
            "body": body
        },
        "publish_date": publish_date,
        "category": category
    }

def insert_one_mongodb(connection, db, collection, data):
    db = connection[db]
    collection = db[collection]
    try:
        collection.insert_one(data)
    except pymongo.errors.DuplicateKeyError:
        print("Duplicate key error: Document with this _id already exists.")
        return False
    return True

def insert_news():
    connection = pymongo.MongoClient("mongodb://localhost:27017/")
    urls = crawl_all_tuoitre_news_url()
    for url in urls:
        news_data = crawl_single_news(url)
        if news_data:
            success = insert_one_mongodb(connection, "news_db", "tuoitre_news", news_data)
            if success:
                print("Inserted successfully")
            else:
                print("Failed to insert")
        time.sleep(3)
    connection.close()

insert_news()