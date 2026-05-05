import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import os

BASE_URL = "https://books.toscrape.com/catalogue/page-{}.html"

def scrape_page(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    products = []

    for item in soup.find_all("article", class_="product_pod"):
        title = item.h3.a["title"]
        price = item.find("p", class_="price_color").text
        stock = item.find("p", class_="instock availability").text.strip()
        rating = item.find("p", class_="star-rating")["class"][1] #class="star-rating Two"


        rating_map = {
            "One": 1,
            "Two": 2,
            "Three": 3,
            "Four": 4,
            "Five": 5
        }

        rating = rating_map.get(rating , 0  ) #default 0 if not found

        products.append({
            "title": title,
            "price": price,
            "stock": stock,
            "rating": rating,
            "scrape_time": str(datetime.now())
        })

    return products


def scrape(pages=2):
    all_products = []

    for page in range(1, pages + 1):
        url = BASE_URL.format(page)
        print(f"Scraping: {url}")

        products = scrape_page(url)
        all_products.extend(products)

    return all_products


def save(data):
    os.makedirs("data_lake/raw", exist_ok=True)
    filename = f"data_lake/raw/books_{datetime.now().date()}.json"

    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved: {filename}")


if __name__ == "__main__":
    data = scrape(pages=2)
    save(data)