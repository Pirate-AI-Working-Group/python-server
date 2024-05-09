import requests
import pandas as pd
from tqdm import tqdm
from owid import catalog

def load_data_from_catalog():
    seen = set()
    url = "https://ourworldindata.org/headerMenu.json"
    data = requests.get(url).json()
    cats_list = []
    for cat in data["categories"]:
        cats_list.append(cat["slug"])

        for entry in cat["entries"]:
            if entry["slug"] not in cats_list:
                cats_list.append(entry["slug"])
            for i in entry["slug"].split("-"):
                if i not in cats_list:
                    cats_list.append(i)

        for subcategory in cat["subcategories"]:
            if subcategory["slug"] not in cats_list:
                cats_list.append(subcategory["slug"])
            for i in subcategory["slug"].split("-"):
                if i not in cats_list:
                    cats_list.append(i)

    url = "https://ourworldindata.org/dods.json"
    data = requests.get(url).json()
    for key in data.keys():
        key = key.replace("_", "-")
        if key not in cats_list:
            cats_list.append(key)
        for i in key.split("-"):
            if i not in cats_list:
                cats_list.append(i)
    try:
        # Get unique slugs
        cats_list = list(set(cats_list))
        #  progress bar
        for cat_slug in tqdm(cats_list):
            try:
                results = catalog.find(cat_slug)
                for i in results.iloc:
                    if i.path not in seen:
                        tables = i.load()
                        path = i.path
                        version = i.version
                        for table in tables.items():
                            for r,row in enumerate(table):
                                for c,col in enumerate(row):
                                    print(r,c,col)
            except Exception:
                print(f"Failed to load {cat_slug}")
    except Exception as e:
        print(f"Error: {e}")

load_data_from_catalog()