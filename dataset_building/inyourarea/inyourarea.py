import os
import sqlite3
import sys
import aiocsv
import aiofiles
import aiohttp
import asyncio
# import utility.helper as helper
# import handel_reddit
lock = asyncio.Lock()


async def GetlocalPublicationsPostcode(postcode):
    url = f"https://production.inyourarea.co.uk/api/localPublications/{postcode}"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0",
        "Accept": "*/*",
        "Accept-Language": "en-GB,en;q=0.5",
        "content-type": "application/json",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache"
    }
    for _ in range(30):
            try:
                await asyncio.sleep(10)
                async with aiohttp.ClientSession() as session:
                        async with session.get(url, headers=headers) as response:
                            status = response.status
                            data = await response.json()
                return data, status
            except Exception as e:
                print("inyourarea 1 error:", e)


async def GetlocalFeedPostcode(postcode: str, where):
    url = f"https://production.inyourarea.co.uk/facade/feed/{postcode}/{where}/news/all"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0",
        "Accept": "*/*",
        "Accept-Language": "en-GB,en;q=0.5",
        "content-type": "text/plain",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache"
    }
    for _ in range(30):
            try:
                await asyncio.sleep(10)
                async with aiohttp.ClientSession() as session:
                        async with session.get(url, headers=headers) as response:
                            status = response.status
                            data = await response.json()
                        return data, status
            except Exception as e:
                print("inyourarea 2 error:", e)


async def facade_feed_postcode(postcode):
    url = f"https://production.inyourarea.co.uk/facade/feed/{postcode}"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0",
        "Accept": "*/*",
        "Accept-Language": "en-GB,en;q=0.5",
        "content-type": "text/plain",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache"
    }
    for _ in range(30):
            try:
                await asyncio.sleep(10)
                async with aiohttp.ClientSession() as session:
                        async with session.get(url, headers=headers) as response:
                            status = response.status
                            data = await response.json()
                            return data, status
            except Exception as e:
                print("inyourarea 3 error:", e)
DB_FILE = "inyourarea.db"
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()
def make_table():
    # Create tables if they don't exist
    c.execute('''CREATE TABLE IF NOT EXISTS Publication (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    postcode TEXT
                 )''')
    c.execute('''CREATE TABLE IF NOT EXISTS Article (
                    id INTEGER PRIMARY KEY,
                    publication_id INTEGER,
                    title TEXT,
                    url TEXT,
                    tags TEXT,
                    primary_place_concept_uri TEXT,
                    FOREIGN KEY (publication_id) REFERENCES Publication(id)
                 )''')

async def main_inyourarea():
    async with aiofiles.open("./data/ukpostcodes.csv", mode='r') as csv_file:
        reader = aiocsv.AsyncReader(csv_file)
        async for row in reader:
            try:
                postcode = row[1].replace(" ", "")
                publications, status = await GetlocalPublicationsPostcode(postcode)
                if status != 200:
                    continue
                for publication in publications:
                    # Insert publication data into the Publication table
                    c.execute("INSERT INTO Publication (name, postcode) VALUES (?, ?)",
                              (publication['name'], postcode))
                    publication_id = c.lastrowid
                feed, status = await facade_feed_postcode(postcode)
                if status != 200:
                    continue
                locations = [i["name"] for i in feed["parms"]["props"]["locations"]]
                where = ",".join(locations)
                feed, status = await GetlocalFeedPostcode(postcode, where)
                if status != 200:
                    continue
                if feed is None:
                    continue
                if "parms" in feed.keys():
                    for feed_item in feed["parms"]["asyncProps"]["feed"]:
                        if feed_item["type"] == "articles":
                            for relatedItem in feed_item["relatedItems"]:
                                # Insert article data into the Article table
                                c.execute("INSERT INTO Article (publication_id, title, url, tags, primary_place_concept_uri) VALUES (?, ?, ?, ?, ?)",
                                          (publication_id, relatedItem["title"], relatedItem["url"], relatedItem["tags"], relatedItem.get("primary_place_concept_uri")))
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                print("main_inyourarea:", e)

    # Commit changes and close the database connection
    conn.commit()
    conn.close()
