"""
finds news websites and gets data from mediabiasfactcheck.com to find out if they are a reliable source and what their bias. 
it will pass this data to find the rss,atom,json feed and reddit post for the website and add it to the database and Train AI (not-included).
"""

import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import asyncio
import json
import utility.helper as helper

"""
will get data from mediabiasfactcheck.com on a potential news source
"""

def getPotentialNewsSourceData(html_data, title):
    soup = BeautifulSoup(html_data, 'html.parser')
    output = {}

    # Find the site URL
    possible_links = [
        "p:-soup-contains(\"Source:\") > a[rel*=\"noreferrer\"]",
        "p:-soup-contains(\"Source:\") > a[rel*=\"noopener\"]",
        "p:-soup-contains(\"source:\") > a[rel*=\"noreferrer\"]",
        "p:-soup-contains(\"source:\") > a[rel*=\"noopener\"]",
        "p:-soup-contains(\"source:\") > a[rel*=\"noopener\"]",
    ]
    for selector in possible_links:
        links = soup.select(selector)
        for link in links:
            link = link.get("href")
            output["domain"] = link

    try:
        data_dict = {}

        # Find the site bias data
        data_sector = [
            'div[class*="entry-content"] > p:-soup-contains("Factual Reporting")',
            'div.clearfix > p:-soup-contains("Factual Reporting"):has(span[style]):has(strong)',
            'div[class="entry-content"] > p:has(span[style])',
            'div[class*="entry-content"] > p:has(span[style])',
            'div[class*="entry-content"] > p:has(strong)',
        ]
        for selector in data_sector:
            for element in soup.select(selector):
                keys = []
                values = []
                # Extract text and strong elements
                key_elements = element.find_all(text=True, recursive=False)
                value_elements = element.select('strong')
                for key_element in key_elements:
                    key = key_element.get_text(strip=True)
                    if len(key) > 0:
                        keys.append(key)
                for value_element in value_elements:
                    value = value_element.get_text(strip=True)
                    if len(value) > 0:
                        values.append(value)
                if not keys or not values:
                    continue
                data_dict = dict(zip(keys, values))
        output["mediaBiasFactCheck"] = data_dict
    except Exception as e:
        print(e)

    return output
"""
will get data from mediabiasfactcheck.com and find get items in the list for center, right, right-center, left-center, left, pro-science
"""


async def main_mediaBiasFactCheck():
    urls = [
        ("https://mediabiasfactcheck.com/center/", "center"),
        ("https://mediabiasfactcheck.com/right/", "right"),
        ("https://mediabiasfactcheck.com/right-center/", "right-center"),
        ("https://mediabiasfactcheck.com/leftcenter/", "leftcenter"),
        ("https://mediabiasfactcheck.com/left/", "left"),
        ("https://mediabiasfactcheck.com/pro-science/", "pro-science"),
    ]
    datas = []
    header = {
        
    }
    for url, political_ailment in urls:
        
        async with helper.get_max_http_connections_semaphore():
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    html = await response.text()
        soup = BeautifulSoup(html, "html.parser")
        # loop over all the links in the table and add them to the list
        for link in soup.select("#mbfc-table > tbody > tr > td > span > a"):
            try:
                async with helper.get_max_http_connections_semaphore():
                    async with aiohttp.ClientSession() as session:
                        async with session.get(link.get("href")) as response:
                            html_data = await response.text()
                            ccc = getPotentialNewsSourceData(html_data, "")
                            ccc["political_ailment"] = political_ailment
                            datas.append(ccc)
            except Exception as e:
                print(e)
    for link in datas:
        await helper.task_init_run_callbacks("domain",link)
# asyncio.run(main_mediaBiasFactCheck())