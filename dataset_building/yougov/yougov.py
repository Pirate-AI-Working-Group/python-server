import sqlite3
import aiohttp


headers = {
    'authority': 'yougov.co.uk',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-GB,en;q=0.9',
    'cache-control': 'no-cache',
    'cookie': 'visid_incap_1861213=1m7FPD3WRXuq2Cy8AhgaLL7yE2UAAAAAQUIPAAAAAAAo+2B/8YI8A5rCkeXqn/IR; OptanonAlertBoxClosed=2023-09-27T09:16:02.013Z; ajs_user_id=null; ajs_group_id=null; ajs_anonymous_id=%22da1eb0af-3e19-4748-a179-66c2bb4f5134%22; _mkto_trk=id:464-VHH-988&token:_mch-yougov.co.uk-1695806168554-65436; _ga=GA1.1.1925496614.1695806169; cb_user_id=null; cb_group_id=null; cb_anonymous_id=%2228796516-5998-4dff-ba86-259547626913%22; incap_ses_454_1861213=JIzsQgRLr3AgtTfyTfBMBgdZNmUAAAAAlV/aqJFGv6GduALg4V3kKw==; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Oct+23+2023+12%3A29%3A31+GMT%2B0100+(British+Summer+Time)&version=202308.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=507db960-74a1-4dec-a44a-8b114ebfeaba&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2C0003%3A1%2C0004%3A1&geolocation=%3B&AwaitingReconsent=false; _vwo_uuid_v2=D35AA70B7F89FDD8ADCF52ACC2CC37E84|f9b6946d3538b6ff32a0145754cc963f; _vis_opt_test_cookie=1; _hjSessionUser_1656737=eyJpZCI6ImD96DB8D504F427B7B84622A1E9C7E13CE%7C9cfdfc91c60a61bc66c014254ddbf897(2sg)l~2%7C(351)u~D96DB8D504F427B7B84622A1E9C7E13CE(1p78)m~3%241695806165%3A81.67070072%3A%3A(2cE)k~*(NtN)n~2254406%3A1(EP; _ga_NZLE1BRHWP=GS1.1.1698060572.2.0.1698060572.0.0.0; incap_ses_802_1861213=VKiLSmUMkDUqKVBH3EchCz1cNmUAAAAAj8pUgcOwQRvlJDVsIenYQw==',
    'dnt': '1',
    'pragma': 'no-cache',
    'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': 'Linux',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
}
cookies = {
    "visid_incap_1861213": "1m7FPD3WRXuq2Cy8AhgaLL7yE2UAAAAAQUIPAAAAAAAo+2B/8YI8A5rCkeXqn/IR",
    "OptanonAlertBoxClosed": "2023-09-27T09:16:02.013Z",
    "ajs_user_id": None,
    "ajs_group_id": None,
    "ajs_anonymous_id": "da1eb0af-3e19-4748-a179-66c2bb4f5134",
    "_mkto_trk": "id:464-VHH-988&token:_mch-yougov.co.uk-1695806168554-65436",
    "_ga": "GA1.1.1925496614.1695806169",
    "cb_user_id": None,
    "cb_group_id": None,
    "cb_anonymous_id": "28796516-5998-4dff-ba86-259547626913",
    "incap_ses_454_1861213": "JIzsQgRLr3AgtTfyTfBMBgdZNmUAAAAAlV/aqJFGv6GduALg4V3kKw==",
    "OptanonConsent": "isGpcEnabled=0&datestamp=Mon+Oct+23+2023+12%3A29%3A31+GMT%2B0100+(British+Summer+Time)&version=202308.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=507db960-74a1-4dec-a44a-8b114ebfeaba&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2C+ C0003%3A1%2CC0004%3A1&geolocation=%3B&AwaitingReconsent=false",
    "_vwo_uuid_v2": "D35AA70B7F89FDD8ADCF52ACC2CC37E84|f9b6946d3538b6ff32a0145754cc963f",
    "_vis_opt_test_cookie": "1",
    "_hjSessionUser_1656737": "eyJpZCI6ImQ5NWRlYc2bb4f5134",
    "_hjSession_1656737": "eyJpZCI6ImE3MD4f5134",
    "_vwo": "ts~oHlXauE(NtN)w~D96DB8D504F427B7B84622A1E9C7E13CE%7C9cfdfc91c60a61bc66c014254ddbf897(2sg)l~2%7C(351)u~D96DB8D504F427B7B84622A1E9C7E13CE(1p78)m~3%241695806165%3A81.67070072%3A%3A(2cE)k~*(NtN)n~2254406%3A1(EP",
    "_ga_NZLE1BRHWP": "GS1.1.1698060572.2.0.1698060572.0.0.0"
}

# RATINGS

async def get_rating_details(uid,limit,offset):
    url = f"https://yougov.co.uk/_pubapis/v5/uk/search/entity/?group={uid}&sort_by=popularity&limit={limit}&offset={offset}"
    async with aiohttp.ClientSession(headers=headers, cookies=cookies) as client:
        async with client.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data
            else:
                print(resp.status)
                return None
# TRACKERS

async def get_trackers_details(name):
    url = f"https://yougov.co.uk/_pubapis/v5/uk/trackers/{name}/details/"
    async with aiohttp.ClientSession(headers=headers, cookies=cookies) as client:
        async with client.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data
            else:
                print(resp.status)
                return None


async def get_trackers_overall(uuid):
    url = f"https://yougov.co.uk/_pubapis/v5/uk/trackers/{uuid}/overall/"
    async with aiohttp.ClientSession(headers=headers, cookies=cookies) as client:
        async with client.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data
            else:
                print(resp.status)
                return None

async def get_trackers():
    for category in ["technology", "entertainment", "consumer", "sport", "travel", "society", "international", "health", "economy", "overview", "politics"]:
        url = f"https://yougov.co.uk/_pubapis/v5/uk/search/content/trackers/?category={category}"
        async with aiohttp.ClientSession(headers=headers, cookies=cookies) as client:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        yield data
                    else:
                        print(resp.status)
async def main():
    conn = sqlite3.connect('yougov_data.db')
    cursor = conn.cursor()  # Initialize cursor here
    async for i in get_trackers():
        poll = {
            "opinion": {},
            "where": "yougov",
        }
        for c in i["data"]:
            poll["title"] = c["title"]
            poll["uuid"] = c["uuid"]
            poll["updated_at"] = c["updated_at"]
            tracker =  await get_trackers_overall(c['slug'])
            tracker_id = cursor.execute('''INSERT INTO trackers (title, uuid, updated_at, where_from)
                               VALUES (?, ?, ?, ?)''',
                             (poll['title'], poll['uuid'], poll['updated_at'], poll['where'])).lastrowid
            print(c["navigation_card_data"]["question_labels"])
            for i, (opinion_name, opinion_values) in enumerate(c["navigation_card_data"]["question_labels"]).items():
                for opinion_index, opinion_value in enumerate(opinion_values):
                    # Inserting data into the opinions table
                    cursor.execute('''INSERT INTO opinions (tracker_id, opinion_name, opinion_value,opinion_index)
                                      VALUES (?, ?, ?, ?)''',
                                   (tracker_id, opinion_name, opinion_value,opinion_index))

async def main2():
    conn = sqlite3.connect('yougov_data.db')
    cursor = conn.cursor()  # Initialize cursor here
    # Create a table for storing ratings data
    cursor.execute('''CREATE TABLE IF NOT EXISTS trackers (
                        id INTEGER PRIMARY KEY,
                        title TEXT,
                        uuid TEXT,
                        updated_at TEXT,
                        where_from TEXT
                    )''')

    # Create a table for storing opinions
    cursor.execute('''CREATE TABLE IF NOT EXISTS opinions (
                        id INTEGER PRIMARY KEY,
                        tracker_id INTEGER,
                        opinion_name TEXT,
                        opinion_value REAL,
                        opinion_index INTEGER,
                        FOREIGN KEY (tracker_id) REFERENCES trackers (id)
                    )''')
    cursor = conn.cursor()
    async for i in get_trackers():
        poll = {
            "opinion": {},
            "where": "yougov",
        }
        print(i["data"])
        for c in i["data"]:
            poll["title"] = c["title"]
            poll["uuid"] = c["uuid"]
            poll["updated_at"] = c["updated_at"]
            tracker_id = cursor.execute('''INSERT INTO trackers (title, uuid, updated_at, where_from)
                                           VALUES (?, ?, ?, ?)''',
                                         (poll['title'], poll['uuid'], poll['updated_at'], poll['where'])).lastrowid
            print(i["data"])
            for i, (opinion_name, opinion_values) in enumerate(c["opinions"].items()):
                for opinion_index, opinion_value in enumerate(opinion_values):
                    # Inserting data into the opinions table
                    cursor.execute('''INSERT INTO opinions (tracker_id, opinion_name, opinion_value,opinion_index)
                                      VALUES (?, ?, ?, ?)''',
                                   (tracker_id, opinion_name, opinion_value,opinion_index))
                

if __name__ == '__main__':
    pass
    import asyncio
    asyncio.run(main())
