import aiohttp
import sqlite3

# Create a SQLite connection and cursor
conn = sqlite3.connect('open_access_data.db')
c = conn.cursor()

# Create a table to store the data
c.execute('''CREATE TABLE IF NOT EXISTS open_access_data
             (id INTEGER PRIMARY KEY, 
             doi TEXT,
             pmc TEXT,
             pmid TEXT,
             journal TEXT,
             publisher TEXT,
             published TEXT,
             subject TEXT,
             title TEXT)''')
conn.commit()


async def mainTaskOpenAccessButton_doi(doi):
    doi = data["doi"]
    url = f"https://bg.api.oa.works/find?id={doi}"
    async with helper.get_max_http_connections_semaphore():
        async with aiohttp.ClientSession() as session:
            async with session.head(url) as response:
                data = await response.json()
                # Insert data into SQLite table
                c.execute('''INSERT INTO open_access_data
                             (doi, journal, publisher, published, subject, title)
                             VALUES (?, ?, ?, ?, ?, ?)''',
                          (data.get("doi"), data.get("journal"), data.get("publisher"),
                           data.get("published"), data.get("subject"), data.get("title")))
                conn.commit()


async def mainTaskOpenAccessButton_pmc(pmc):
    pmc = data["pmc"]
    url = f"https://bg.api.oa.works/find?id={pmc}"
    async with helper.get_max_http_connections_semaphore():
        async with aiohttp.ClientSession() as session:
            async with session.head(url) as response:
                data = await response.json()
                # Insert data into SQLite table
                c.execute('''INSERT INTO open_access_data
                             (pmc, journal, publisher, published, subject, title)
                             VALUES (?, ?, ?, ?, ?, ?)''',
                          (data.get("pmc"), data.get("journal"), data.get("publisher"),
                           data.get("published"), data.get("subject"), data.get("title")))
                conn.commit()


async def mainTaskOpenAccessButton_pmid(pmid):
    pmid = data["pmid"]
    url = f"https://bg.api.oa.works/find?id={pmid}"
    async with helper.get_max_http_connections_semaphore():
        async with aiohttp.ClientSession() as session:
            async with session.head(url) as response:
                data = await response.json()
                # Insert data into SQLite table
                c.execute('''INSERT INTO open_access_data
                             (pmid, journal, publisher, published, subject, title)
                             VALUES (?, ?, ?, ?, ?, ?)''',
                          (data.get("pmid"), data.get("journal"), data.get("publisher"),
                           data.get("published"), data.get("subject"), data.get("title")))
                conn.commit()

# Don't forget to close the SQLite connection when you're done
conn.close()
