import asyncio
import aiohttp
from bs4 import BeautifulSoup
import sqlite3

# Create a SQLite database connection
conn = sqlite3.connect('fact_checking_sources.db')

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Create a table to store fact checking sources
cursor.execute('''
    CREATE TABLE IF NOT EXISTS fact_checking_sources (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_url TEXT NOT NULL
    )
''')

ifcn_verified_signatories = "div.row:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(3) > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div:nth-child(4) > a:nth-child(2)"
ifcn_renewal = "div.row:nth-child(4) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div:nth-child(4) > a:nth-child(2)"
ifcn_expired = "div.row:nth-child(6) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div:nth-child(4) > a:nth-child(2)"

# Find IFCN sources
async def find_ifcn():
    url = "https://ifcncodeofprinciples.poynter.org/signatories"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html_data = await response.text()
    
    soup = BeautifulSoup(html_data, 'html.parser')
    
    # Iterate through the verified signatories
    for i in soup.select(ifcn_verified_signatories):
        href = i["href"]
        source_name = "IFCN Verified Signatory"
        source_type = "verified_signatories"
        
        # Insert data into the SQLite database
        insert_data(conn, source_name, source_type, href)

    # Iterate through the renewal sources
    for i in soup.select(ifcn_renewal):
        href = i["href"]
        source_name = "IFCN Renewal"
        source_type = "ifcn_renewal"
        
        # Insert data into the SQLite database
        insert_data(conn, source_name, source_type, href)

    # Iterate through the expired sources
    for i in soup.select(ifcn_expired):
        href = i["href"]
        source_name = "IFCN Expired"
        source_type = "ifcn_expired"
        
        # Insert data into the SQLite database
        insert_data(conn, source_name, source_type, href)

async def insert_data(conn, source_name, source_type, source_url):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO fact_checking_sources (source_name, source_type, source_url)
        VALUES (?, ?, ?)
    ''', (source_name, source_type, source_url))
    conn.commit()
