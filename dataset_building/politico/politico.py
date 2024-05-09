import aiohttp
import asyncio
import time
import sqlite3

async def get_poll_of_polls(poll_type):
    base_url = f"https://www.politico.eu/wp-json/politico/v1/poll-of-polls/{poll_type}"
    async with aiohttp.ClientSession() as session:
        async with session.get(base_url) as response:
            if response.status == 200:
                polls = await response.json()
                return polls
            else:
                print(f"Error: {response.status}")
                return None

def date_str_to_unix(date_str):
    return int(time.mktime(time.strptime(date_str, "%Y-%m-%d")))

async def main():
    poll_types = ["EU-parliament", "GB-parliament", "GB-leadership-approval", "GB-scottish-independence"]
    
    # Connect to SQLite database
    conn = sqlite3.connect('poll_data.db')
    c = conn.cursor()
    
    # Create tables if not exist
    c.execute('''CREATE TABLE IF NOT EXISTS parties
                 (party_id INTEGER PRIMARY KEY, name TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS polls
                 (poll_id INTEGER PRIMARY KEY, where TEXT, firm TEXT, date INTEGER, sample_size INTEGER)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS poll_results
                 (result_id INTEGER PRIMARY KEY, poll_id INTEGER, party_id INTEGER, percentage REAL,
                 FOREIGN KEY(poll_id) REFERENCES polls(poll_id),
                 FOREIGN KEY(party_id) REFERENCES parties(party_id))''')
    
    data = {}
    for poll_type in poll_types:
        polls = await get_poll_of_polls(poll_type)
        
        for poll in polls['polls']:
            firm = poll["firm"]
            date = date_str_to_unix(poll["date"])
            sample_size = poll["sample_size"]
            # Insert poll data
            c.execute("INSERT INTO polls (where, firm, date, sample_size) VALUES (?, ?, ?, ?)",
                      ("politico", firm, date, sample_size))
            poll_id = c.lastrowid
            
            # Insert parties' data and poll results
            for party, percentage in poll["parties"].items():
                # Insert party if not exist
                c.execute("INSERT OR IGNORE INTO parties (name) VALUES (?)", (party,))
                party_id = c.lastrowid
                # Insert poll result
                c.execute("INSERT INTO poll_results (poll_id, party_id, percentage) VALUES (?, ?, ?)",
                          (poll_id, party_id, percentage))
    
    # Commit changes and close connection
    conn.commit()
    conn.close()

# Run the event loop
asyncio.run(main())
