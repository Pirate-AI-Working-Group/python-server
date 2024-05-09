import sqlite3
import utility.helper as helper

# List of URLs
urls = [
    "https://pirateparty.org.au/",
    "https://piratenpartei.at/",
    # Add the rest of the URLs
]

# Function to fetch data from URLs
async def get_pirateparty():
    for url in urls:
        temp2 = {
            "domain": url,
            "is_pirate": True,
        }
        await helper.run_callbacks("domain", temp2)
    return

# Function to initialize SQLite connection and create a table
def initialize_database():
    connection = sqlite3.connect('pirateparty.db')
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pirateparty (
            id INTEGER PRIMARY KEY,
            domain TEXT NOT NULL,
            is_pirate INTEGER
        )
    ''')
    connection.commit()
    connection.close()

# Function to insert data into SQLite table
def insert_into_database(data):
    connection = sqlite3.connect('pirateparty.db')
    cursor = connection.cursor()
    for entry in data:
        cursor.execute('''
            INSERT INTO pirateparty (domain, is_pirate)
            VALUES (?, ?)
        ''', (entry['domain'], entry['is_pirate']))
    connection.commit()
    connection.close()

# Initialize database
initialize_database()

# Fetch data from URLs and insert into database
async def main():
    data = await get_pirateparty()
    insert_into_database(data)

# Run the main function
await main()
