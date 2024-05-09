import aiohttp
import asyncio
import sqlite3

# Create SQLite database connection
conn = sqlite3.connect('markets.db')
cursor = conn.cursor()

# Create tables if not exists
cursor.execute('''CREATE TABLE IF NOT EXISTS markets (
                    id TEXT PRIMARY KEY,
                    creatorId TEXT,
                    creatorUsername TEXT,
                    creatorName TEXT,
                    createdTime INTEGER,
                    creatorAvatarUrl TEXT,
                    closeTime INTEGER,
                    question TEXT,
                    slug TEXT,
                    url TEXT,
                    totalLiquidity REAL,
                    outcomeType TEXT,
                    mechanism TEXT,
                    volume REAL,
                    volume24Hours REAL,
                    isResolved INTEGER,
                    uniqueBettorCount INTEGER,
                    lastUpdatedTime INTEGER,
                    lastBetTime INTEGER,
                    lastCommentTime INTEGER
                )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS market_positions (
                    market_id TEXT,
                    order TEXT,
                    top INTEGER,
                    bottom INTEGER,
                    userId TEXT,
                    FOREIGN KEY (market_id) REFERENCES markets(id)
                )''')

# Function to insert market data into SQLite database
async def insert_market_data(market_data):
    cursor.execute('''INSERT INTO markets (id, creatorId, creatorUsername, creatorName, createdTime, 
                    creatorAvatarUrl, closeTime, question, slug, url, totalLiquidity, outcomeType, mechanism,
                    volume, volume24Hours, isResolved, uniqueBettorCount, lastUpdatedTime, lastBetTime, lastCommentTime)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                    (market_data['id'], market_data['creatorId'], market_data['creatorUsername'], 
                    market_data['creatorName'], market_data['createdTime'], market_data['creatorAvatarUrl'], 
                    market_data['closeTime'], market_data['question'], market_data['slug'], market_data['url'], 
                    market_data.get('totalLiquidity', 0), market_data['outcomeType'], market_data['mechanism'], 
                    market_data.get('volume', 0), market_data.get('volume24Hours', 0), market_data['isResolved'], 
                    market_data['uniqueBettorCount'], market_data['lastUpdatedTime'], market_data['lastBetTime'], 
                    market_data.get('lastCommentTime', 0)))
    conn.commit()

# Function to insert market positions data into SQLite database
async def insert_market_positions_data(market_id, positions_data):
    for position in positions_data:
        cursor.execute('''INSERT INTO market_positions (market_id, order, top, bottom, userId)
                        VALUES (?, ?, ?, ?, ?)''', 
                        (market_id, position['order'], position.get('top'), position.get('bottom'), position.get('userId')))
    conn.commit()

# Your existing functions remain unchanged except for calling the insert functions

async def get_markets(limit=1000, sort='created-time', order='desc', before=None, userId=None, groupId=None):
    await asyncio.sleep(1800)
    base_url = "https://api.manifold.markets/v0/markets"
    params = {
        'limit': limit,
    }
    if groupId is not None:
        params['groupId'] = groupId
    if before is not None:
        params['before'] = before
    if sort is not None:
        params['sort'] = sort
    if order is not None:
        params['order'] = order
    if userId is not None:
        params['userId'] = userId
    print(params)
    async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params) as response:
                if response.status == 200:
                    markets = await response.json()
                    for market in markets:
                        await insert_market_data(market)  # Insert market data into SQLite
                    return markets
                else:
                    print(f"Error: {response.status}")
                    return None

async def get_market(market_id):
    base_url = f"https://api.manifold.markets/v0/market/{market_id}"
    async with aiohttp.ClientSession() as session:
            async with session.get(base_url) as response:
                if response.status == 200:
                    market_data = await response.json()
                    await insert_market_data(market_data)  # Insert market data into SQLite
                    return market_data
                else:
                    print(f"Error: {response.status}")
                    return None

async def get_market_positions(market_id, order='profit', top=None, bottom=None, userId=None):
    base_url = f"https://api.manifold.markets/v0/market/{market_id}/positions"
    params = {
        'order': order,
        'top': top,
        'bottom': bottom,
        'userId': userId
    }
    async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params) as response:
                if response.status == 200:
                    positions_data = await response.json()
                    await insert_market_positions_data(market_id, positions_data)  # Insert positions data into SQLite
                    return positions_data
                else:
                    print(f"Error: {response.status}")
                    return None

async def main ():
    before = None
    while True:
        get_markets_response = await get_markets(before=before)
        for i in get_markets_response:
            print(i)
            before = i['id']
            i["question"]
            print(i.keys())
    
asyncio.run(main())