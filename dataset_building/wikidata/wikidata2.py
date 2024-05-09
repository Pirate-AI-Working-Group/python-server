import asyncio
import json
import time
import traceback
from typing import Any, Dict, Set
import aiohttp
import itertools
from progressbar import progressbar
import sqlite3

# Connect to SQLite database
conn = sqlite3.connect('wikidata.db')
cursor = conn.cursor()

# Create tables if they don't exist
cursor.execute('''CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    labels TEXT,
                    aliases TEXT
                )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS claims (
                    entity_id TEXT,
                    prop TEXT,
                    prop_value TEXT,
                    FOREIGN KEY (entity_id) REFERENCES entities(id)
                )''')

cursor.execute('''CREATE TABLE IF NOT EXISTS sitelinks (
                    entity_id TEXT,
                    site_name TEXT,
                    url TEXT,
                    FOREIGN KEY (entity_id) REFERENCES entities(id)
                )''')
cursor.execute('''CREATE TABLE IF NOT EXISTS labels (
                    entity_id TEXT,
                    label TEXT,
                    PRIMARY KEY (entity_id, label),
                    FOREIGN KEY (entity_id) REFERENCES entities(id)
                )''')
cursor.execute('''CREATE TABLE IF NOT EXISTS aliases (
                    entity_id TEXT,
                    alias TEXT,
                    FOREIGN KEY (entity_id) REFERENCES entities(id)
                )''')
conn.commit()




Qs = {
    "Q107183145",
    "Q107183151",
    "Q107208914",
    "Q107208916",
    "Q107227701",
    "Q107236569",
    "Q107236603",
    "Q107236680",
    "Q107236776",
    "Q107236849",
    "Q107236883",
    "Q107236916",
    "Q107236966",
    "Q107237013",
    "Q107258494",
    "Q107258644",
    "Q107258714",
    "Q107258724",
    "Q107259292",
    "Q107259317",
    "Q113486165",
    "Q115689759",
    "Q118587151",
    "Q142043",
    "Q442927",
    "Q738377",
    "Q1110794",
    "Q1868552",
    "Q2006125",
    "Q2065227",
    "Q4689686",
    "Q4736543",
    "Q10593346",
    "Q11313190",
    "Q19046104",
    "Q20850562",
    "Q62470341",
    "Q106162750",
    "Q106634743",
    "Q106635276",
    "Q106635283",
    "Q106650857",
    "Q106650895",
    "Q106650967",
    "Q106651073",
    "Q106651089",
    "Q106651148",
    "Q106651150",
    "Q106651156",
    "Q106651322",
    "Q106651333",
    "Q106651338",
    "Q106651343",
    "Q106651350",
    "Q106651372",
    "Q106651387",
    "Q106651395",
    "Q106651430",
    "Q106651444",
    "Q106652786",
    "Q106652971",
    "Q106652977",
    "Q106661426",
    "Q106664402",
    "Q106664450",
    "Q106664582",
    "Q106668171",
    "Q106668248",
    "Q106668332",
    "Q106668420",
    "Q106668471",
    "Q106668535",
    "Q106668608",
    "Q106668646",
    "Q106668702",
    "Q106671524",
    "Q106671613",
    "Q106676147",
    "Q106676186",
    "Q106676275",
    "Q106676417",
    "Q106677807",
    "Q106677862",
    "Q106677940",
    "Q106678098",
    "Q106678195",
    "Q106687540",
    "Q106687570",
    "Q106687639",
    "Q106687644",
    "Q106687653",
    "Q107170444",
    "Q107170468",
    "Q107170485",
    "Q107170506",
    "Q107170524",
    "Q107170655",
    "Q107171978",
    "Q107171980",
    "Q107171982",
    "Q107171988",
    "Q107171989",
    "Q107178411",
    "Q107178453",
    "Q107179479",
    "Q107181454",
    "Q107181559",
    "Q107181855",
    "Q107181871",
    "Q107181979",
    "Q107182185",
    "Q107182188",
    "Q107182194",
    "Q107182201",
    "Q107182208",
    "Q107182322",
    "Q107182376",
    "Q107182456",
    "Q107182467",
    "Q107183119",
    "Q107183122",
    "Q107183126",
    "Q107183138",
}
CCC = {
    "P106": "occupation",
    "P1387": "political alignment",
    "P101": "field of work",
    "P361": "part of",
    "P31": "instance of",
    "P279": "subclass of",
    "P463": "member of",
    "P2650": "interested in",
    "P463": "member of",
    "P1142": "political ideology",
    "P140": "religion or worldview",
    "P5004": "in opposition to",
}
PROPERTY_CLASS = {
    **CCC,
    "P6886": "writing language",
    "P2572": "hashtag",
    "P27": "country of citizenship",
    "P172": "ethnic group",
    "P91": "sexual orientation",
    "P1142": "political ideology",
    "P39": "position held",
    "P69": "educated at",
    "P361": "part of",
}

PROPERTY_DATA = {
    "P803": "academic minor",
    "P811": "academic major",
    "P7590": "eBay username",
    "P7931": "Adelsvapen ID",
    "P10749": "Time.com author ID",
    "P3106	": "Guardian topic ID",
    "P856": "official website",
    "P12203": "official wiki URL",
    "P1581": "official blog URL",
    "P1019": "web feed URL",
    "P8964": "OpenReview.net profile ID",
    "P8919": "Gab username",
    "P8743": "Scholars Strategy Network ID",
    "P8159": "SciProfiles ID",
    "P6634": "LinkedIn personal profile ID",
    "P6552": "X user numeric ID",
    "P496": "ORCID iD",
    "P4265": "Reddit username",
    "P4033": "Mastodon address",
    "P4012": "Semantic Scholar author ID",
    "P3984": "subreddit",
    "P12409": "Bluesky DID",
    "P3943": "Tumblr username",
    "P3899": "Medium username",
    "P3267": "Flickr user ID",
    "P2572": "hashtag",
    "P2397": "YouTube channel ID",
    "P2038": "ResearchGate profile ID",
    "P2013": "Facebook username",
    "P2003": "Instagram username",
    "P2002": "X username",
    "P11705": "Facebook numeric ID",
    "P11532": "Hackage username",
    "P1153": "Scopus author ID",
    "P11245": "YouTube handle",
    "P11003": "Scinapse author ID",
    "P10858": "Truth Social username",
    "P10465": "CiteSeerX person ID",
    "P10283": "OpenAlex ID",
    "P214": "VIAF ID",
    "P402": "OpenStreetMap relation ID",
    "P434": "MusicBrainz artist ID",
    "P435": "MusicBrainz work ID",
    "P436": "MusicBrainz release group ID",
    "P508": "BNCF Thesaurus ID",
    "P648": "Open Library ID",
    "P966": "MusicBrainz label ID",
    "P982": "MusicBrainz area ID",
    "P1004": "MusicBrainz place ID",
    "P1047": "Catholic Hierarchy person ID",
    "P1248": "KulturNav-ID",
    "P1282": "OpenStreetMap tag or key",
    "P1330": "MusicBrainz instrument ID",
    "P1407": "MusicBrainz series ID",
    "P1617": "BBC Things ID",
    "P1820": "Open Food Facts food additive ID",
    "P1821": "Open Food Facts food category ID",
    "P2014": "Museum of Modern Art work ID",
    "P2174": "Museum of Modern Art artist ID",
    "P2427": "GRID ID",
    "P2456": "DBLP author ID",
    "P2607": "BookBrainz author ID",
    "P2689": "BARTOC ID",
    "P2845": "RAN ID",
    "P2889": "FamilySearch person ID",
    "P2949": "WikiTree person ID",
    "P3016": "French national research structure ID",
    "P3076": "Open Beauty Facts category ID",
    "P3180": "Visual Novel Database ID",
    "P3217": "Dictionary of Swedish National Biography ID",
    "P3266": "Library of Congress Format Description Document ID",
    "P3308": "lib.reviews ID",
    "P3348": "National Library of Greece ID",
    "P3417": "Quora topic ID",
    "P3987": "SHARE Catalogue author ID",
    "P4104": "Carnegie Hall agent ID",
    "P4404": "MusicBrainz recording ID",
    "P5383": "archINFORM project ID",
    "P5508": "archINFORM person/group ID",
    "P5573": "archINFORM location ID",
    "P5604": "archINFORM keyword ID",
    "P5731": "Angelicum ID",
    "P5739": "Pontificia Università della Santa Croce ID",
    "P6329": "Share-VDE 1.0 author ID",
    "P6465": "Democracy Club candidate ID",
    "P6766": "Who's on First ID",
    "P6782": "ROR ID",
    "P6944": "Bionomia ID",
    "P7607": "WikiTree category or space",
    "P7778": "Museum of Modern Art online exhibition ID",
    "P8351": "vglist video game ID",
    "P8424": "OpenHistoricalMap relation ID",
    "P8971": "Cinémathèque québécoise person ID",
    "P10283": "OpenAlex ID",
    "P10369": "Lingua Libre ID",
    "P10689": "OpenStreetMap way ID",
    "P10730": "Data Commons ID",
    "P10832": "WorldCat Entities ID",
    "P11693": "OpenStreetMap node ID",
    "P12149": "archINFORM award ID",
    "P12233": "DraCor ID",
    "P12442": "TopKar ID",
    "P12458": "Parsifal cluster ID",
}
Extra = {}


async def get_property_MORE_data():
    Q4p = [
        "Q93433126",
        "Q18608871",
        "Q105388954",
        "Q105946994",
        "Q18610173",
        # "Q22997934",
        "Q21745557",
        # "Q24075706",
        # "Q18614948",
        "Q29548341",
        # "Q19833377",
        # "Q21745557",
    ]
    for q in Q4p:
        sparql_query = f"""
        SELECT ?props ?propsLabel WHERE {{
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
            ?props wdt:P31 wd:{q}.
        }}
        """
        async with aiohttp.ClientSession() as session:
            for i in range(4):
                try:
                    data = await execute_sparql_query(session, sparql_query)
                    for i in data["results"]["bindings"]:
                        Extra[
                            i["props"]["value"].replace(
                                "http://www.wikidata.org/entity/", ""
                            )
                        ] = i["propsLabel"]["value"]
                except:
                    continue
                break


ALL_PROPERTIES = {**PROPERTY_CLASS, **PROPERTY_DATA, **Extra}

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
USER_AGENT = "LucyApp/0.0.1"
laszy = False
MAX_CONCURRENT_REQUESTS = 3

database = {}

async def get_json_response(session, url, params=None, headers=None):
        for i in range(5):
            try:
                async with session.get(
                    url, params=params, headers=headers, timeout=10
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:
                        print("Too many requests",response.status)
                        await asyncio.sleep(5)
                    else:
                        print(f"Error status code: {response.status}")
            except aiohttp.ClientError as e:
                print(f"Error: {e}")
                break
        return None


async def get_wikidata_entity(session, entity_id):
    params = {"action": "wbgetentities", "ids": entity_id, "format": "json"}
    return await get_json_response(session, WIKIDATA_API_URL, params=params)


async def execute_sparql_query(session, sparql_query):
    full_url = f"{WIKIDATA_ENDPOINT}?query={sparql_query}"
    headers = {"Accept": "application/sparql-results+json", "User-Agent": USER_AGENT}
    try:
        return await get_json_response(session, full_url, headers=headers)
    except aiohttp.ClientError as e:
        print("Error executing SPARQL query:", e)
    return None


async def fetch_data_for_property(session, property):
    sparql_query = (
        f"SELECT ?item ?propValue WHERE {{ ?item wdt:{property} ?propValue. }}"
    )
    json_result = await execute_sparql_query(session, sparql_query)
    return json_result["results"]["bindings"] if json_result else []


async def fetch_data_for_property_and_entity(session, property1, property2):
    sparql_query = f"""
    SELECT ?item ?propLabel ?prop  WHERE {{
    ?item wdt:{property1} ?prop.
    ?item wdt:{property2} ?non_pop.
    }}
    """
    json_result = await execute_sparql_query(session, sparql_query)
    return json_result["results"]["bindings"] if json_result else []

async def get_wiki_article_url():
    sites = []
    async with aiohttp.ClientSession() as session:
        sparql_query = """
            SELECT DISTINCT ?site WHERE {
             ?article schema:isPartOf ?site
            }
        """
        data = await execute_sparql_query(session, sparql_query)
        sites = [base["site"]["value"] for base in data["results"]["bindings"]]
    
    for entity_id in cursor.execute("SELECT DISTINCT entity_id FROM entities").fetchall():
        entity_id = entity_id[0]  # Unpack from tuple
        # Skip if labels are already fetched
        if cursor.execute("SELECT COUNT(*) FROM labels WHERE entity_id=?", (entity_id,)).fetchone()[0] > 0:
            continue

        # Fetch and insert labels
        for database_key in cursor.execute("SELECT DISTINCT key FROM properties").fetchall():
            database_key = database_key[0]  # Unpack from tuple
            try:
                sparql_query = f"""
                    SELECT ?itemLabel
                    WHERE {{
                      wd:{entity_id} rdfs:label ?itemLabel.
                    }}
                """
                json_result = await execute_sparql_query(session, sparql_query)
                labels = set(i["itemLabel"]["value"] for i in json_result["results"]["bindings"])
                # Insert labels into SQLite
                for label in labels:
                    cursor.execute('''INSERT OR IGNORE INTO labels (entity_id, label) VALUES (?, ?)''', (entity_id, label))
                    conn.commit()
            except Exception as e:
                print("Error fetching labels:", e)
                continue

        # Fetch and insert sitelinks
        for database_key in cursor.execute("SELECT DISTINCT key FROM properties").fetchall():
            database_key = database_key[0]  # Unpack from tuple
            try:
                for site in sites:
                    sparql_query = f"""
                    SELECT ?item ?article WHERE {{
                      ?none ^wdt:{database_key} ?item.
                      ?item wikibase:sitelinks ?sl;
                        ^schema:about ?article.
                      ?article schema:isPartOf <{site}>.
                    }}
                    """
                    json_result = await execute_sparql_query(session, sparql_query)
                    urls = [i["article"]["value"] for i in json_result["results"]["bindings"]]
                    cursor.executemany('''INSERT INTO sitelinks (entity_id, site_name, url) VALUES (?, ?, ?)''', [(entity_id, site, url) for url in urls])
                    conn.commit()
            except Exception as e:
                print("Error fetching sitelinks:", e)
                continue

def convert_dict(d: Dict[str, Dict[str, set]]) -> Dict[str, Dict[str, list]]:
    converted_dict = {}
    for key, inner_dict in d.items():
        converted_inner_dict = {
            inner_key: list(inner_set)
            for inner_key, inner_set in inner_dict.items()
        }
        converted_dict[key] = converted_inner_dict
    return converted_dict

async def process_claim(entity_id: str, prop: str, claim: Dict[str, Any]) -> None:
    prop_value = claim.get("mainsnak", {}).get("property")
    if prop_value is not None and (prop in ALL_PROPERTIES or prop in Extra):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
        if isinstance(value, str):
            # Insert data into SQLite database
            cursor.execute('''INSERT INTO claims (entity_id,prop,prop_value) VALUES (?, ?, ?)''', (entity_id, prop, prop_value))
            conn.commit()

async def process_entity(entity_id, session):
    async with semaphore:
        try:
            data = await get_wikidata_entity(session, entity_id)
            entity_data = data.get("entities", {}).get(entity_id, {})
            
            # Process claims and insert into SQLite
            for prop, claims in entity_data.get("claims", {}).items():
                for claim in claims:
                    # Process and insert claim data into SQLite
                    prop_value = claim.get("mainsnak", {}).get("property")
                    value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
                    cursor.execute('''INSERT INTO claims (entity_id,prop,prop_value) VALUES (?, ?, ?)''', (entity_id, prop_value, value))
                    conn.commit()

            cursor.execute('''INSERT INTO labels (entity_id, label) VALUES (?, ?)''', (entity_id, entity_data.get("labels", {})))
            cursor.execute('''INSERT INTO aliases (entity_id, alias) VALUES (?, ?)''', (entity_id, entity_data.get("aliases", {})))
            cursor.execute('''INSERT INTO sitelinks (entity_id, site, url) VALUES (?, ?, ?)''', ((entity_id, site, url) for site, url in entity_data.get("sitelinks", {}).items()))
            
        except Exception as e:
            traceback.print_exc()
            print(f"Error processing entity {entity_id}: {e}")


async def run_wiki_data_task():
    properties_failed = set()
    # Fetch data for each property
    for property in ALL_PROPERTIES.keys():
        print("Remaining properties:", list(ALL_PROPERTIES).index(property), "/", len(ALL_PROPERTIES))
        try:
            async with aiohttp.ClientSession() as session:
                results = await fetch_data_for_property(session, property)
            for result in results:
                print("Remaining properties:", list(results).index(result), "/", len(results))
                entity_id = result["item"]["value"].replace("http://www.wikidata.org/entity/", "")
                prop_value = result["propValue"]["value"]
                cursor.execute('''INSERT INTO claims (entity_id,prop,prop_value) VALUES (?, ?, ?)''', (entity_id, property, prop_value))
                conn.commit()
        except Exception as e:
            traceback.print_exc()
            print(f"Error processing property {property}: {e}")
            properties_failed.add(property)
            continue
    for p1 in properties_failed.union(properties_failed):
        for p2 in CCC.keys():
            if p2 == "P463":
                continue
            try:
                async with aiohttp.ClientSession() as session:
                    results = await fetch_data_for_property_and_entity(session, p1, p2)
                for result in results:
                    entity_id = result["item"]["value"].replace("http://www.wikidata.org/entity/", "")
                    prop_value = result["prop"]["value"]
                    cursor.execute('''INSERT INTO claims (entity_id,prop,prop_value) VALUES (?, ?, ?)''', (entity_id, p1, prop_value))
                    conn.commit()
            except Exception as e:
                traceback.print_exc()
                print(f"Error processing entity {p2} @ {p1}: {e}")
    # Fetch article URLs for entities
    await get_wiki_article_url()

# Run the main task
asyncio.run(run_wiki_data_task())
