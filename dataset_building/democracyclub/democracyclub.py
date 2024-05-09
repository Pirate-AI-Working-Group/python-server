import aiohttp
import asyncio

async def get_persons():
    page = 1
    while True: 
        url = f"https://candidates.democracyclub.org.uk/api/v0.9/persons/?page={page}"
        # print(url)
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()
                if "results" not in data:
                    break
                for i in data["results"]:
                    ddd = {
                        "id": i["id"],
                        "memberships": [],
                    }
                    if i["gender"] != "":
                        ddd["gender"] = i["gender"]
                    if  i["thumbnail"] != "":
                        ddd["thumbnail"] =  i["thumbnail"]
                    if  i["email"] != "":
                        ddd["email"] =  i["email"]
                    if  i["death_date"] != "":
                        ddd["death_date"] =  i["death_date"]
                    if  i["birth_date"] != "":
                        ddd["birth_date"] =  i["birth_date"]
                        # print("contact_details: ",x)
                    for x in i["memberships"]:
                            ddddsa= {
                             "role":x["role"],  
                             "post_id":x["post"]["id"],  
                             "post_slug":x["post"]['slug'],  
                             "role":x["role"],  
                             "on_behalf_of_name":x["on_behalf_of"]['name'],
                             "on_behalf_of_id":x["on_behalf_of"]['id'],
                             "election_start_date":x["start_date"],
                             "election_end_date":x["end_date"],
                             "election_label":x["label"],
                            }
                            ddd["memberships"].append(ddddsa)
                            # print("role: ",ddddsa)
                            # print("election: ",x["election"]["name"])
                            # print("election: ",x["election"]["id"])
                            # print(ddddsa)
                            ddd["memberships"]
                            continue
                    for x in i["contact_details"]:
                        if x["contact_type"] == "twitter":
                            continue
                    
                    for identifiers in i["identifiers"]:
                        if identifiers["scheme"] == "twitter":
                            pass
                        elif identifiers["scheme"] == "uk.org.publicwhip":
                            pass
                        # else:
                        #     print(identifiers)
        page = page + 1
    
async def get_memberships():
    page = 1
    while True: 
        url = f"https://candidates.democracyclub.org.uk/api/v0.9/memberships/?page={page}"
        print(url)
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()
                if "results" not in data:
                    break
                for i in data["results"]:
                    print(i.keys())
        page = page + 1
    
async def get_organizations():
    page = 1
    while True: 
        url = f"https://candidates.democracyclub.org.uk/api/v0.9/organizations/?page={page}"
        # print(url)
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()
                if "results" not in data:
                    break
                for i in data["results"]:
                    ddd = {
                        "register":i["register"],
                        "classification":i["classification"],
                        "name":i["name"],
                        "parent":i["parent"],
                        "dissolution_date":i["dissolution_date"]
                    }
                    for identifier in i["identifiers"]:
                        if identifier["scheme"] == "electoral-commission":
                            pass
                        elif identifier["scheme"] == "popit-organization":
                            pass
                        elif identifier["scheme"] == "ynmp-party":
                            pass
                        else:
                            print("missing identifier",identifier)
                    for contact_detail in i["contact_details"]:
                        if contact_detail["contact_type"] == "twitter":
                            pass
                        else:
                            print("missing contact_detail",contact_detail)
        page = page + 1

async def main():
    await get_persons()
    # await get_memberships()
    await get_organizations()

asyncio.run(main())