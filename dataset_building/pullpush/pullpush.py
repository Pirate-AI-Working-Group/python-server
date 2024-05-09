import asyncio
import json
import os
import aiofiles
import asyncio
import zstandard as zstd

if __name__ != "__main__":
    from utility.helper import run_callbacks
    from utility.helper import task_init_run_callbacks
max_found = 0
max_found_buffer = 0
max_count = 0
# The `log` variable in the code snippet is opening a file named "log.txt" in write mode. This file is
# intended to be used for logging purposes, but currently, it is not being utilized in the code
# provided.
class Zreader:
    def __init__(self, file, chunk_size=16384):
        '''Init method'''
        self.file = file
        self.fh = open(file, 'rb')
        self.chunk_size = chunk_size
        self.dctx = zstd.ZstdDecompressor(max_window_size=2147483648)
        self.reader = self.dctx.stream_reader(self.fh)
        self.buffer = b''  # Change buffer type to bytes

    async def readlines(self):
        global max_found
        global max_found_buffer
        global max_count
        '''Generator method that creates an iterator for each line of JSON'''
        count = 0
        # log = open("/home/lucy/Code/code_reddit/log.txt", "w")
        while True:
            count = count + 1
            try:
                await asyncio.to_thread(self.reader.read,size=self.chunk_size)
                chunk = self.reader.read(self.chunk_size)  # Read binary chunk
                if not chunk:
                    break
                if max_count > 50:
                    # running out of memory, reset the count and skip processing this chunk
                    if b"\n" in chunk:
                        index = chunk.index(b"\n") + 1
                        chunk = chunk[index:]
                        max_count = 0
                    max_count = 1 + max_count
                    print("over max_count:"+str(max_count))
                lines = (self.buffer + chunk).split(b"\n")  # Split by bytes
                max_count = max(max_count, count)
                print("max_count:"+str(max_count))
                for line in lines[:-1]:
                    # log.write(self.file +" " + str(len(line)) +" " + str(max_found) +" " + str(max_found_buffer) + "\n")
                    count = 0
                    max_found = max(max_found, len(line))
                    yield json.loads(line.decode('utf-8', errors='ignore'))  # Decode with error handling
                self.buffer = lines[-1]
                max_found_buffer = max(max_found_buffer, len(self.buffer))
                
            except Exception as e:
                print(e)
                break
        # log.write("max_count:"+str(max_count) +"\n")
        # print("max_count:"+str(max_count))
        # log.write("max_found:"+str(max_found) +"\n")
        # print("max_found:"+str(max_found))
        # log.write("max_found_buffer:"+str(max_found_buffer) + "\n")
        # print("max_found_buffer:"+str(max_found_buffer) )
        # log.write("file:"+str(self.file) + "\n")
        # print("file:"+str(self.file) )
        # log.close()




async def get_submissions(path):
    print(path)
    fh = Zreader(path)
    async for line in fh.readlines():
        data = {
            "where": "pullpush",
            "reddit_submissions": {}
        }
        if "subreddit" in line.keys():
            data["name"] = line["subreddit"]
        if "url" in line.keys():
            data["url"] = line["url"]
            data["reddit_url"] = line["url"]
        if "permalink" in line.keys():
            data["permalink"] = line["permalink"]
        if "subreddit_id" in line.keys():
            data["subreddit_id"] = line["subreddit_id"]
        if "title" in line.keys():
            data["title"] = line["title"]
        if "link_flair_text" in line.keys():
            data["link_flair_text"] = line["link_flair_text"]
        if "selftext" in line.keys():
            data["selftext"] = line["selftext"]
        if "selftext_html" in line.keys():
            data["selftext_html"] = line["selftext_html"]
        if "flair" in line.keys():
            data["flair"] = line["flair"]
        if "title" in line.keys():
            data["reddit_submissions"]["title"] =line["title"]
        if "id" in line.keys():
            data["reddit_submissions"]["id"] =line["id"]
        if "upvote_ratio" in line.keys():
            data["reddit_submissions"]["upvote_ratio"] =line["upvote_ratio"]
        if "upvote_ratio" in line.keys():
            data["reddit_submissions"]["ups"] =line["upvote_ratio"]
        if "no_follow" in line.keys():
            data["reddit_submissions"]["no_follow"] =line["no_follow"]
        if "num_comments" in line.keys():
            data["reddit_submissions"]["num_comments"] =line["num_comments"]
        if "num_crossposts" in line.keys():
            data["reddit_submissions"]["num_crossposts"] =line["num_crossposts"]
        if "num_reports" in line.keys():
            data["reddit_submissions"]["num_reports"] =line["num_reports"]
        if "score" in line.keys():
            data["reddit_submissions"]["score"] =line["score"]
        if "created_utc" in line.keys():
            data["reddit_submissions"]["created_utc"] =line["created_utc"]
        if "spoiler" in line.keys():
            data["reddit_submissions"]["spoiler"] =line["spoiler"]
        if "is_crosspostable" in line.keys():
            data["reddit_submissions"]["is_crosspostable"] =line["is_crosspostable"]
        if "is_meta" in line.keys():
            data["reddit_submissions"]["is_meta"] =line["is_meta"]
        if "is_original_content" in line.keys():
            data["reddit_submissions"]["is_original_content"] =line["is_original_content"]
        if "is_reddit_media_domain" in line.keys():
            data["reddit_submissions"]["is_reddit_media_domain"] =line["is_reddit_media_domain"]
        if "is_robot_indexable" in line.keys():
            data["reddit_submissions"]["is_robot_indexable"] =line["is_robot_indexable"]
        if "is_self" in line.keys():
            data["reddit_submissions"]["is_self"] =line["is_self"]
        if "is_video" in line.keys():
            data["reddit_submissions"]["is_video"] =line["is_video"]
        if "locked" in line.keys():
            data["reddit_submissions"]["locked"] =line["locked"]
        if "pinned" in line.keys():
            data["reddit_submissions"]["pinned"] =line["pinned"]
        if "author_flair_text" in line.keys():
            data["reddit_submissions"]["author_flair_text"] =line["author_flair_text"]
        if "author_flair_type" in line.keys():
            data["reddit_submissions"]["author_flair_type"] =line["author_flair_type"]
        if "created_utc" in line.keys():
            data["reddit_submissions"]["created_utc"] =line["created_utc"]
        if "id" in line.keys():
            data["reddit_submissions"]["id"] =line["id"]
        if "total_awards_received" in line.keys():
            data["reddit_submissions"]["total_awards_received"] =line["total_awards_received"]
        if "treatment_tags" in line.keys():
            data["reddit_submissions"]["treatment_tags"] =line["treatment_tags"]
        if "domain" in line.keys():
            data["reddit_submissions"]["domain"] =line["domain"]
        if __name__ != "__main__":
            await task_init_run_callbacks("reddit_post", data)
        else:
            pass
            # print(data)

async def mainloop_pullpush():
    folder_path = "/home/lucy/Code/code_reddit/data/pullshift/submissions/"
    # loop over file in folder
    ccc = os.listdir(folder_path)
    # sort by name
    ccc = sorted(ccc)
    for path in ccc:
        # must end in .zst
        await get_submissions(folder_path+path)
if __name__ == "__main__":
    asyncio.run(mainloop_pullpush())
    print("max_found size ->", max_found)
    print("max_found_buffer size ->", max_found_buffer)
    print("max_count ->", max_count)