import aiohttp
import asyncio
import os
import json

start_id = CUSTOM_ID
end_id = CUSTOM_ID
output_folder = "captions"
os.makedirs(output_folder, exist_ok=True)

CONCURRENT_REQUESTS = 100
sem = asyncio.Semaphore(CONCURRENT_REQUESTS)

last_file_path = "last.txt"
last_id_lock = asyncio.Lock()

async def write_last_id(material_id):
    async with last_id_lock:
        with open(last_file_path, "w", encoding="utf-8") as f:
            f.write(str(material_id))

async def fetch_and_save(session, material_id):
    url = f"https://mediaservices.viacom.com/api/getCaptions?materialId={material_id}"
    async with sem:
        try:
            async with session.get(url, timeout=5) as response:
                if response.status == 200:
                    data = await response.json(content_type=None)
                    if data:
                        output_path = os.path.join(output_folder, f"{material_id}.json")
                        with open(output_path, "w", encoding="utf-8") as f:
                            json.dump(data, f, indent=4, ensure_ascii=False)
                        await write_last_id(material_id)
                        print(f"Saved: {material_id}")
                    else:
                        print(f"Empty JSON for {material_id}")
                else:
                    print(f"Error of request for {material_id}: {response.status}")
        except Exception as e:
            print(f"Error for {material_id}: {e}")

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_save(session, material_id) for material_id in range(start_id, end_id + 1)]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
