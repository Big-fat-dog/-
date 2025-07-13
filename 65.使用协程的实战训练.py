import logging
import asyncio
import aiohttp
import json
from motor.motor_asyncio import AsyncIOMotorClient
logging.basicConfig(level= logging.INFO)
semaphore = asyncio.Semaphore(5)
MONGO_CONNECTION_STRING = "mongodb://localhost:27017"
MONGO_DB_NAME = 'BOOKS'
MONGO_COLLECTION_NAME = 'books'
client = AsyncIOMotorClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DB_NAME]
collection = db[MONGO_COLLECTION_NAME]
async def save(data):
    logging.info('saving data %s',data)
    if data:
        result = await collection.update_one({'id':data['id']}, {'$set':data}, upsert=True)
        return result
async def get_page_url(url):
    #用于获取每页的url,从此处获取id以进入详情页
    page_urls = []
    for i in range(0, 503):
        page_url = f'{url}' +f'{i*18}'
        page_urls.append(page_url)
    return page_urls
async def scrape_page_url():
    timeout = aiohttp.ClientTimeout(total=5)
    async with semaphore:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = 'https://spa5.scrape.center/api/book/?limit=18&offset='
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'}
            page_urls = await get_page_url(url)
            responses = []
            try:
                for i in page_urls:
                    response = await session.get(i,headers=headers)#这里的await不能少，不然报错
                    logging.info(f'正在获取第{i}页的内容')
                    data = await response.json() #解析也需要等待因为是io任务，故会被变成coroutine对象
                    responses.append(data)
            except Exception as e:
                logging.error(f'第{i}页获取失败')
            return responses
async def scrape_detail_Url():
    a =await scrape_page_url()
    datas = []
    urls = []
    first_url = 'https://spa5.scrape.center/api/book/'
    for i in a:
        id = i['results'][0]['id']
        url = first_url + id
        urls.append(url)
    timeout = aiohttp.ClientTimeout(total=5)
    async with semaphore:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'}
                for url in urls:
                    response = await session.get(url,headers=headers)
                    logging.info(f"正在获取{url}的内容")
                    data = await response.json()
                    datas.append(data)
            except Exception as e:
                logging.error(f"{url}请求出现错误")
        return datas
async def main():
    tasks = await scrape_detail_Url()
    for data in tasks:
        await save(data)
if __name__ == '__main__':
    asyncio.run(main())







