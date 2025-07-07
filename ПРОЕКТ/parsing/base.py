from abc import  ABC, abstractmethod
import aiohttp
import asyncio
import os
from fake_useragent import UserAgent
from typing import Any
import aiofiles
import json
from bs4 import BeautifulSoup
import random

class Readable(ABC):
    def __init__(self): pass
    
    @abstractmethod
    def get(self): pass

class Writable(ABC):
    def __init__(self): pass
    
    @abstractmethod
    def put(self, data: Any): pass

class WorkerWithHtml(Readable):
    
    def __init__(self):
        super().__init__()
        self._user = UserAgent().random
        
    @property
    def user(self):
        return  self._user

    async def get(self, 
                  session: aiohttp.ClientSession, 
                  url: str,
                  semaphore: asyncio.Semaphore= None,
                  accept: str= '*/*'):
        

        header = {'Accept' : accept, 
                  'User-Agent': self._user,
                  "Accept-Language": "ru,en;q=0.9",
                  "Accept-Encoding": "gzip, deflate, br",
                  "Connection": "keep-alive",
                  "Upgrade-Insecure-Requests": "1",
                  "Sec-Fetch-Dest": "document",
                  "Sec-Fetch-Mode": "navigate",
                  "Sec-Fetch-Site": "none",
                  "Sec-Fetch-User": "?1",
                  "Referer": "https://23met.ru/"}
        

        async def read_data_in_site():
            data = None
            if semaphore:
                async with semaphore:
                    # Чтение данных из сайта
                    async with session.get(url= url, 
                                        headers= header) as response:
                        data = await response.text()
                        
            else:
                # Чтение данных из сайта
                async with session.get(url= url, 
                                        headers= header) as response:
                    data = await response.text()
            return data

        data = await read_data_in_site()
        while BeautifulSoup(data, 'lxml').find('title').text == 'Слишком много запросов':
            print("Блокировка! Жду 2 минуты и пробую снова...")
            header['User-Agent'] = UserAgent().random
            await asyncio.sleep(120)
            data = await read_data_in_site()

        await asyncio.sleep(random.uniform(1, 3))
        return data

    async def _read_main_site(self, url: str, accept: str= "*/*"):

        # Забираем данные с основного сайта и кладем их в файла 
        data = None
        semaphore = asyncio.Semaphore(1)
        async with aiohttp.ClientSession() as session:
            data = await self.get(session= session, 
                                  url= url, 
                                  semaphore= semaphore,
                                  accept= accept)
        
        return data

class WorkerWithFiles(Readable, Writable):

    def __init__(self):
        super().__init__()

    async def get(self, path: str):
        data = None
        async with aiofiles.open(file= path) as file:
            data = await file.read()
        return data
    
    async def put(self, path: str, data: Any):
        async with aiofiles.open(file= path, mode= 'w') as file:
            await file.write(data)
    
    async def _put_json_file(self, path: str, data: Any):
        async with aiofiles.open(path, 'w') as file:
            json_str = json.dumps(obj= data, indent= 4, ensure_ascii= False) 
            await file.write(json_str)

class Parser(ABC):
    def __init__(self, base_url: str):
        self.__file_worker = WorkerWithFiles()
        self.__html_worker = WorkerWithHtml()
        self.base_url = base_url


    @abstractmethod
    def parsing(): pass

    @property
    def user_agent(self):
        return self.__html_worker.user
    
    async def put_file(self, path: str, data: Any):
        await self.__file_worker.put(path= path, data= data)
    

    async def get_file(self, path: str):
        return await self.__file_worker.get(path= path)

    
    async def get_html(self, 
                       session: aiohttp.ClientSession,
                       url: str,
                       semaphore: asyncio.Semaphore= None,
                       accept: str= '*/*'):
        return await self.__html_worker.get(session= session, url= url, semaphore= semaphore, accept= accept)
    

    async def _read_main_site_and_save(self, 
                                       path_main_site: str, 
                                       file_name_for_main_site: str,
                                       accept: str= "*/*"):
        
        data = await self.__html_worker._read_main_site(self.base_url, 
                                                        accept= accept)
        
        # # сохраняет данные в файл
        # if BeautifulSoup(data, 'lxml').find(name= 'title').text == "Слишком много запросов":
        #     raise Exception("Вас заблокировали!")
        
        path= os.path.join(path_main_site, file_name_for_main_site)
        await self.put_file(path= path, data= data)
        

    async def _save_data_in_json_file(self, path: str, data: Any):
        await self.__file_worker._put_json_file(path, data)
