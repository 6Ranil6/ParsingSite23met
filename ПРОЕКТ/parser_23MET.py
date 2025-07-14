from base import Parser
import aiohttp
import asyncio
import aiolimiter
import os
import re
from GoogleParser import GoogleParser

class ParserSite_23MET(Parser):
    def __init__(self, base_url= "https://23met.ru",
                       proxy_list: list= None,
                       max_rate= 1,
                       time_period= 10):
        

        super().__init__(base_url, proxy_list)
        # В sitemap-е указано crawl delay = 10 -> 1 запрос в 10 секунд должен происходить...
        self.__limiter = aiolimiter.AsyncLimiter(max_rate= max_rate,
                                                 time_period= time_period)
        DIR_NAME = "23MET_DATA"
        os.makedirs(DIR_NAME, exist_ok=True)
        self._dir_path = os.path.join(os.getcwd(), DIR_NAME) 

    @staticmethod
    def sanitize_filename(filename: str) -> str:
        # Заменяем все недопустимые символы на "_"
        return re.sub(r'[\\/:"*?<>|]+', '_', filename)

    async def __get_and_save_site_data(self, session, url:str, accept):
        data = await self.get_html(session= session,
                                   url= url,
                                   accept= accept)
        file_path = os.path.join(self._dir_path, url.split('/')[-1] + ".html")
        await self.put_file(path= file_path, data= data)
    
    async def __process_single_url_with_limiter(self, session, url, accept):
        async with self.__limiter:
            await self.__get_and_save_site_data(session=session, url=url, accept=accept)

    async def parsing(self,
                      accept= '*/*',
                      with_update_sites_info= False,
                      num= 100,
                      start= 0,
                      stop= 100):
        
        google_searcher = GoogleParser(query_for_browser= 'site:23met.ru прайс-лист')
        if with_update_sites_info:
            await google_searcher.run(num= num, 
                                      start= start, 
                                      stop= stop)
        else:
            await google_searcher.parsing()
        
        urls = google_searcher.get_urls()
            
        async with aiohttp.ClientSession() as session:
            tasks = []
            for url in urls:
                task = asyncio.create_task(self.__process_single_url_with_limiter(session= session, url= url, accept= accept))
                tasks.append(task)
            await asyncio.gather(*tasks)
