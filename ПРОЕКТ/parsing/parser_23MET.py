from base import Parser
import aiohttp
import asyncio
import aiolimiter
import os
from bs4 import BeautifulSoup
import re
import json
from time import time

class ParserSite_23MET(Parser):
    def __init__(self, base_url= "https://23met.ru/sitemap.xml",
                       proxy_list: list= None,
                       max_rate= 1,
                       time_period= 10):
        

        super().__init__(base_url, proxy_list)
        # В sitemap-е указано crawl delay = 10 -> 1 запрос в 10 секунд должен происходить...
        self.__limiter = aiolimiter.AsyncLimiter(max_rate= max_rate,
                                                 time_period= time_period)

    @staticmethod
    def sanitize_filename(filename: str) -> str:
        # Заменяем все недопустимые символы на "_"
        return re.sub(r'[\\/:"*?<>|]+', '_', filename)


    async def _get_html_sitemap_of_main_site(self, session):
        # Находим SITEMAP XML главного сайта и сохраняем данные в файл
        response = await self.get_html(url= self.base_url,
                                       session= session,
                                       accept= self.__accept)
        await self.put_file(path= os.path.join(self.__path, self.__file_name_for_main_site),
                            data= response)
            

    def _parsing_main_site(self):
        "Парсим с главного сайта все sitemap-ы"
        response = self.get_data_in_file_no_async(file_path= os.path.join(self.__path, self.__file_name_for_main_site))
        soup = BeautifulSoup(markup= response,
                             features= 'lxml')
        
        return [el.get_text() for el in soup.find_all(name= 'loc')]


    async def get_html(self, session, url, accept = '*/*'):
        """Переопределил метод get_html, таким образом, чтобы limiter срабатывал"""
        async with self.__limiter:
            return await super().get_html(session= session, 
                                          url= url,
                                          accept= accept)
        

    async def download_AND_save(self, session, url, file_path, accept = '*/*'):
        response = await self.get_html(session= session, 
                                       url= url,
                                       accept= accept)
        await self.put_file(file_path, response)
        self.__counter+= 1
        print(f"Скачал и сохранил: {self.__counter} из {self.__num_all_tasks} ..... ({self.__counter/self.__num_all_tasks:.2%})")


    async def _parsing_sitemap_files(self, file_path):
        data = await self.get_file(file_path)
        soup = BeautifulSoup(data, 'lxml')
        hrefs = [href.get_text() for href in soup.find_all(name= 'loc')]
        self.__counter += 1
        print(f"Спарсил: {self.__counter} из {self.__num_all_tasks} ..... ({self.__counter/self.__num_all_tasks:.2%})")
        return hrefs

    async def _save_site_with_data_in_file(self, dir_path, session, url, accept= '*/*'):
        data = await self.get_html(session= session,
                                   url= url,
                                   accept= accept)
        file_name_el = url.split('/')
        await self.put_file(os.path.join(dir_path, "_".join([file_name_el[-2], file_name_el[-1]])), data)
        self.__counter += 1
        print(f"Скачал и сохранил: {self.__counter} из {self.__num_all_tasks} ..... ({self.__counter/self.__num_all_tasks:.2%})")
  
    # async def _parsing_site_with_data_from_file(self, dir_path):
    #     data = await self.get_file(dir_path)
    #     soup = BeautifulSoup()

    def __sanitize_hrefs_file(self):
        STOPWORD = "https://23met.ru/services"
        with open(self.__hrefs_path) as file:
            raw_urls = json.load(file)

        raw_urls = raw_urls[:raw_urls.index(STOPWORD)]

        unique_base_urls = {url.split('?')[0].rstrip('/') for url in raw_urls}

        sorted_urls = sorted(list(unique_base_urls))

        category_urls = set()
        for i in range(len(sorted_urls) - 1):
            current_url = sorted_urls[i]
            next_url = sorted_urls[i+1]
            if next_url.startswith(current_url + '/'):
                category_urls.add(current_url)

        final_urls = [
            url for url in sorted_urls 
            if url not in category_urls
        ]

        self.__hrefs_path2 = os.path.join(self.__path, 'hrefs_on_data.json')
        with open(self.__hrefs_path2, 'w') as file:
            json.dump(final_urls, file, indent= 4, ensure_ascii= False)


    async def save_data(self,
                        file_name_for_main_site= 'main_site.html',
                        accept= '*/*',
                        dir_name= None):
        
        self.__file_name_for_main_site = file_name_for_main_site
        self.__accept = accept

        path = os.getcwd()

        if dir_name:
            path = os.path.join(path, dir_name)
            if not os.path.isdir(path):
                os.mkdir(path)
                print(f"Создал папку {dir_name} по пути {path}")
            else:
                print(f"Не стал создавать папку {dir_name}, т.к она уже существует!")
        self.__path = path
        self.__hrefs_path = os.path.join(self.__path, 'all_hrefs_on_data.json')

        async with aiohttp.ClientSession() as session:

            await self._get_html_sitemap_of_main_site(session= session)
            sitemap_urls = self._parsing_main_site()

            self.__num_all_tasks = len(sitemap_urls) # нужно просто для красивого отображения
            self.__counter = 0 # нужно просто для красивого отображения

            tasks = []
            file_paths = []
            for url in sitemap_urls:
                file_path = os.path.join(self.__path, 
                                         ParserSite_23MET.sanitize_filename(url.split('/')[-1]))
                file_paths.append(file_path)

                task = asyncio.create_task(self.download_AND_save(session= session,
                                                                  url= url,
                                                                  file_path= file_path,
                                                                  accept= accept))
                tasks.append(task)
            await asyncio.gather(*tasks)
            
            self.__counter = 0 
            tasks = []
            for file_path in file_paths:
                tasks.append(asyncio.create_task(self._parsing_sitemap_files(file_path= file_path)))
            hrefs = [item for result_list in await asyncio.gather(*tasks) for item in result_list] 
            await self._save_data_in_json_file(self.__hrefs_path, hrefs)
            self.__sanitize_hrefs_file()
            
            with open(self.__hrefs_path2) as file:
                urls = json.load(file)
            
            

            self.__dir_sites = os.path.join(self.__path, "Data")
            os.makedirs(self.__dir_sites, exist_ok=True)

            self.__num_all_tasks = 36659
            self.__counter = 0
            tasks = []
            for url in urls:
                task = asyncio.create_task(self._save_site_with_data_in_file(dir_path= self.__dir_sites,
                                                                             session= session,
                                                                             url= url)) 
                tasks.append(task)

            start = time()
            await asyncio.gather(*tasks)
            print(f"Time =", time() - start)

    async def parsing(self,
                      file_name_for_main_site= 'main_site.html',
                      accept= '*/*',
                      dir_name= None):
        
        await self.save_data(file_name_for_main_site= file_name_for_main_site,
                             accept= accept,
                             dir_name= dir_name)
