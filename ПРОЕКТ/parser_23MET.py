from base import Parser
import aiohttp
import asyncio
import aiolimiter
import os
import re
from GoogleParser import GoogleParser
from bs4 import BeautifulSoup
import pandas as pd

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
        self.__file_paths = None
        self.__unique_columns_name = None

    async def __get_and_save_site_data(self, session, url:str, accept):
        data = await self.get_html(session= session,
                                   url= url,
                                   accept= accept)
        if self.__checking(data):
            file_path = os.path.join(self._dir_path, url.split('/')[-1] + ".html")
            await self.put_file(path= file_path, data= data)
    
    async def __process_single_url_with_limiter(self, session, url, accept):
        async with self.__limiter:
            await self.__get_and_save_site_data(session=session, url=url, accept=accept)

    def __checking(self, html):
        try:
            soup = BeautifulSoup(html, 'lxml')
        except TypeError:
            print(html, "тип None")
            return False
        if re.search(r"прайс-лист — 23MET.ru\Z", soup.find('title').text):
            return True
        return False

    async def save_data(self,
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
    
    async def __get_one_site_unique_columns_name(self, file_path):
        html = await self.get_file(file_path)
        unique_column_names = set()
        if self.__checking(html):
            soup = BeautifulSoup(html, 'lxml')
            tables = soup.find_all('table', 'tablesorter')
            for table in tables:
                table: BeautifulSoup
                columns_name = table.find('thead').find_all('th')
                for column_name in columns_name:
                    unique_column_names.add(column_name.text)
            return unique_column_names
        else:
            return None
        
    async def __get_all_unique_columns_name(self):
        tasks = []
        if not self.__file_paths:
            print("Не был инициализирован self.__file_paths. Создаю его сам")
            file_names = os.listdir(self._dir_path)
            self.__file_paths = [os.path.join(self._dir_path, file_name) for file_name in file_names]
        
        for file_path in self.__file_paths:
            tasks.append(asyncio.create_task(self.__get_one_site_unique_columns_name(file_path)))
        results = [result for result in await asyncio.gather(*tasks) if result]
        return list({item for result in results  for item in result})

    
    async def _parsing_one_site(self, file_path):
        html = await self.get_file(file_path)
        data = dict()
        if not self.__unique_columns_name:
            print("Переменная self.__unique_columns_name не была инициализирована. Инициализирую ее!")
            self.__unique_columns_name = await self.__get_all_unique_columns_name()
        for column_name in self.__unique_columns_name:
            data[column_name] = []

        if self.__checking(html):
            soup = BeautifulSoup(html, 'lxml')
            tables = soup.find_all('table', 'tablesorter')

            for table in tables:
                table: BeautifulSoup
                columns_name = [column.text for column in table.find('thead').find_all('th')]
                trS_in_tbody = table.find('tbody').find_all('tr')
                for tr_in_tbody in trS_in_tbody:
                    tdS_in_tbody = tr_in_tbody.find_all('td')
                    for column_name, td_in_tbody in zip(columns_name, tdS_in_tbody):
                        if td_in_tbody.text == '':
                            data[column_name].append(None)
                        else:
                            data[column_name].append(td_in_tbody.text)
                    
                    for unique_column_name in self.__unique_columns_name:
                        if unique_column_name not in columns_name:
                            data[unique_column_name].append(None)
            return data
        
        else:
            return None
    def __delete_intermediate_data(self):
        for file_path in self.__file_paths:
            os.remove(file_path)

    async def parsing(self, with_save_result= True):
        file_names = os.listdir(self._dir_path)
        self.__file_paths = [os.path.join(self._dir_path, file_name) for file_name in file_names]
        self.__unique_columns_name = await self.__get_all_unique_columns_name()
        data = dict()
        for column_name in self.__unique_columns_name:
            data[column_name] = []

        main_df = pd.DataFrame(data)
        tasks = []
        for file_path in self.__file_paths:
            tasks.append(asyncio.create_task(self._parsing_one_site(file_path)))
        results = await asyncio.gather(*tasks)
        
        sites_without_needing_data= []
        df_s = dict()
        for index, result in enumerate(results):
            if not result:
                sites_without_needing_data.append(self.__file_paths[index])
            else:
                try:
                    df_s[self.__file_paths[index]] = pd.DataFrame(data= result)
                except ValueError:
                    print("Не все масивы одной длинны тут:", self.__file_paths[index])

        if sites_without_needing_data:
            print("Эти сайты не подходят под шаблон парсинга:", sites_without_needing_data)
        
        main_df = pd.concat(list(df_s.values()), ignore_index=True)
        main_df = main_df.sort_values(by= 'Наименование', ignore_index=True)
        if with_save_result:
            main_df.to_csv(os.path.join(self._dir_path, 'result.csv'))
        return main_df
        

    async def run(self,
                  accept= '*/*',
                  num= 100,
                  start= 0,
                  stop= 100,
                  with_update_sites_info= False,
                  with_save_result= True,
                  with_remove_intermediate_data= False):
        print("Начинаю процесс скачивания данных с сайта")
        await self.save_data(accept= accept,
                             with_update_sites_info= with_update_sites_info,
                             num= num,
                             start= start,
                             stop= stop)
        
        print("Начинаю процесс забора данных со скаченных сайтов")
        await self.parsing(with_save_result= with_save_result)

        if with_remove_intermediate_data:
            print("Удаляю все промежуточные данные")
            self.__delete_intermediate_data()
