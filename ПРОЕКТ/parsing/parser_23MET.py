from base import Parser
import aiohttp
import asyncio
import os
from bs4 import BeautifulSoup
import re
from aiohttp import TCPConnector

class ParserSite_23MET(Parser):
    def __init__(self, base_url= "https://23met.ru/price",
                        proxy_list: list= None):
        super().__init__(base_url, proxy_list)

    @staticmethod
    def sanitize_filename(filename: str) -> str:
        # Заменяем все недопустимые символы на "_"
        return re.sub(r'[\\/:"*?<>|]+', '_', filename)
    
    async def __get_hrefs_for_next_sites(self,
                                         file_path: str):
        
        #сначало парсим основной сайт
        html_file = await self.get_file(path= file_path)
        soup = BeautifulSoup(markup= html_file, 
                             features= 'lxml')
        
        items_html_with_href= soup.find(name= 'nav', 
                                        attrs={"id" : "left-container", "class" : "left-container-mainpage-static"})\
                                  .find(name= 'ul')\
                                  .find_all(name= 'a')
        
        # формируем json с ссылками на сайты, внутри которых будут еще одни сайты с сылками на данные
        hrefs_next_sites = dict()
        for item in items_html_with_href:
            item: BeautifulSoup
            item_name = item.get_text()
            item_href = self.base_url + item.get('href').replace('/price', '')
            hrefs_next_sites[item_name] = item_href

        return hrefs_next_sites

    async def _fetch_and_save_one(self, session, semaphore, url, path, detail_name, accept):
        """Скачивает и сразу сохраняет одну страницу."""

        async with semaphore:
            print(f"Начинаю обработку {detail_name}...")
            # ВАЖНО: get_html теперь должен принимать сессию, так как мы ее создаем снаружи
            html = await self.get_html(session=session, url=url, accept=accept)

            if html:
                safe_name = ParserSite_23MET.sanitize_filename(filename= detail_name)
                file_path = os.path.join(path, f"{safe_name}.html")
                await self.put_file(path=file_path, data=html)
                print(f"Сохранил {detail_name} в {file_path}")
            else:
                print(f"Не удалось скачать {detail_name} с URL: {url}")

    async def parsing(self,
                      file_name_for_main_site= 'main_site.html',
                      accept= '*/*',
                      dir_name= None,
                      MAX_TASKS= 25):
        
        path = os.getcwd()

        if dir_name:
            path = os.path.join(path, dir_name)
            if not os.path.isdir(path):
                os.mkdir(path)
                print(f"Создал папку {dir_name} по пути {path}")
            else:
                print(f"Не стал создавать папку {dir_name}, т.к она уже существует!")
        
        # --- ЦЕНТРАЛИЗОВАННОЕ СОЗДАНИЕ СЕССИИ ---
        # Создаем коннектор и сессию ОДИН РАЗ для всех запросов этого парсера
        connector = TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total= 20, connect=10)
        async with aiohttp.ClientSession(connector=connector, timeout= timeout) as session:
            try:
                # --- Скачиваем главный сайт, используя созданную сессию ---
                print("Скачиваю главную страницу...")
                main_html = await self.get_html(session=session, url=self.base_url, accept=accept)
                
                if not main_html:
                    raise Exception("Не удалось скачать главную страницу, завершаю работу.")
                
                main_file_path = os.path.join(path, file_name_for_main_site)
                await self.put_file(path=main_file_path, data=main_html)
                print(f"Сохранил главную страницу в {main_file_path}")

            except Exception as ex:
                print(ex)
                return ex

            # Получаем json с ссылками на сайты
            hrefs_next_sites = await self.__get_hrefs_for_next_sites(main_file_path) 

            semaphore = asyncio.Semaphore(MAX_TASKS)
            
            SUBMAIN_DIR_NAME = 'submain_sites'
            dir_path = os.path.join(path, SUBMAIN_DIR_NAME) 
            counter= 0
            while os.path.isdir(dir_path):
                dir_path += str(counter)
                counter += 1
            os.mkdir(dir_path)
            
            tasks = []
            for detail_name, submain_url in hrefs_next_sites.items():
                task = asyncio.create_task(
                    self._fetch_and_save_one(session, semaphore, submain_url, dir_path, detail_name, accept)
                )
                tasks.append(task)
            await asyncio.gather(*tasks)
