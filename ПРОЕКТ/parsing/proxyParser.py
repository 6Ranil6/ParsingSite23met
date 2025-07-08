from base import Parser
import asyncio
import aiohttp
import os
from bs4 import BeautifulSoup
import re
import json

class ParserProxyLib(Parser):
    def __init__(self, base_url= "https://proxylib.com/free-proxy-list"):
        super().__init__(base_url)

    async def _fetch_and_save_site(self, name_file, session, url, semaphore, accept= '*/*'):
        html = await self.get_html(session= session,
                                   url= url,
                                   semaphore= semaphore,
                                   accept= accept)

        file_path = os.path.join(self.__dir_path, name_file)
        if html:
            await self.put_file(path= file_path, data= html)
            print(f"Сохранил данные с {url} в {file_path}")
        else:
            print(f"Не удалось скачать с {url}")
    
    def __delete_all_page_files(self, MAX_PAGES):
        for page_num in range(MAX_PAGES + 1):
            name_file = f"Page{page_num}.html"
            os.remove(os.path.join(self.__dir_path, name_file))

    async def _GET_socket_and_type(self, 
                                   file_path):
        """
        Сразу парсит и сокеты и тип соединения
        """
        html = await self.get_file(path= file_path)
        soup = BeautifulSoup(html, 'lxml')
        data_list = soup.find_all(name= 'td')
        sockets= []
        types= []

        for data in data_list:
            data: BeautifulSoup
            a_tag_with_onclick = data.find(name= 'a', onclick= True)
            
            try:
                a_tag_title = data.find(name='a')['title']
            except:
                a_tag_title = None

            if a_tag_with_onclick:
                match = re.search(pattern= r"copyToClipboard", string= a_tag_with_onclick['onclick'])
                # match = re.search(pattern=)
                if match:
                    sockets.append(a_tag_with_onclick.get_text().strip())
            
            if a_tag_title:
                match = re.match(pattern= r'Free', string= a_tag_title)
                if match:
                    types.append(a_tag_title.split()[1])
        
        return zip(types, sockets)
    
    def _create_dir(self, dir_name= "PROXY"):
        # Создаем директорию в которой будем сохранять все данные
        self.__dir_path = os.path.join(os.getcwd(), dir_name)
        
        if not os.path.isdir(self.__dir_path):
            os.mkdir(self.__dir_path)    

    async def parsing(self,
                      dir_name= "PROXY",
                      CONECTION_PROTOCOL_TYPE= 'https',
                      MAX_PAGES= 77,
                      MAX_TASKS= 25,
                      delete_all_page_files= True):
        
        self._create_dir(dir_name)

        semaphore = asyncio.Semaphore(MAX_TASKS)
        async with aiohttp.ClientSession() as session:
            tasks= []
            page_num = 1
            for page_num in range(MAX_PAGES + 1):
                url = self.base_url + f"/?proxy_page={page_num}"
                name_file = f"Page{page_num}.html"

                task= asyncio.create_task(self._fetch_and_save_site(name_file= name_file,
                                                                    session= session,
                                                                    url= url,
                                                                    semaphore= semaphore,
                                                                    accept= '*/*'))# Скачиваем данные с сайта
                
               
                tasks.append(task)

            print("Запускаю задачи на чтение всех страниц сайта и сохрание всего в файлы!")
            await asyncio.gather(*tasks)
            
            tasks2= []
            for page_num in range(MAX_PAGES + 1):
                name_file = f"Page{page_num}.html"
                task2= asyncio.create_task(self._GET_socket_and_type(file_path= os.path.join(self.__dir_path, name_file)))# Вынимаем данные из сайта
                tasks2.append(task2)

            print(f"Запускаю парсинг сокетов и их типов({CONECTION_PROTOCOL_TYPE})")
            
            json = {CONECTION_PROTOCOL_TYPE.upper() : []}
            for pair in await asyncio.gather(*tasks2):
                for t, s in pair:
                    if t == CONECTION_PROTOCOL_TYPE.upper():
                        json[t].append(f"{CONECTION_PROTOCOL_TYPE.lower()}://" + s)
            
            if json:
                # сохраняем JSON в файл: "proxy.json"
                await self._save_data_in_json_file(path= os.path.join(self.__dir_path, 'proxy.json'), 
                                               data= json)
                print("Все завершилось успешно!")
            else:
                print(f"Не нашлось PROXY с {CONECTION_PROTOCOL_TYPE}")

        if delete_all_page_files:
            print("Удаляю все промежуточные данные!")
            self.__delete_all_page_files(MAX_PAGES= MAX_PAGES)
            
        update_json= {"base_url": self.base_url,
                      "dir_name": dir_name,
                      "CONECTION_PROTOCOL_TYPE": CONECTION_PROTOCOL_TYPE.upper(),
                      "MAX_PAGES": MAX_PAGES,
                      "MAX_TASKS": MAX_TASKS,
                      "delete_all_page_files": delete_all_page_files}
        
        await self._save_data_in_json_file(path= os.path.join(dir_name, 'update_setting.json'), data= update_json)

    async def update(self):
        try:
            with open(file= os.path.join(self.__dir_path, 'update_setting.json')) as f:
                data = json.load(fp= f)
        except FileNotFoundError as exp:
            print("Вероятнее всего, вы еще ниразу не применяли parsing()! Поэтому я сам вызову его, но с базовыми параметрами!")
            self.parsing()
        del data['base_url']
        await self.parsing(**data)
    
    def get_sockets(self):
        try:
            protocol_type = None
            with open(file= os.path.join(self.__dir_path, 'update_setting.json')) as file:
                protocol_type = json.load(file)["CONECTION_PROTOCOL_TYPE"]
        except AttributeError:
            self._create_dir()
            with open(file= os.path.join(self.__dir_path, 'update_setting.json')) as file:
                protocol_type = json.load(file)["CONECTION_PROTOCOL_TYPE"]
        with open(file= os.path.join(self.__dir_path, 'proxy.json')) as f:
            return json.load(f)[protocol_type]
