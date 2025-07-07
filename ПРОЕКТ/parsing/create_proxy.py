from base import Parser
import asyncio
import aiohttp
import os

class ParserProxyLib(Parser):
    def __init__(self, base_url):
        super().__init__(base_url)

    async def _fetch_and_save_site(self, dir_path, name_file, session, url, semaphore, accept= '*/*'):
        html = await self.get_html(session= session,
                                   url= url,
                                   semaphore= semaphore,
                                   accept= accept)

        file_path = os.path.join(dir_path, name_file)
        if html:
            await self.put_file(path= file_path, data= html)
            print(f"Сохранил данные с {url} в {file_path}")
        else:
            print(f"Не удалось скачать с {url}")

    async def parsing(self,
                      dir_name= "PROXY",
                      CONECTION_PROTOCOL_TYPE= 'https',
                      LIMIT_MINIT= 20, 
                      MAX_PAGES= 77,
                      MAX_TASKS= 5):

        # Создаем директорию в которой будем сохранять все данные
        dir_path = os.path.join(os.getcwd(), dir_name)
        
        if not os.path.isdir(dir_path):
            os.mkdir(dir_path)
        
        

        semaphore = asyncio.Semaphore(MAX_TASKS)
        data= None
        async with aiohttp.ClientSession() as session:
            tasks= []
            page_num = 1
            while page_num <= MAX_PAGES:
                url = self.base_url + f"/?proxy_page={page_num}"
                task= asyncio.create_task(self._fetch_and_save_site(dir_path=dir_path,
                                                                    name_file= f"Page{page_num}.html",
                                                                    session= session,
                                                                    url= url,
                                                                    semaphore= semaphore,
                                                                    accept= '*/*')) # Скачиваем данные с сайта
                tasks.append(task)
                page_num+= 1
            print("Запускаю задачи")
            data= await asyncio.gather(*tasks)
        print(data)

def main():
    # проверил вручную page всего 77 штук
    base_url= "https://proxylib.com/free-proxy-list"
    a = ParserProxyLib(base_url)
    asyncio.run(a.parsing())


if __name__ == "__main__":
    main()
