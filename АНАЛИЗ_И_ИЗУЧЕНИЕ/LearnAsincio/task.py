import asyncio
import aiohttp
import aiofiles
import requests
from typing import Dict, Any, Union
import os
from fake_useragent import UserAgent
from bs4 import BeautifulSoup

def get_html_file(url: str, path: str, file_name:str = "file.txt", header: Dict[str, str] = None) -> bool:
    """
    Функция которая создает файл с html разметкой сайта `url`

    Args:
        url (str): URL
        header (Dict[str, str], optional): Http-header. Defaults to None.
        file_name (str): С каким названием создать файл? Default to file.txt
        path (str): Путь по которому сохранять файл

    Returns:
        bool: Получилось ли создать файл?
    """
    html = None
    all_path = os.path.join(path, 'data')

    with requests.get(url= url, 
                      headers= header) as resp:
        print(f"Закончил парсить сайт {resp.url}")
        html = resp.text

    if not os.path.isdir(all_path):
        os.mkdir(all_path)

    key = False
    file_path = os.path.join(all_path, file_name)
    try:
        with open(file_path, 'w') as f:
            f.write(html)
            key= True

    except:
        print("Не удалось записать в файл данные!")
        raise

    finally:
        return key


def read_html_file_and_get_href(path: str) -> Dict[str, str]:
    """
    Читает все в файле и формирует json

    Args:
        path (str): путь к файлу, который нужно спарсить

    Returns:
        Dict[str, str]: json НАЗВАНИЕ ПРОДУКТА = ссылка на продукт
    """
    data = None
    with open(path) as f:
        data = f.read() 

    soup = BeautifulSoup(markup= data, 
                         features= 'lxml')
    product_list = soup.find(name= 'div', attrs= {"id" : "frts"}).next_sibling.next_sibling.find_all('li')
    json = dict() 
    for product in product_list:
        item_name = product.find('a').get('title')
        item_href = product.find('a').get('href')
        json[item_name] = item_href

    return json

async def create_fruits_file(fruit_name: str, path: str, data: Any) -> None:
    
    file_path = path + fruit_name.capitalize() + ".html"
    try:
        async with aiofiles.open(file_path, 'w') as file:
            await file.write(data)
    except:
        print("Что-то пошло не так, при записе файла")
        raise

async def get_html(session: aiohttp.ClientSession, href: str) -> Union[None, str]:
    html = None
    try:
        async with session.get(href) as response:
            html = await response.text()
    except:
        print(f"Что-то пошло не так при получении данных из html страницы {href}")
        raise
    finally:
        return html


async def main():
    url = "https://fitaudit.ru/categories/fds"
    user = UserAgent().random
    header = {'Accept' : "*/*", 
              'user-agent' : user}
    path = os.getcwd()
    file_name = "fruits.html"

    get_html_file(url= url,
                  path= path,
                  file_name= file_name,
                  header= header)
    
    fruit_href = read_html_file_and_get_href(os.path.join(path, 'data', file_name))
    print(fruit_href)
    hrefs = fruit_href.values()
    fruits = fruit_href.keys()


    get_html_tasks =[] 
    async with aiohttp.ClientSession() as session: 
        for href in hrefs:
            task = asyncio.create_task(get_html(session= session, href= href))
            get_html_tasks.append(task)
    
        data = await asyncio.gather(*get_html_tasks)

    create_files_tasks = []
    
    if len(data) != len(fruits):
        print('Количество фруктов не совпало с количеством html-страниц')
        return "ОШИБКА!"
    
    else:
        for fruit, d in zip(fruits, data):
            task = asyncio.create_task(create_fruits_file(fruit_name= fruit, path= os.path.join(path, 'data'), data= d))

        await asyncio.gather(*create_files_tasks)

if __name__ == "__main__":
    asyncio.run(main()) 
