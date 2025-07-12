from proxyParser import ParserProxyLib
from parser_23MET import ParserSite_23MET
from time import time
import asyncio 

async def main():
    st = time()

    # # --- ЭТАП 1: Получение списка прокси ---
    print("--- ЭТАП 1: Начинаю парсинг прокси-серверов ---")
    proxy_parser = ParserProxyLib(base_url="https://proxylib.com/free-proxy-list")
    
    await proxy_parser.parsing() 
    proxy_list = proxy_parser.get_sockets()
    
    if not proxy_list:
        print("Не удалось получить список прокси. Завершение работы.")
        return
        
    print(f"Получено {len(proxy_list)} HTTPS прокси. Перехожу к следующему этапу.")
    print("-" * 50)

    # --- ЭТАП 2: Парсинг целевого сайта с использованием прокси ---
    print("--- ЭТАП 1: Начинаю парсинг 23met.ru с использованием прокси ---")
    worker = ParserSite_23MET(base_url="https://23met.ru/price",
                              proxy_list=proxy_list)
    
    await worker.parsing(MAX_TASKS=25)
    
    print("-" * 50)
    print(f"ALL_TIME = {time() - st} sec")

    # # --- ЭТАП 2: Парсинг целевого сайта с использованием прокси ---
    # print("--- ЭТАП 1: Начинаю парсинг 23met.ru с использованием прокси ---")
    # worker = ParserSite_23MET(base_url="https://23met.ru/price",
    #                           proxy_list= [''])
    
    # await worker.parsing(MAX_TASKS= 1)
    
    # print("-" * 50)
    # print(f"ALL_TIME = {time() - st} sec")

asyncio.run(main())
