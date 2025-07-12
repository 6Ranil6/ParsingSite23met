from parser_23MET import ParserSite_23MET
from proxyParser import ParserProxyLib
import asyncio 


# async def main():
#     prox = ParserProxyLib()
#     await prox.parsing(url_for_checking= 'https://23met.ru/')

#     prox_list = prox.get_sockets()
#     if prox_list:
#         a = ParserSite_23MET(max_rate= 1, proxy_list= prox_list, time_period= 10)
#         await a.save_data(dir_name= 'Test')
#     else:
#         print("Не оказалось рабочих прокси серверов")

async def main():
    a = ParserSite_23MET(max_rate= 1, time_period= 5)
    await a.save_data(dir_name= 'DATA_FROM_PARSING')

if __name__ == "__main__":
    asyncio.run(main())
