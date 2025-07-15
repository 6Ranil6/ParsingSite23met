from parser_23MET import ParserSite_23MET
from proxyParser import ParserProxyLib
import asyncio 
import pandas as pd

async def main(with_proxy= False):
    if with_proxy:
        proxy = ParserProxyLib(max_rate= 100, time_period= 1)
        await proxy.parsing(url_for_checking= 'https://23met.ru/')
        proxy_list = proxy.get_sockets()
        if not proxy_list:
            proxy_list = None
        main_parser = ParserSite_23MET(max_rate= 100, proxy_list= proxy_list)
    else:
        main_parser = ParserSite_23MET(max_rate= 100)
    df =await main_parser.run(stop= 300,
                              with_update_sites_info= True, 
                              with_remove_intermediate_data= True)
    df: pd.DataFrame
    print(f"Размер спращенных данных:", len(df))
    # asyncio.run(a.parsing(with_update_sites_info= True, stop= 400)) #Если нужно обновить данные от поиска в Google


if __name__ == "__main__":
    asyncio.run(main())
