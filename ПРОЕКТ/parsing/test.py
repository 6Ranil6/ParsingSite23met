from proxyParser import ParserProxyLib
from parser_23MET import ParserSite_23MET
from time import time
import asyncio 

async def main():
    st = time()
    proxy = ParserProxyLib(base_url= "https://proxylib.com/free-proxy-list")
    await proxy.parsing(MAX_TASKS= 25)
    worker = ParserSite_23MET(base_url= "https://23met.ru/price",
                              proxy_lib= proxy)
    await worker.parsing(MAX_TASKS= 25)
    print(f"ALL_TIME = {time() - st} sec")
asyncio.run(main())
     

