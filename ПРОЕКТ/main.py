from parser_23MET import ParserSite_23MET
import asyncio 

async def main():
    a = ParserSite_23MET(max_rate= 100)
    # asyncio.run(a.parsing(with_update_sites_info= True, stop= 400)) #Если нужно обновить данные от поиска в Google
    await a.parsing()


if __name__ == "__main__":
    asyncio.run(main())
