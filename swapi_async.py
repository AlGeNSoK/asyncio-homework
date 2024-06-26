import asyncio
import aiohttp
from more_itertools import chunked
from models import init_orm, SwapiPeople, Session

import datetime


async def get_value(urls, key_v):
    if isinstance(urls, str):
        urls = [urls]
    list_names = []
    async with aiohttp.ClientSession() as http_session:
        for url in urls:
            response = await http_session.get(url)
            json_data = await response.json()
            list_names.append(json_data.get(key_v))
        return ", ".join(list_names)


async def get_people(http_session, person_id):
    async with aiohttp.ClientSession() as session:
        response = await http_session.get(f"https://swapi.py4e.com/api/people/{person_id}")
        json_data = await response.json()
        return json_data


async def insert_to_database(json_list):
    async with Session() as session:
        orm_objects = []
        for item in json_list:
            if item.get("name") is None:
                continue
            orm_object = SwapiPeople(
                birth_year=item.get("birth_year"),
                eye_color=item.get("eye_color"),
                films=await get_value(item.get("films"), 'title'),
                gender=item.get("gender"),
                hair_color=item.get("hair_color"),
                height=item.get("height"),
                homeworld=await get_value(item.get("homeworld"), 'name'),
                mass=item.get("mass"),
                name=item.get("name"),
                skin_color=item.get("skin_color"),
                species=await get_value(item.get("species"), 'name'),
                starships=await get_value(item.get("starships"), 'name'),
                vehicles=await get_value(item.get("vehicles"), 'name')
            )
            orm_objects.append(orm_object)

        session.add_all(orm_objects)
        await session.commit()


MAX_REQUESTS = 5


async def main():
    await init_orm()
    http_session = aiohttp.ClientSession()
    for chunk_i in chunked(range(1, 101), MAX_REQUESTS):
        coros = [get_people(http_session, i) for i in chunk_i]
        result = await asyncio.gather(*coros)
        asyncio.create_task(insert_to_database(result))
    await http_session.close()
    tasks = asyncio.all_tasks()
    current_task = asyncio.current_task()
    tasks.remove(current_task)
    await asyncio.gather(*tasks)


start_time = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start_time)
