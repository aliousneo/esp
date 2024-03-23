from __future__ import annotations
from lxml import etree
import os
from pathlib import Path
from typing import ClassVar

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_streaming_bulk


class AsyncElasticService:
    client: ClassVar[AsyncElasticsearch|None] = None
    xml_files_tmp_dir: Path = Path('backend/tmp/xml')
    products_xml_file: str = 'products.xml'
    products_info_xml_file: str = 'products_info.xml'
    safe_xml_parser = etree.XMLParser(
        ns_clean=True,
        no_network=True,
        resolve_entities=False,
        encoding='utf-8'
    )

    @classmethod
    async def connect(cls: type[AsyncElasticService]) -> None:
        cls.client = AsyncElasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'], #http://elasticsearch:9200
        api_key=('api-key-id', 'api-key-secret')
    )
    
    @classmethod
    async def close_connection(cls: type[AsyncElasticService]) -> None:
        await cls.client.close()

    @classmethod
    async def ingest_products(cls: type[AsyncElasticService]):
        tree: etree.ElementTree = etree.parse(
            cls.products_xml_file,
            parser=cls.safe_xml_parser
        )
        root: etree._Element = tree.getroot()
        for product in root.iterfind('.//product'):
            yield product

    @classmethod
    async def ingest_products_info(cls: type[AsyncElasticService]):
        tree: etree.ElementTree = etree.parse(
            cls.products_xml_file,
            parser=cls.safe_xml_parser
        )
        root: etree._Element = tree.getroot()
        for product_info in root.iterfind('.//productInfo'):
            yield product_info


    @classmethod
    async def ingest(cls: type[AsyncElasticService], index: str):
        if not (await cls.client.indices.exists(index=index)):
            await cls.client.indices.create(index=index)

        async for _ in async_streaming_bulk(
            client=cls.client, index=index, actions=cls.ingest_products()
        ):
            pass
        
        async for _ in async_streaming_bulk(
            client=cls.client, index=index, actions=cls.ingest_products_info()
        ):
            pass

        return {"status": "ok"}

    @classmethod
    async def search(cls, query: str):

        # result = cls.client.search(
        #     index="my_index",
        #     query={
        #         "match": {
        #             "foo": "foo"
        #         }
        #     },
        # )
        # print(f'{result=}')

        # result = await cls.client.search(
        #     index='games', body={'query': {'multi_match': {'query': query, 'fields': [ 'title', '*_name' ]}}}
        # )
        # print(f'{result=}')


        # Requires sanitation (of "query" string from FE)
        result = await cls.client.search(
            index='games', body={'query': {'simple_query_string': {'query': query, 'fields': [ 'title', '*_name' ]}}}
        )
        print(f'{result=}')



        print('query ', query)
        customers = {'customers': ['John', 'Jane']}
        response: dict[str, str] = {
            'search_results': [
                k for k in customers['customers']
                if k.lower().startswith(query.lower())
            ]
        }
        return response