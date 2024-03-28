from __future__ import annotations
from typing import ClassVar
import time
from openai import OpenAI

from elasticsearch import AsyncElasticsearch

open_ai_client = OpenAI(api_key="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")


def get_embedding(text: str, model: str = "text-embedding-ada-002"):
    text = text.replace("\n", " ")
    return open_ai_client.embeddings.create(input=[text], model=model).data[0].embedding


class AsyncElasticService:

    client: ClassVar[AsyncElasticsearch|None] = None

    @classmethod
    async def connect(cls: type[AsyncElasticService]) -> None:
        cls.client = AsyncElasticsearch(
            cloud_id="Vector_Search_Cluster:ZXVyb3BlLXdlc3QyLmdjcC5lbGFzdGljLWNsb3VkLmNvbTo0NDMkOTRhMjRlMWZhMGY0NDQzNGI3N2Q1MDlkMzU2OTNhNDgkNGM2YmI2MGZmOGI3NGVmYmI1Y2VjZGUxMDA4ZmFiNjA=",
            basic_auth=("elastic", "SCHvZVVakupfKWJVFlPdxCae")
        )

    @classmethod
    async def close_connection(cls: type[AsyncElasticService]) -> None:
        await cls.client.close()

    @classmethod
    async def search(cls, query: str) -> dict[str, list[dict[str, str]]]:
        start_time = time.time()

        filter_query = {
            "knn": {
                "field": "search_vector",
                "query_vector": get_embedding(query),
                "k": 5,
                "num_candidates": 50,
                "boost": 0.1
            },
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "description": query
                            }
                        },
                        {
                            "match": {
                                "search_patterns": query
                            }
                        }
                    ]
                }
            }
        }

        print("--- Embedings %s seconds ---" % (time.time() - start_time))

        start_time = time.time()
        result = await AsyncElasticService.client.search(
            index='product_search',
            body=filter_query,
            source=['product_key', 'description'],
            min_score=0.50
        )

        print("--- query took %s seconds ---" % (time.time() - start_time))
        return {
            'search_results': [
                {
                    'product_key': item['_source']['product_key'],
                    'description': item['_source']['description']
                }
                for item in result['hits']['hits']
            ]
        }
