from __future__ import annotations
from typing import ClassVar

from elasticsearch import AsyncElasticsearch
from sentence_transformers import SentenceTransformer


class AsyncElasticService:

    client: ClassVar[AsyncElasticsearch|None] = None

    @classmethod
    async def connect(cls: type[AsyncElasticService]) -> None:
        cls.client = AsyncElasticsearch(
            cloud_id="somecloud",
            basic_auth=("some", "any")
        )

    @classmethod
    async def close_connection(cls: type[AsyncElasticService]) -> None:
        await cls.client.close()

    @classmethod
    async def search(cls, query: str) -> dict[str, list[dict[str, str]]]:
        model: SentenceTransformer = SentenceTransformer("all-mpnet-base-v2")
        query_input = model.encode(query)
        es_query_dict = {
            'field': 'search_vector',
            'query_vector': query_input,
            # 'k': 2,
            # 'num_candidates': 500
        }

        result = await AsyncElasticService.client.search(
            index='vect_search_products',
            knn=es_query_dict,
            source=['product_key', 'description'],
            min_score=0.50
        )

        return {
            'search_results':[
                {
                    'product_key': item['_source']['product_key'],
                    'description': item['_source']['description']
                }
                for item in result['hits']['hits']
            ]
        }
