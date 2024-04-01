from __future__ import annotations
import re
from typing import ClassVar
from elasticsearch import AsyncElasticsearch
from openai import OpenAI

open_ai_client = OpenAI(api_key="")


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
        product_code_in_text_pattern = r'\b[A-Z]{2}-\d{3}-\d{2,3}\s?[A-Z]{0,2}\b'
        product_code_pattern = r'^[A-Z]{2}-\d{3}-\d{2,3}\s?[A-Z]{0,2}$'
        if re.match(product_code_pattern, query):
            filter_query = {
                "query": {
                    "match": {
                        "product_key": query
                    }
                }
            }
        else:
            filter_query = {
                "knn": {
                    "field": "search_vector",
                    "query_vector": get_embedding(query),
                    "k": 1,
                    "num_candidates": 100,
                    "boost": 0.1
                },
                "query": {
                    "bool": {
                        "should": [
                            {
                                "match": {
                                    "description": {
                                        "query": query,
                                        "boost": 7,
                                        "fuzziness": 2
                                    },
                                },
                            },
                            {
                                "term": {
                                    "description": {
                                        "value": query,
                                        "boost": 10
                                    },
                                }
                            },
                            {
                                "match": {
                                    "search_patterns": {
                                        "query": query,
                                        "boost": 3,
                                        "fuzziness": 2
                                    },
                                }
                            }
                        ]
                    }
                }
            }

            if re.match(product_code_in_text_pattern, query):
                prod_number = re.findall(product_code_in_text_pattern, query)
                if prod_number:
                    filter_query['query']['bool']['must'] = [
                        {
                            "match": {
                                "product_key": {
                                    "query": prod_number[0].strip(),
                                    "boost": 15
                                }
                            }
                        }
                    ]

            pattern = r"[-+]?(?:\d*\.*\d+)"
            number = [num.replace(',', '.') for num in re.findall(pattern, query)]

            if number and not re.match(product_code_pattern, query):
                number = float(number[0])
                if not filter_query['query']['bool'].get('must'):
                    filter_query['query']['bool']['must'] = []
                filter_query['query']['bool']['must'].extend(
                    [
                        {
                            "bool": {
                                "should": [
                                    {
                                        "terms": {
                                            "dimensions": [
                                                number
                                            ]
                                        }
                                    },
                                    {
                                        "range": {
                                            "dimensions": {
                                                "gte": number - 0.5,
                                                "lte": number + 0.5,
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                )

        result = await AsyncElasticService.client.search(
            index='product_search_with_dimensions',
            body=filter_query,
            source=['product_key', 'description'],
            min_score=0.50
        )

        return {
            'search_results': [
                {
                    'product_key': item['_source']['product_key'],
                    'description': item['_source']['description']
                }
                for item in result['hits']['hits']
            ]
        }
