import re
from elasticsearch import Elasticsearch
from openai import OpenAI
import threading
import string
import json


es = Elasticsearch(
    cloud_id="Vector_Search_Cluster:ZXVyb3BlLXdlc3QyLmdjcC5lbGFzdGljLWNsb3VkLmNvbTo0NDMkOTRhMjRlMWZhMGY0NDQzNGI3N2Q1MDlkMzU2OTNhNDgkNGM2YmI2MGZmOGI3NGVmYmI1Y2VjZGUxMDA4ZmFiNjA=",
    basic_auth=("elastic", "X2t97nvmIsTRHhxKtVI7o63X")
)
es.ping()


open_ai_client = OpenAI(api_key="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")

MAPPING = {
    "properties": {
      "description": {
        "type": "text"
      },
      "dimensions": {
        "type": "integer"
      },
      "main_description_part": {
        "type": "text"
      },
      "normalized_description": {
        "type": "text"
      },
      "normalized_search_patterns": {
        "type": "text"
      },
      "product_key": {
        "type": "text"
      },
      "search_patterns": {
        "type": "text"
      },
      "search_vector": {
          "type": "dense_vector",
          "dims": 1536,
          "index": True,
          "similarity": "l2_norm"
      }
    }
}


def get_embedding(text: str, model: str = "text-embedding-ada-002"):
    text = text.replace("\n", " ")
    return open_ai_client.embeddings.create(input=[text], model=model).data[0].embedding


def reindex_data(size, _from, tn):
    all_products = es.search(
        index="vect_search_products",
        body={
            "query": {
                "match_all": {}
            },
            "size": size,
            "from": _from,
            "source": ["description", "product_key", "search_patterns"]
        }
    )

    all_products = [prod['_source'] for prod in all_products["hits"]["hits"]]
    total = len(all_products)

    for index, product in enumerate(all_products):
        product['search_vector'] = get_embedding(product['search_patterns'])
        pattern = r"[-+]?(?:\d*\.*\d+)"

        numbers = {*[float(num.replace(',', '.')) for num in re.findall(pattern, product['search_patterns'])]}
        numbers.union({*[float(num.replace(',', '.')) for num in re.findall(pattern, product['description'])]})
        product['dimensions'] = list(numbers)
        es.index(index='product_search_with_dimensions', document=product)
        print(f'Done {index+1}/{total}. Thread num={tn}')


def add_combined_field(size, _from, tn):
    all_products = es.search(
        index="product_search_with_dimensions",
        body={
            "query": {
                "match_all": {}
            },
            "size": size,
            "from": _from,
        }
    )
    all_products = [prod for prod in all_products["hits"]["hits"]]
    total = len(all_products)

    for index, product in enumerate(all_products):
        prod_data = product['_source']
        translation_table = str.maketrans('', '', string.whitespace + string.punctuation)
        prod_data.update({
            "normalized_description": prod_data['description'].translate(translation_table),
            "normalized_search_patterns": prod_data['search_patterns'].translate(translation_table)
        })
        es.update(
            index="product_search_with_dimensions",
            id=product['_id'],
            doc=prod_data
        )
        print(f'Done {index+1}/{total}. Thread num={tn}')


def split_product_description(size, _from, tn):
    all_products = es.search(
        index="product_search_with_dimensions",
        body={
            "query": {
                "match_all": {}
            },
            "size": size,
            "from": _from,
        }
    )
    all_products = [prod for prod in all_products["hits"]["hits"]]
    total = len(all_products)
    for index, product in enumerate(all_products):
        prod_data = product['_source']
        description = prod_data.get('description', "")
        # with spaces around '-'
        hyphen_desc = description.split(' - ')
        slash_split_desc = description.split(' / ')
        splitted_desc = hyphen_desc or slash_split_desc
        if len(splitted_desc) >= 2:
            prod_data['main_description_part'] = splitted_desc[0]
            es.update(
                index="product_search_with_dimensions",
                id=product['_id'],
                doc=prod_data
            )
            print(f'Done {index + 1}/{total}. Thread num={tn}')
        else:
            print(f'Skip product update. Description is a single line. Product ID {prod_data["product_key"]}')


def read_and_index_json_file():
    f = open('all_data.json')
    data = json.load(f)
    total = len(data)
    for i, record in enumerate(data):
        record['search_vector'] = get_embedding(
            ', '.join(record['search_patterns']) + ' | ' + record['description'] + ' | ' + record['product_key']
        )
        print(record['search_vector'])
        es.index(index='product_search_with_dimensions', document=record)
        print(f'{i}/{total} done')


es.indices.create(index="product_search_with_dimensions", mappings=MAPPING)
read_and_index_json_file()

# size = 2000
# _from = 0
#
# thread_num = 1
# for _from in range(0, 14527, size):
#     t = threading.Thread(target=split_product_description, args=(size, _from, thread_num))
#     t.start()
#     thread_num += 1
