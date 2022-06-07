import json
import requests
from catalog_config import CatalogConfig
from infrastructure.repositories.postgres import *
from infrastructure.repositories.elastic import *

config = CatalogConfig()
config.read()

elastic = Elastic(
    config["ELASTIC"]["URI"],
    config["ELASTIC"]["APM_INDEX"]
)

data = get_max_date_elastic(config['POSTGRES']['URI']).isoformat()

resp = elastic.search(
        dt_from = data,
        query='service.name: "catalog_api_search_global"'
    )

response = metrics_to_postgre(resp)

insert_postgre_elastic(config['POSTGRES']['URI'], response, data)


_header = {
    'Content-Type': 'application/json'
}

_url_metrics = config['ELASTIC']['URI']
_url_postgres = config['POSTGRES']['URI']

date_ref = get_max_date(_url_postgres)

query = {
    'query':
        """
            SELECT
                "body.additional_data.seller_id",
                "body.additional_data.search",
                "body.timestamp",
                "body.additional_data.results_qty",
                "body.additional_data.user_id",
                "body.additional_data.scanner",
                case when "body.additional_data.results_qty" = 0 then 'EAN MISSING' else 'EAN FOUND' end as ean_missing_found_ds,
                "body.tags"
            FROM
                "logger-*"
            WHERE
                "body.additional_data.seller_id" is not null
                and "body.additional_data.operation" ='FIND_SKU_BY_EAN'
                AND "body.timestamp" > '{date_ref}'
        """.format(date_ref = date_ref.strftime('%Y-%m-%dT%H:%M:%S.%f'))
}

def get_data(query):
    r = requests.get(_url_metrics + '_sql?format=json', headers = _header, data = json.dumps(query))
    return r.json()

data = get_data(query)

columns = []
for d in data.get('columns', []):
    columns.append(d.get('name').replace('.', '_'))

values = []

for d in data.get('rows', []):
    if d[1]:
        d[1] = d[1].strip()

    values.append(tuple(d))

insert_many(_url_postgres, 'metrics_ean_search', values, columns)

exec_procedure(_url_postgres)
