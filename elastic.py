from elasticsearch import Elasticsearch
import psycopg2 as pg
import psycopg2.extras
import datetime
import re


class Elastic:
    def __init__(self, uri, index):
        self.client = Elasticsearch(uri)
        self.index = index

    def search(self, dt_from, query):
        data = self.client.search(
            index=self.index,
            request_timeout=60,
            scroll="5m",
            body={
                "size": 10000,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "query_string": {
                                    "query": f"{query}"
                                }
                            },
                            {
                                "range": {
                                    "@timestamp": {
                                        "gt": f"{dt_from}"
                                    }
                                }
                            }
                        ]
                    }
                },
            },
        )
        logs = data.get("hits", {}).get("hits", [])
        next_scroll = data.get('_scroll_id', None)

        while True:
            scroll = self.client.scroll(scroll_id=next_scroll, scroll='10s')
            scroll_hits = scroll.get("hits", {}).get("hits", [])
            logs += scroll_hits
            next_scroll = scroll.get('_scroll_id', None)
            if len(scroll_hits) == 0:
                break
        return [log.get("_source", {}) for log in logs if log.get("_source", {}).get("http", None) is not None]

    def search_by_scroll(self, scroll_id):
        scroll = self.client.scroll(scroll_id=scroll_id, scroll='10s')
        scroll_hits = scroll.get("hits", {}).get("hits", [])
        next_scroll = scroll.get('_scroll_id', None)
        return scroll_hits, next_scroll


def get_last_date_elastic(uri, table, field, extra=''):
    try:
        with pg.connect(uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f'SELECT {field} FROM {table} WHERE {field} IS NOT NULL {extra} ORDER BY {field} DESC LIMIT 1')
                timestamp = cursor.fetchone()[0] if cursor.rowcount == 1 else datetime.datetime.fromtimestamp(1)
    finally:
        try:
            connection.close()
        except:
            pass

    return timestamp if timestamp != None else datetime.datetime.fromtimestamp(0)


def get_max_date_elastic(uri):
    query = """
        select max(timestamp_dt) as timestamp_dt from metrics_logs_search
        
    """
    try:
        with pg.connect(uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                timestamp = cursor.fetchone()[0] if cursor.rowcount == 1 else None
    finally:
        try:
            connection.close()
        except:
            pass

    return timestamp

def metrics_to_postgre(resp):

    response = []

    for r in resp:
        try:
            _id = (r['container']['id'])
        except:
            _id = None
        try:
            environment = (r['service']['environment'])
        except:
            environment = None
        try:
            x_consumer_username = (r['http']['request']['headers']['X-Credential-Username'])
        except:
            continue
        try:
            re.search(r"https://search\.catalog\.nodis\.com\.br/", r['url']['full'])
            url_full = (r['url']['full'])
        except:
            continue
        try:
            method = (r['http']['request']['method'])
        except:
            method = None
        try:
            socket = (r['http']['request']['socket']['remote_address'])
        except:
            socket = None
        try:
            transaction = (r['transaction']['duration']['us'])
        except:
            transaction = None
        try:
            status = (r['http']['response']['status_code'])
        except:
            status = None
        try:
            timestamp = (r['@timestamp'])
        except:
            timestamp = None

        info = {}
        info.update({'_id': _id,
                      'username': x_consumer_username,
                      'enviroment': environment,
                      'method': method,
                      'timestamp': timestamp,
                      'status': status,
                      'socket': socket,
                      'url_full': url_full,
                      'transaction_duration_us': transaction})
        response.append(info)
    return response


def insert_postgre_elastic(key_postgre, response, data):
    try:
        connection = pg.connect(key_postgre)
        curs = connection.cursor()
        postgres_insert_query = """ INSERT INTO metrics_logs_search (_id,
                                        username,
                                        enviroment,
                                        method,
                                        timestamp_dt,
                                        url_full,
                                        socket_remote_address,
                                        status_code,
                                        transaction_duration_us)
                                        VALUES (%(_id)s,
                                        %(username)s,
                                        %(enviroment)s,
                                        %(method)s,
                                        %(timestamp)s,
                                        %(url_full)s,
                                        %(socket)s,
                                        %(status)s,
                                        %(transaction_duration_us)s)"""
        pg.extras.execute_batch(curs, postgres_insert_query, response, page_size=len(response))
        connection.commit()
        count = curs.rowcount
        print(count, "Record inserted successfully into mobile table")
        curs.close()
        connection.close()
        print("PostgreSQL connection is closed")
        print(f'Dados inseridos na tabela, a partir de {data}')

    except (Exception, pg.Error) as error:
        print("Failed to insert record into mobile table", error)
