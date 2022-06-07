import datetime
import psycopg2
import psycopg2.extras

from catalog_utility_kit.chunks import chunks


def get_last_date(uri, table, field, extra=''):
    try:
        with psycopg2.connect(uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f'SELECT {field} FROM {table} WHERE {field} IS NOT NULL {extra} ORDER BY {field} DESC LIMIT 1')
                timestamp = cursor.fetchone()[0] if cursor.rowcount == 1 else datetime.datetime.fromtimestamp(1)
    finally:
        try:
            connection.close()
        except:
            pass

    return timestamp if timestamp != None else datetime.datetime.fromtimestamp(0)

def get_max_date(uri):
    query = """
        select max(created_at_dt) as created_at_dt from
        (select max(created_at_dt) as created_at_dt from graylog_ean_missing
        union
        select max(created_at_dt) from graylog_ean_found) t
    """
    try:
        with psycopg2.connect(uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                timestamp = cursor.fetchone()[0] if cursor.rowcount == 1 else None
    finally:
        try:
            connection.close()
        except:
            pass
    
    return timestamp

def exec_procedure(uri):
    try:
        with psycopg2.connect(uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute('call stp_prc_graylog_ean_missing_found()')
    except Exception as e:
        print(e)
    finally:
        try:
            connection.close()
        except:
            pass

def execute(uri, query):
    print(f"{datetime.datetime.now()} Executing {query}")

    try:
        with psycopg2.connect(uri) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
    finally:
        try:
            connection.close()
        except:
            pass

def upsert_many(uri, table, values, columns, primary_key):
    if len(values) <= 0:
        return

    print(f"{datetime.datetime.now()} Sending {len(values)}")

    fields = ', '.join(["%s" for c in columns])
    all_columns = ', '.join(columns)
    excluded_columns = 'EXCLUDED.' + ', EXCLUDED.'.join(columns)

    sql = f"""
        INSERT INTO {table}({all_columns})
            VALUES({fields})
        ON CONFLICT({primary_key})
            DO UPDATE SET
                ({all_columns}) = ({excluded_columns})
    """

    try:
        with psycopg2.connect(uri) as connection:
            with connection.cursor() as cursor:
                for chunk in chunks(values, 500):
                    psycopg2.extras.execute_batch(cursor, sql, chunk, page_size=len(chunk))
                    connection.commit()
    finally:
        try:
            connection.close()
        except:
            pass

def insert_many(uri, table, values, columns):
    if len(values) <= 0:
        return
    
    print(f"{datetime.datetime.now()} Sending {len(values)}")

    fields = ', '.join(["%s" for c in columns])
    all_columns = ', '.join(columns)

    sql = f"""
        INSERT INTO {table}({all_columns})
            VALUES({fields})
    """

    try:
        with psycopg2.connect(uri) as connection:
            with connection.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, sql, values, page_size=len(values))
                connection.commit()
                print('Commited')
    except Exception as e:
        print(e)
    finally:
        try:
            connection.close()
        except:
            pass
