#!/usr/bin/python3
# -*- coding:utf-8 -*-

import math
import sqlite3
import traceback
import mo_sql_parsing

from tqdm.autonotebook import tqdm
from elasticsearch import Elasticsearch, helpers

from setting import ES_URL, SQLITE_PATH, SQLITE_MAX_SELECT


def handle_es(data_list, primary_key, index_name):
    for i in data_list:
        yield {
            "_op_type": 'index',
            "_id": i[primary_key],
            "_source": i,
            "_type": 'doc',
            "_index": index_name,
        }


def fetch_dict_result(cur):
    row_headers = [x[0] for x in cur.description]
    rv = cur.fetchall()
    json_data = []
    for result in rv:
        json_data.append(dict(zip(row_headers, result)))
    return json_data


if __name__ == "__main__":

    es = Elasticsearch(ES_URL)
    print(es.info())

    conn = sqlite3.Connection(SQLITE_PATH)

    cur = conn.cursor()

    try:
        cur.execute("select name,sql from sqlite_master where type='table'")
        result = cur.fetchall()

        for table in result:
            data = mo_sql_parsing.parse(table[1])
            if isinstance(data["create table"]["constraint"], list):
                primary_id = data["create table"]["constraint"][0]["primary_key"][
                    "columns"
                ]
            else:
                primary_id = data["create table"]["constraint"]["primary_key"][
                    "columns"
                ]
            table_name = data["create table"]["name"].lower()

            if table_name not in es.indices.get_alias().keys():
                es.indices.create(index=table_name)
            else:
                continue
            columns_list = []

            sql_count = "select count(1) from " + table_name
            cur.execute(sql_count)
            count = cur.fetchall()

            for d in data["create table"]["columns"]:
                columns_list.append(d["name"])
                sql = (
                    "select "
                    + ",".join(i for i in columns_list)
                    + " from {} limit {} offset {}"
                )

            for coun in tqdm(
                range(math.ceil(count[0][0] / SQLITE_MAX_SELECT)),
                desc='Start Sqlite To Es ,Table is ' + table_name,
            ):
                cur.execute(
                    sql.format(table_name, SQLITE_MAX_SELECT, coun * SQLITE_MAX_SELECT)
                )
                data_list = fetch_dict_result(cur)

                helpers.bulk(es, handle_es(data_list, primary_id, table_name))
                for ok, response in helpers.streaming_bulk(es, data_list):
                    if not ok:
                        print(response)
    except Exception:
        print(str(traceback.print_exc()))
    finally:
        cur.close()
        conn.close()
        es.close()
