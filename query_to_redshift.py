import psycopg2
import pandas as pd
import numpy as np

def connect_redshift_db():
    cnx = psycopg2.connect(dbname='dwh',
                           host='redshift-prod.data.service.home24.net',
                           port='5439',
                           password='zQZ8x6ZhRoTf7GS',
                           user='gabriele_sabato')
    return cnx

cnx_redshift = connect_redshift_db()
cursor_redshift = cnx_redshift.cursor()

def redshift_query_to_dataframe(_cursor, _query):
    _cursor.execute(_query)
    colnames = [desc[0] for desc in _cursor.description]
    return pd.DataFrame(_cursor.fetchall(), columns =[desc[0] for desc in _cursor.description])

def redshift_query_to_list(_cursor, _query):
    _cursor.execute(_query)
    return np.array(_cursor.fetchall())


def redshift_batch_query_from_list(_list, _query):
    for i in range(0,len(_list)):
        if i == 0:
            df = redshift_query_to_dataframe(cursor_redshift, _query.format((_list[i][0])))
        else:
            df = pd.concat([df,redshift_query_to_dataframe(cursor_redshift, _query.format((_list[i][0])))])
            if i%10 == 0:
                print(_list[i][0])
    return df

