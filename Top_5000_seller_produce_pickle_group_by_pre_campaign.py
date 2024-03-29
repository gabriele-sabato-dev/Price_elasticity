#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pickle
import pandas as pd
import numpy as np
import sys

sys.('echo $JAVA_HOME')

import psycopg2
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').config('spark.driver.memory', '16g').appName(
    'Voucher_model').getOrCreate()


def connect_redshift_db():
    cnx = psycopg2.connect(dbname='dwh',
                           host='redshift-prod.data.service.home24.net',
                           port='5439',
                           user='gabriele_sabato',
                           password='zQZ8x6ZhRoTf7GS')
    return cnx


cnx_redshift = connect_redshift_db()
cursor_redshift = cnx_redshift.cursor()


def query_to_dataframe(_cursor, _query):
    _cursor.execute(_query)
    colnames = [desc[0] for desc in _cursor.description]
    print(colnames)
    return pd.DataFrame(_cursor.fetchall(), columns=colnames)


# CREATE MARKET SESSION SPARK DF
sql_query_market_session = """
SELECT report_date, sum(sessions) as session
from public.marketing_daily_dashboard_base
WHERE report_date >= '2020-01-01' and report_date <= '2021-04-28'
group by 1
order by 1
"""

df_marketing_session = query_to_dataframe(cursor_redshift, sql_query_market_session)

df_spark_market_session = spark.createDataFrame(df_marketing_session)

# CREATE MARKET COST SPARK DF
sql_query_market_cost = """
SELECT report_date, sum(cost) as marketing_cost_spend
FROM public.marketing_cost_consolidated
WHERE report_date >= '2020-01-01' AND report_date <= '2021-04-28'
AND marketing_channel != 'Sales personnel'
group by 1
order by 1
"""

df_marketing_cost = query_to_dataframe(cursor_redshift, sql_query_market_cost)

df_spark_market_cost = spark.createDataFrame(df_marketing_cost)

#RETRIEVE VOUCHER VALUE FOR ALL ITEMS
sql_voucher_value = """
WITH vv as (SELECT product_code,
       voucher_value,
       campaign_date,
       cp.webshop_code,
       ROW_NUMBER() over (PARTITION BY product_code, campaign_date ORDER BY updated_at DESC) as rn
  FROM public.campaign_details as cd
       INNER JOIN public.campaign_product as cp on cp.campaign_id = cd.id and cp.key = cd.key and cp.webshop_code = 'DE'
 WHERE campaign_date >= '2020-01-01'
   and campaign_date <= '2021-04-28')
   SELECT product_code, voucher_value, campaign_date, webshop_code
   from vv
where rn =1
and voucher_value is not null
ORDER BY product_code, campaign_date ASC"""

df_voucher = query_to_dataframe(cursor_redshift, sql_voucher_value)

df = spark.read.parquet(
    '/Users/gabriele.sabato/PycharmProjects/raw_data/price_elasticity_model_data/part-*',
    header=True)

df.createOrReplaceTempView('model_data_table')

df_s_voucher = spark.createDataFrame(df_voucher)

df_s_voucher.createOrReplaceTempView('model_voucher')

df_d_item = spark.read.parquet('/Users/gabriele.sabato/PycharmProjects/raw_data/redshift_tables/d_item/*')

df_d_item.createOrReplaceTempView('d_item_pre_voucher')


sql_apply_voucher = spark.sql("""
SELECT dipv.*,
       mv.voucher_value,
       dipv.item_price * (1-mv.voucher_value) as discounted_price
FROM d_item_pre_voucher as dipv
INNER JOIN model_voucher as mv on mv.product_code = dipv.item_code and mv.campaign_date = dipv.meta_date
""")

df_item_visits = spark.read.parquet(
    '/Users/gabriele.sabato/PycharmProjects/raw_data/redshift_tables/item_visits/*=202*/*.parquet', header=True)

df_f_orders = spark.read.parquet(
    '/Users/gabriele.sabato/PycharmProjects/raw_data/redshift_tables/f_orders/*.parquet', header=True)

df_f_orders.createOrReplaceTempView('f_orders')

df_d_calendar = spark.read.parquet(
    '/Users/gabriele.sabato/PycharmProjects/raw_data/redshift_tables/d_calendar/*.parquet', header=True)

df_d_calendar.createOrReplaceTempView('d_calendar')

df_d_order_flags = spark.read.parquet(
    '/Users/gabriele.sabato/PycharmProjects/raw_data/redshift_tables/d_order_flags/*.parquet', header=True)

df_d_order_flags.createOrReplaceTempView('d_order_flags')


df_list_top_seller = spark.sql("""
SELECT di.item_code,
       sum(fo.amount + amount_discount) AS order_amount
  FROM f_orders as fo
       INNER JOIN d_calendar c ON c.date_skey = fo.order_date_skey
       INNER JOIN d_item_tmp di ON di.item_skey = fo.item_skey
INNER JOIN d_order_flags dof on dof.order_flags_skey = fo.order_flags_skey

 WHERE c.date >= '2021-04-28' - 1*interval ' 1 year'
and dof.is_cancelled_immediately = 'is not cancelled immediately'
and dof.is_cancelled_after_payment = 'is not cancelled after payment'
and dof.is_cancelled_before_payment = 'is not cancelled before payment'
group by di.item_code
ORDER BY order_amount DESC""")

df_list_top_seller.createOrReplaceTempView('top_seller_list')

# df_list_top_seller.show()

df_list_top_seller_10K = spark.sql("""
SELECT item_code
from top_seller_list
LIMIT 10000""")

top_seller_list_string_10k = df_list_top_seller_10K.rdd.map(lambda x: x.item_code).collect()

#List_of_n_items = np.arange(2000, 10001, 1000)
List_of_n_items = [10000] #how many items
#print(List_of_n_items)

map_of_items = {}


# DEFINE Number of items and get list
def get_list_of_items(N_items=10, top_seller_list=top_seller_list_string_10k):
    selected_items = top_seller_list[:N_items]
    selected_items[0] = """'""" + selected_items[0]
    selected_items[-1] = selected_items[-1] + """'"""
    separator = """' , '"""
    f_string = separator.join(selected_items)
    # print(f_string)
    return f_string


for n_item in List_of_n_items:
    map_of_items[n_item] = get_list_of_items(n_item)

# print(map_of_items[10])


# In[27]:


for key in map_of_items.keys():
    N_final_items = key
    final_string = map_of_items[key]

    # In[28]:

    df_item_visits.createOrReplaceTempView('item_visits')

    df_item_visits_df = spark.sql("""SELECT item_code,
                                         meta_date,
                                        SUM(unique_views) as all_unique_views
                                    from item_visits
                                  where item_code in ({lista}) and meta_date <= '2021-04-28'
                                  GROUP BY item_code, meta_date""".format(lista=final_string))
    # df_item_visits_df.show()

    # In[29]:

    df2 = spark.sql(
        """SELECT * 
            from model_data_table 
            where update_date >= '2020-01-01'
            and  update_date <= '2021-04-28' --pre campaign data
            and item_code in ({lista}) 
            ORDER BY update_date""".format(
            lista=final_string))
    # df2.show()

    # In[30]:

    df4 = spark.sql(
        """SELECT item_code, 
                  item_main_category,
                  item_sub_category_1, 
                  item_sub_category_2,
                  item_parent_item_code 
            from d_item_tmp 
            where item_code in ({lista})""".format(
            lista=final_string))
    # df4.show()

    # In[31]:

    df4.createOrReplaceTempView('d_item_filtered')
    df2.createOrReplaceTempView('model_data_table_YTD')
    df_item_visits_df.createOrReplaceTempView('item_views_YTD')
    df_spark_market_session.createOrReplaceTempView('market_session')
    df_spark_market_cost.createOrReplaceTempView('market_cost')

    # In[32]:

    # df_item_visits_df.describe(['item_code']).show()

    # In[33]:

    sql_price_visit_join = """ 
    SELECT mdt_ytd.*, it_ytd.all_unique_views, ms.session, mc.marketing_cost_spend
    FROM model_data_table_ytd as mdt_ytd 
    LEFT JOIN item_views_ytd as it_ytd
    on mdt_ytd.item_code = it_ytd.item_code and mdt_ytd.update_date = it_ytd.meta_date
    LEFT JOIN market_session as ms
    on mdt_ytd.update_date = ms.report_date
    LEFT JOIN market_cost as mc
    on mdt_ytd.update_date = mc.report_date
    """

    print(sql_price_visit_join)

    # In[34]:
    df_price_visit_join = spark.sql(sql_price_visit_join)
    df_price_visit_join.createOrReplaceTempView('mdt_YTD_it')
    # df_price_visit_join.describe(['item_code']).show()

    # In[35]:

    sql_top_item_query_group_by = """
    WITH tmp_tbl AS (
      SELECT *,
             CASE
                 WHEN lag(delivery_weeks, 1) OVER (PARTITION BY item_code ORDER BY update_date ASC) =
                      delivery_weeks
                     AND
                      lag(item_price, 1) OVER (PARTITION BY item_code ORDER BY update_date ASC) =
                      item_price
                     THEN NULL
                 ELSE RANK() OVER (PARTITION BY item_code ORDER BY update_date)
                 END AS ranking_col --filled with row number or delivery week if the previous one is part of the same group,
        FROM mdt_ytd_it
  ),
       tmp_tbl2 AS (
           SELECT update_date,
                  item_code,
                  sales,
                  all_unique_views,
                  item_price,
                  delivery_weeks,
                  session,
                  marketing_cost_spend,
                  CASE
                      WHEN ranking_col IS NULL
                          THEN last(ranking_col, True) OVER (PARTITION BY item_code ORDER BY update_date ROWS BETWEEN UNBOUNDED PRECEDING and 1 PRECEDING)
                      ELSE ranking_col
                      END AS ranks
             FROM tmp_tbl
       )
SELECT CAST(MIN(update_date) as date)                     AS min_date,
       CAST (MAX(update_date) as date)                    AS max_date,
       item_code,
       item_price,
       delivery_weeks,
       avg(sales) as avg_sales,
       avg(all_unique_views) as avg_unique_views,
       avg(session) as avg_sessions,
       avg(marketing_cost_spend) as avg_marketing_cost_spend,
       CASE WHEN sum(all_unique_views) <> 0 THEN sum(sales)/sum(all_unique_views) ELSE NULL END as conv_rate,
       CAST (datediff(MAX(update_date), MIN(update_date) ) + 1 as int) AS bin
  FROM tmp_tbl2
 GROUP BY ranks, delivery_weeks, item_price, item_code
 ORDER BY item_code, min_date ASC;
"""

    # In[36]:

    # print(sql_top_item_query_group_by)

    # In[37]:

    sql_top_item_df = spark.sql(sql_top_item_query_group_by)

    # In[38]:

    # sql_top_item_df.show(5000,False)

    # In[101]:

    # CREATE A TABLE VIEW FOR TopX sellers
    sql_top_item_df.createOrReplaceTempView('top_sellers')

    # In[102]:

    # JOIN THE TWO TABLEs
    sql_join_query = """
    SELECT top.*, di.item_parent_item_code, di.item_main_category, di.item_sub_category_1, di.item_sub_category_2                          
    from top_sellers as top                   
    INNER JOIN d_item_filtered as di on di.item_code = top.item_code 
    ORDER BY top.min_date ASC"""

    # In[103]:

    print(sql_join_query)

    # In[104]:

    final_top_seller_df = spark.sql(sql_join_query)

    # In[105]:

    final_top_seller_df.show(3000, False)

    # In[106]:

    final_top_seller_df.describe(['bin']).show()

    # In[107]:

    pd_df_top_sellers = final_top_seller_df.toPandas()

    # In[110]:

    pd_df_top_sellers['log_price'] = np.log(pd_df_top_sellers['item_price'] + 0.0001)

    # In[111]:

    pd_df_top_sellers['log_delivery_weeks'] = np.log(pd_df_top_sellers['delivery_weeks'] + 0.0001)

    # In[112]:

    pd_df_top_sellers['log_sales'] = np.log(pd_df_top_sellers['avg_sales'] + 0.0001)

    # In[113]:

    pd_df_top_sellers['log_avg_unique_views'] = np.log(pd_df_top_sellers['avg_unique_views'] + 0.0001)

    # In[114]:

    pd_df_top_sellers['log_conv_rate'] = np.log(pd_df_top_sellers['conv_rate'] + 0.0001)

    pd_df_top_sellers['conv_rate'] = pd_df_top_sellers['conv_rate'] + 0.000001

    pd_df_top_sellers['log_avg_sessions'] = np.log(pd_df_top_sellers['avg_sessions'] + 0.0001)
    # In[115]:
    pd_df_top_sellers = pd_df_top_sellers.astype({'avg_marketing_cost_spend': float})
    # In[115]:
    pd_df_top_sellers.dtypes
    # In[115]:
    pd_df_top_sellers['log_avg_marketing_cost_spend'] = np.log(pd_df_top_sellers['avg_marketing_cost_spend'] + 0.0001)
    # In[116]:
    N_fin_it_str = str(N_final_items)
    final_name = '/Users/gabriele.sabato/PycharmProjects/raw_data/DataFrames/Top' + N_fin_it_str + '_seller_YTD_group_by_item_visit_20210615_pre_campaign_w_voucher.pickle'
    print(final_name)
    pd_df_top_sellers.dropna(inplace=True)
    pd_df_top_sellers.to_pickle(final_name)