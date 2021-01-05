#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pickle
import pandas as pd
import numpy as np

spark = SparkSession.builder.master('local').config('spark.driver.memory', '8g').appName('Ready_for_analysis').getOrCreate()

df = spark.read.parquet(
    '/Users/gabriele.sabato/PycharmProjects/raw_data/price_elasticity_model_data/part-*',
    header=True)

df.createOrReplaceTempView('model_data_table')

#df.show()

df_d_item = spark.read.parquet('/Users/gabriele.sabato/PycharmProjects/raw_data/redshift_tables/d_item/*')

df_d_item.createOrReplaceTempView('d_item_tmp')

df_item_visits = spark.read.parquet(
    '/Users/gabriele.sabato/PycharmProjects/raw_data/redshift_tables/item_visits/*2020*/*.parquet',header=True)

df_f_orders = spark.read.parquet(
    '/Users/gabriele.sabato/PycharmProjects/raw_data/redshift_tables/f_orders/*.parquet',header=True)

df_f_orders.createOrReplaceTempView('f_orders')


df_d_calendar = spark.read.parquet(
    '/Users/gabriele.sabato/PycharmProjects/raw_data/redshift_tables/d_calendar/*.parquet',header=True)

df_d_calendar.createOrReplaceTempView('d_calendar')

df_d_order_flags = spark.read.parquet(
    '/Users/gabriele.sabato/PycharmProjects/raw_data/redshift_tables/d_order_flags/*.parquet',header=True)

df_d_order_flags.createOrReplaceTempView('d_order_flags')

df_list_top_seller = spark.sql("""
SELECT di.item_code,
       sum(fo.amount + amount_discount) AS order_amount

  FROM f_orders as fo
       INNER JOIN d_calendar c ON c.date_skey = fo.order_date_skey
       INNER JOIN d_item_tmp di ON di.item_skey = fo.item_skey
INNER JOIN d_order_flags dof on dof.order_flags_skey = fo.order_flags_skey

 WHERE c.date >= current_date - 1*interval ' 1 year'
and dof.is_cancelled_immediately = 'is not cancelled immediately'
and dof.is_cancelled_after_payment = 'is not cancelled after payment'
and dof.is_cancelled_before_payment = 'is not cancelled before payment'
group by di.item_code
ORDER BY order_amount DESC""")

df_list_top_seller.createOrReplaceTempView('top_seller_list')

#df_list_top_seller.show()

df_list_top_seller_10K= spark.sql("""
SELECT item_code
from top_seller_list
""")

top_seller_list_string_10k = df_list_top_seller_10K.rdd.map(lambda x: x.item_code).collect()


List_of_n_items = np.arange(2000,10001,1000)
print(List_of_n_items)

map_of_items = {}

#DEFINE Number of items and get list
def get_list_of_items(N_items = 10,top_seller_list = top_seller_list_string_10k):
    selected_items = top_seller_list[:N_items]
    selected_items[0]= """'""" + selected_items[0]
    selected_items[-1]= selected_items[-1] + """'"""
    separator =  """' , '"""
    f_string = separator.join(selected_items)
    #print(f_string)
    return f_string

for n_item in List_of_n_items:
    map_of_items[n_item]=get_list_of_items(n_item)

#print(map_of_items[10])


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
                                  where item_code in ({lista})
                                  GROUP BY item_code, meta_date""".format(lista=final_string))
    #df_item_visits_df.show()


# In[29]:

    df2 = spark.sql("""SELECT * from model_data_table where update_date >= '2020-01-01' and item_code in ({lista}) ORDER BY update_date""".format(lista=final_string))
    #df2.show()


# In[30]:

    df4 = spark.sql ("""SELECT item_code, item_main_category, item_sub_category_1, item_sub_category_2,item_parent_item_code from d_item_tmp where item_code in ({lista})""".format(lista=final_string))
    #df4.show()

# In[31]:


    df4.createOrReplaceTempView('d_item_filtered')
    df2.createOrReplaceTempView('model_data_table_YTD')
    df_item_visits_df.createOrReplaceTempView('item_views_YTD')


# In[32]:

    #df_item_visits_df.describe(['item_code']).show()


# In[33]:

    sql_price_visit_join= """ SELECT mdt_YTD.*, it_YTD.all_unique_views FROM model_data_table_YTD as mdt_YTD LEFT JOIN item_views_YTD as it_YTD
    on mdt_YTD.item_code = it_YTD.item_code and mdt_YTD.update_date = it_YTD.meta_date """

    print(sql_price_visit_join)


# In[34]:
    df_price_visit_join = spark.sql(sql_price_visit_join)
    df_price_visit_join.createOrReplaceTempView('mdt_YTD_it')
    #df_price_visit_join.describe(['item_code']).show()


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
        FROM mdt_YTD_it
  ),
       tmp_tbl2 AS (
           SELECT update_date,
                  item_code,
                  sales,
                  all_unique_views,
                  item_price,
                  delivery_weeks,
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
       CASE WHEN sum(all_unique_views) <> 0 THEN sum(sales)/sum(all_unique_views) ELSE NULL END as conv_rate,
       CAST (datediff(MAX(update_date), MIN(update_date) ) + 1 as int) AS bin
  FROM tmp_tbl2
 GROUP BY ranks, delivery_weeks, item_price, item_code
 ORDER BY item_code, min_date ASC;
"""


# In[36]:


    #print(sql_top_item_query_group_by)


# In[37]:


    sql_top_item_df = spark.sql(sql_top_item_query_group_by)


# In[38]:


    #sql_top_item_df.show(5000,False)


# In[101]:


#CREATE A TABLE VIEW FOR TopX sellers
    sql_top_item_df.createOrReplaceTempView('top_sellers')


# In[102]:


    #JOIN THE TWO TABLEs
    sql_join_query = " SELECT top.*, di.item_parent_item_code, di.item_main_category, di.item_sub_category_1, di.item_sub_category_2"                  "        from top_sellers as top "                  "INNER JOIN d_item_filtered as di on di.item_code = top.item_code "                 "ORDER BY top.min_date ASC"


# In[103]:


    print(sql_join_query)


# In[104]:


    final_top_seller_df = spark.sql(sql_join_query)


# In[105]:


    final_top_seller_df.show(3000,False)


# In[106]:


    final_top_seller_df.describe(['bin']).show()


# In[107]:


    pd_df_top_sellers = final_top_seller_df.toPandas()


# In[110]:


    pd_df_top_sellers['log_price'] = np.log(pd_df_top_sellers['item_price']+0.0001)


# In[111]:


    pd_df_top_sellers['log_delivery_weeks'] = np.log(pd_df_top_sellers['delivery_weeks']+0.0001)


# In[112]:


    pd_df_top_sellers['log_sales'] = np.log(pd_df_top_sellers['avg_sales']+0.0001)


# In[113]:


    pd_df_top_sellers['log_avg_unique_views'] = np.log(pd_df_top_sellers['avg_unique_views']+0.0001)


# In[114]:


    pd_df_top_sellers['log_conv_rate']= np.log(pd_df_top_sellers['conv_rate']+0.0001)


# In[115]:
    N_fin_it_str = str(N_final_items)
    final_name = '/Users/gabriele.sabato/PycharmProjects/raw_data/DataFrames/Top' + N_fin_it_str + '_seller_YTD_group_by_item_visit.pickle'
    print(final_name)
    pd_df_top_sellers.to_pickle(final_name)

