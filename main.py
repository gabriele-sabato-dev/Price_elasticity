# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import query_to_redshift as qtr
import pandas as pd
import numpy as np
import read_parquet as rpqt

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.

query = """
SELECT 
  FROM public.item_catalog_history
 LIMIT 10
"""

output_path = '/Users/gabriele.sabato/PycharmProjects/raw_data/price_elasticity_aggr/'
input_path = '/Users/gabriele.sabato/PycharmProjects/raw_data/price_elasticity/part*'
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    #df = qtr.redshift_query_to_dataframe(qtr.cursor_redshift, query)
    #print(df.head())
    df = rpqt.load_parquet_from_local_to_pd_df(input_path)
    print(df['item_code'].describe())
    rpqt.write_df_to_pq(df,output_path+'filt_DE_item_code_new_format.snappy.parquet')
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
