#!/usr/bin/env python
# coding: utf-8

# In[1]:


#IMPORTING SETUP PACKAGES
import statsmodels.formula.api as smf
import pandas as pd
import numpy as np

#get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt
plt.style.use('seaborn-whitegrid')

from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))

pd.set_option('display.max_rows', 5000)


# In[2]:
numbers_of_items  = np.arange(10,11,1)

for num in numbers_of_items:
    file_name = '/Users/gabriele.sabato/PycharmProjects/raw_data/DataFrames/Top'+ str(num)+'_seller_YTD_group_by_item_visit.pickle'
    df = pd.read_pickle(file_name)
    df.dropna(inplace=True)
    print(df.head(5000))
# In[4]:
    wls_model_with_fe = smf.wls("log_sales ~ log_price + log_delivery_weeks + log_avg_unique_views + C(item_code)", df, weights=df['bin']).fit()
    #print(wls_model_with_fe.summary())


# In[5]:
    #wls_model_with_fe_all_in = smf.wls("log_sales ~ log_price + log_delivery_weeks + log_avg_unique_views + log_conv_rate +  C(item_code)", df, weights=df['bin']).fit()
    #print(wls_model_with_fe_all_in.summary())

# In[19]:

#WLS CV rate vs the rest
    #wls_cr_with_fe = smf.wls("log_conv_rate ~ log_price + log_delivery_weeks + C(item_code)", df, weights=df['bin']).fit()
    #print(wls_cr_with_fe.summary())

# In[24]:
    wls_cr_with_fe = smf.wls("conv_rate ~ log_price + log_delivery_weeks + C(item_code)", df, weights=df['bin']).fit()
    print(wls_cr_with_fe.summary())
    beta_log_price, beta_std_err_log_price = wls_cr_with_fe.params[-2].round(4),wls_cr_with_fe.bse[-2].round(4)
    beta_log_del, beta_std_err_log_del = wls_cr_with_fe.params[-1].round(4),wls_cr_with_fe.bse[-1].round(4)
    Result_log_price = 'Log_price  = ' + str(beta_log_price) + ' ± ' + str(beta_std_err_log_price)
    Result_log_del = 'Log_del_week = ' + str(beta_log_del) + ' ± ' + str(beta_std_err_log_del)

    print(f'Top X sellers      = {num}\n'
          f'Log_price          = {Result_log_price} \n '
          f'Log_delivery_weeks = {Result_log_del}')


# In[12]:

    #wls_cr_del_w_with_fe = smf.wls("log_conv_rate ~ log_price + delivery_weeks + C(item_code)", df, weights=df['bin']).fit()
    #print(wls_cr_del_w_with_fe.summary())
