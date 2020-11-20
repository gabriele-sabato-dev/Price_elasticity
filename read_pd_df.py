import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def read_pd_df(_path):
    return pd.read_pickle(_path)

if __name__ == '__main__':
    print('Hello read_pd_df')
    df = read_pd_df('/Users/gabriele.sabato/PycharmProjects/raw_data/DataFrames/price_elasticity_YTD.pickle')
    df[df[item_code==]]
    print(df.head())