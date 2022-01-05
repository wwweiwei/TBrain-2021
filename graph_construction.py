import datatable as dt
import pandas as pd
import numpy as np
from datatable import (dt, f, by, ifelse, update, sort, count, min, max, mean, sum, rowsum)
from tqdm import tqdm 

path = './data/'

def read_data():
    data = dt.fread(path+'tbrain_cc_training_48tags_hash_final.csv')
    print('data.shape: ', data.shape)

    c_node = dt.unique(data['chid'])
    print(c_node)
    return data

def construct_c_node(data):
    '''
    c_node feature
        'masts', 'educd', 'trdtp', 'naty', 'poscd', 'cuorg', 'slam', 'gender_code', 'age', 'primary_card'
    '''
    # df_old = pd.read_csv('./data/c_node.csv')
    # data = data[:, mean(f.txn_amt), by('chid')] ## order by chid

    data = data[:, f['masts':'primary_card'], by('chid')] ## order by chid
    df_pd = data.to_pandas()

    df_c_node = pd.DataFrame(columns = ['chid', 'masts', 'educd', 'trdtp', 'naty', 'poscd', 'cuorg', 'slam', 'gender_code', 'age', 'primary_card'])
    # df_c_node = pd.DataFrame(columns = ['chid', 'masts', 'educd', 'trdtp', 'naty', 'poscd', 'cuorg', 'slam', 'gender_code', 'age', 'primary_card', 'txn_amt'])
    # df_mean_txt_amt = pd.DataFrame(columns = ['chid', 'txn_amt'])

    last_chid = 0
    for i in tqdm(range(data.shape[0])):
        if last_chid != df_pd.iloc[i, 0]: ## last_chid != current_chid
            df_c_node = df_c_node.append(df_pd.iloc[i])
            # df_mean_txt_amt = df_mean_txt_amt.append(df_pd.iloc[i])
            last_chid = df_pd.iloc[i, 0]

    # df_c_node = pd.merge(df_old, df_mean_txt_amt, on = 'chid')
    # df_c_node = df_c_node.drop(['Unnamed: 0'], axis=1)

    print(df_c_node)
    df_c_node.to_csv('./data/c_node.csv')

def construct_s_node(data):    
    '''
    s_node feature
        mean('txt_amt)
    '''
    data = data[:, mean(f.txn_amt), by('shop_tag')]
    df_pd = data.to_pandas()
    df_pd.loc[df_pd['shop_tag'] == 'other', 'shop_tag'] = '49'
    print(df_pd)
    df_pd.to_csv('./data/s_node.csv')

def construct_c2s_edge(data):
    '''
    | chid | shop_tag | dt |sum(txt_amt) |
    | ---- | -------- | -- | ----------- |
    | 1    | 32       | 1  | 10000       |
    | 5    | 17       | 1  | 10000       |
    | 46   | 3        | 1  | 10000       |
    | ...  | ...      | ...| ...         |
    '''
    data = data[:, mean(f.txn_amt), by('dt', 'chid', 'shop_tag')]
    df_pd = data.to_pandas()
    df_pd.loc[df_pd['shop_tag'] == 'other', 'shop_tag'] = '49'
    print(df_pd)
    df_pd.to_csv('./data/c2s_egde.csv')


if __name__ == '__main__':
    data = read_data()
    # construct_c_node(data)
    # construct_s_node(data)
    construct_c2s_edge(data)
