import pandas as pd
from ta_lib.core.api import load_yml,create_db_connection,create_context
from ta_lib.data_utils import digital_constants
from ta_lib.data_utils.read_raw import clickhouse
from ta_lib.data_utils.data_processing.digital import digital_overall

import ta_lib.eda.api
from ta_lib.core.api import setanalyse


config_path = '../conf/config.yml'
cfg_=create_context(config_path)

conn=create_db_connection(cfg_.offline,"clickhouse")


def get_cust_level_trans(conn,conf):
    store_master=pd.read_csv(conf['STORE_MASTER'].format(**conf))
    store_master['Company_Name']=store_master['Company_Name'].str.lower()
    dict_stores=dict(store_master.groupby(['Company_Name']).agg({'busn_lcat_id_nu':list}).reset_index().values)

    df_transactions_day_level=pd.read_sql(
        f"""select customer_id,
            company,
            pos_busn_dt,
            count(distinct(order_key,store_number)) as num_trans,
            sum(revenue) as total_sales, 
            sum(offer_transactions) as num_offer_transactions from 
            (select customer_id,
            date(event_ts) as pos_busn_dt,
            order_key,
            store_number,
            multiIf(store_number in {dict_stores['mcopco']},'mcopco','others') as company,
            gross_amount  as revenue, 
            case when(arraySum(items.offer_id)!=0 or arraySum(items.original_offer_id)!=0) then 1 else 0 end as offer_transactions  
            from promo_check_create where order_key is not null and store_number!=0 ) group by customer_id,company, pos_busn_dt""",conn)
    return df_transactions_day_level


import os.path as op


def get_day_level_dv_data(cfg, cutoff_date, lag=7, offer_duration=7):
    '''
    inputs for testing
    cutoff_date = '2021-10-04' #should always be a monday
    lag = 7
    offer_duration = 7
    '''

    # customer_reg_path = cfg_.digital['PRE_ENROLLMENT_MASTER'].format(**cfg_.digital,run_date='20211013')
    customer_reg_path = cfg_.digital['PRE_ENROLLMENT_MASTER'].format(**cfg_.digital, run_date='20220113')

    customer_reg_df = (
        pd.read_parquet(customer_reg_path, columns=['customer_id', 'enrollment_date', 'download_dt'])
            .query("customer_id.notna() and enrollment_date.notna()")
    )
    customer_reg_df['download_dt'] = pd.to_datetime(customer_reg_df['download_dt'])
    customer_reg_df['enrollment_date'] = pd.to_datetime(customer_reg_df['enrollment_date'])
    customer_reg_df = (
        customer_reg_df
            .groupby('customer_id')
            .agg({'download_dt': 'min', 'enrollment_date': 'min'})
            .reset_index()
            .rename(columns={'download_dt': 'first_download_date'})
    )

    offer_start = (pd.to_datetime(cutoff_date) + pd.Timedelta(days=lag))
    offer_end = pd.to_datetime(offer_start) + pd.Timedelta(days=offer_duration - 1)

    segment_path = cfg['GDW_FORMAT_OUTPUT_PATH'].format(**cfg, ref_date=cutoff_date.strftime('%Y%m%d'))
    transactions_daily_path=cfg['CUSTOMER_DAILY_INFO'].format(**cfg,run_date='20211008')
    profile_cust_path = cfg['SEGMENTATION_CUST_PROF_MAP_PATH'].format(**cfg, run_date='20210923', ref_date='20220110')

    # Segment data
    segment_df = pd.read_parquet(segment_path)
    df_segments = segment_df.groupby(['segments']).size().reset_index().rename(
        columns={0: 'segment_size'})  # This is only get segment sizes
    segment_df = segment_df[segment_df.appsflyer_id.isnull()]
    ## Lot of cust ids are null but profile_ids aren't. We will fill just customer_ids
    df_profile_cust = pd.read_parquet(op.join(profile_cust_path, 'part-0.parquet'))
    segment_df = segment_df.merge(df_profile_cust, on='profile_id', suffixes=('_x', '_y'))
    segment_df['customer_id'] = segment_df['customer_id_x']
    fil_ = segment_df.profile_id.notnull() & segment_df.customer_id_x.isnull()
    segment_df.loc[fil_, 'customer_id'] = segment_df.loc[fil_, 'customer_id_y']
    ## optional verification part. I got this as 0 so moving on.
    # print((segment_df.profile_id.notnull() & segment_df.customer_id.isnull()).sum())
    segment_df.drop(['appsflyer_id', 'customer_id_y', 'customer_id_x', 'profile_id'], axis=1, inplace=True)

    df_transactions_day_level = pd.read_parquet(transactions_daily_path)
    df_final = (df_transactions_day_level
                .query(f"pos_busn_dt>=@offer_start and pos_busn_dt<= @offer_end and company =='mcopco' ")
                .groupby(['customer_id', 'pos_busn_dt'])
                .agg(
        num_trans=('num_trans', sum),
    )
                .reset_index()
                .merge(segment_df, on=["customer_id"], how="left")
                .fillna({'segments': 'Pre-registration'})  # Downloaded but not enrolled and not downloaded
                .merge(customer_reg_df, on='customer_id', how='left')
                )
    '''
    df_final will not have the customers. It just has the customers who transacted. For the rest, we have to combine with appflyers data and segment data
    TODO:    
        To be fixed - the segments seems to wrong. 
        There are lot of customers who are called as pre-registration but are already enrolled
    '''

    fil_ = (df_final.segments == 'Pre-registration') & (df_final.first_download_date > cutoff_date)
    df_final.loc[fil_, 'segments'] = 'Not downloaded'

    df_final = df_final.groupby(['pos_busn_dt', 'segments']).sum().reset_index()

    return df_final, df_segments


#### write transactions data
df_transactions_day_level = get_day_level_dv_data(conn, cfg_.digital)
df_transactions_day_level.to_parquet(f'/media/tiger/dev/mcd_ta/discount_engine/data/processed/segmentation/customer_level_daily_info/part-0.parquet')


# df_daily
import os
import os.path as op
cutoff_dates = [(i+ pd.Timedelta(days=1)) for i in pd.date_range(start='2021-11-07', end='2022-01-16', freq='W-SUN')]
df_final = pd.DataFrame()
lag_ = 7
offer_duration_ = 7
for date in cutoff_dates:
    t1 = pd.to_datetime('now')
    c_date = pd.to_datetime(date)
    try:
        df_temp, df_segments = get_day_level_dv_data(cfg_.digital, cutoff_date=c_date, lag=lag_, offer_duration=offer_duration_)
        dir_ = f'/media/tiger/dev/mcd_ta/discount_engine/data/processed/day_level/dv_company/cutoff={c_date.strftime("%Y%m%d")}/lag={lag_}/duration={offer_duration_}/'
        os.makedirs(dir_, exist_ok=True)
        df_temp.to_parquet(f'{dir_}/part-0.parquet',index=False)
    except Exception as e:
        print(e)
        print(f'Failed for cutoff_date={c_date.strftime("%Y-%m-%d")}')
    t2 = pd.to_datetime('now')
    print(f'Time taken for cutoff_date {c_date.strftime("%Y-%m-%d")} is {t2-t1}')