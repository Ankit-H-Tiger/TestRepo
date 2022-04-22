import pandas as pd
import panel as pn
import os
import os.path as op
pn.extension()
from ta_lib.data_utils.utils.digital import get_data_db, update_records, load_updated_pop_info
from ta_lib.core.api import load_yml, create_db_connection, create_context, display_as_tabs, setanalyse
from ta_lib.data_utils import digital_constants
from ta_lib.data_utils.read_raw import segmentation
from ta_lib.data_utils.data_processing.segmentation import segmentation as segmentation_processing

config_path = '../conf/config.yml'
cfg=create_context(config_path)
conn=create_db_connection(cfg.digital,"clickhouse")


segmentation.write_enrollment(cfg,conn,)

def write_customer_mapping(start_dt,end_dt):
    df_dates = pd.DataFrame()
    df_dates['start_date'] = pd.date_range(start_dt, end_dt, freq='MS')
    df_dates['end_date'] = df_dates.start_date.shift(-1)
    df_dates.dropna(inplace=True)
    df_dates['start_date_int'] = (df_dates.start_date - pd.to_datetime('1970-01-01')).dt.total_seconds().astype(int)
    df_dates['end_date_int'] = (df_dates.end_date - pd.to_datetime('1970-01-01')).dt.total_seconds().astype(int)
    import time
    t1 = time.time()

    for i, row in df_dates.iterrows():
        print(row['start_date'])
        # conn.execute("create TEMPORARY table tmptable as select customer_id,order_key,store_number from promo_check_create where profile_id = '' and customer_id is not null ")
        sql_query = f'''select distinct customer_id, profile_id from (select customer_id, profile_id from promo_check_create
                        where profile_id is not null and profile_id !='' and event_ts >= {row['start_date_int']} and event_ts < {row['end_date_int']}
                        UNION ALL
                        select customer_id, profile_id from promo_redeem_create
                        where profile_id is not null and profile_id !='' and event_ts >= {row['start_date_int']} and event_ts < {row['end_date_int']}
                        UNION ALL
                        select customer_id, b.profile_id from (select * from promo_check_create where event_ts >= {row['start_date_int']} and event_ts < {row['end_date_int']})
                        join (select * from promo_redeem_create where event_ts >= {row['start_date_int']} and event_ts < {row['end_date_int']}) as b USING(users_offer_id, external_id)
                        UNION ALL
                        select customer_id, b.profile_id from (select * from promo_check_create where event_ts >= {row['start_date_int']} and event_ts < {row['end_date_int']}) 
                        join (select * from promo_accumulate_create where event_ts >= {row['start_date_int']} and event_ts < {row['end_date_int']}) as b USING(order_key, store_number)
                        UNION ALL
                        select customer_id, b.profile_id from (select * from promo_check_create where event_ts >= {row['start_date_int']} and event_ts < {row['end_date_int']}) 
                        join (select * from loyalty_accumulate_create where event_ts >= {row['start_date_int']} and event_ts < {row['end_date_int']}) as b USING(order_key, store_number)
                        where accumulate_type_id IN (1, 4))
        '''
        df_profl_cust_mapping = pd.read_sql(sql_query, conn)
        df_profl_cust_mapping.to_parquet(
            f"/media/tiger/dev/mcd_ta/discount_engine/data/raw/segmentation/mapping_data/{row['start_date'].strftime('%Y%m%d')}.parquet")
        print((time.time() - t1) / 60)

df_mapping=pd.read_parquet('/media/tiger/dev/mcd_ta/discount_engine/data/raw/segmentation/mapping_data/')
print(df_mapping.shape)
df_mapping=df_mapping.drop_duplicates()
print(df_mapping.shape)

df,enroll_df = segmentation_processing.cust_prof_map_corrected(cfg)
df.to_parquet(op.join(cfg.digital['SEGMENTATION_CUST_PROF_MAP_PATH'].format(**cfg.digital),'part-0.parquet'),index=False)
enroll_df.to_parquet(op.join(cfg.digital['SEGMENTATION_ENROLLMENT_PATH'].format(**cfg.digital),'part-0.parquet'),index=False)

for date_ in date_list:
    ref_date = date_.strftime("%Y-%m-%d")
    print(ref_date)
    segmentation.write_aggregated_promo_check(cfg,conn,ref_date)

for date_ in date_list:
    ref_date = date_.strftime("%Y-%m-%d")
    print("ref_date:",ref_date)
    avg_check_val = segmentation_processing.calc_avg_check_offline(ref_date,conn)
    print(avg_check_val)
    df_seg = segmentation_processing.create_segment_lifecycle(cfg,ref_date,avg_check_val,net_or_gross="net")
    path_ = cfg.digital["SEGMENTATION_OUTPUT_AGG_PATH"].format(ref_date=ref_date.replace('-',''),**cfg.digital)
    os.makedirs(path_,exist_ok=True)
    df_seg.to_parquet(op.join(path_,'part-0.parquet'))

for date_ in date_list:
    ref_date = date_.strftime("%Y-%m-%d")
    print("ref_date:",ref_date)
    df = segmentation_processing.append_pre_registration_users(cfg, conn, ref_date)
    path_ = cfg.digital["GDW_FORMAT_OUTPUT_PATH"].format(ref_date=ref_date.replace('-',''),**cfg.digital)
    os.makedirs(path_,exist_ok=True)
    df.to_parquet(op.join(path_,'part-0.parquet'))

