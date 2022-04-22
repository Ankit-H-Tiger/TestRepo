import pandas as pd
import numpy as np
import multiprocessing as mlp
import time
import argparse

import os
import gc

from ta_lib.data_utils.data_processing import get_aggregated_data
from ta_lib.core.api import load_yml, create_context

# Defining configs

config_path = '../../conf/config.yml'
cfg = create_context(config_path)

result_list = []
def log_result(result):
    # This is called whenever foo_pool(i) returns a result.
    # result_list is modified only by the main process, not the pool workers.
    result_list.append(result)

def call_func(item_level=False, gc=False, grouped=True, mode=False):
    dates_= sorted(pd.date_range('2021-11-01','2022-01-01',freq='MS'),reverse=True)
    dates_=[pd.to_datetime(i) for i in dates_]
    dates_=[f"month_id={i.strftime('%Y%m%d')}" for i in dates_]
#     pool = mlp.Pool(3)
    if not grouped:
        for run_date in dates_[:]:
        #     pool.apply_async(, args=(cfg.offline, result_filter='all', day_level=True, item_level=True, get_price_info=False,include_105=True), callback = log_result)
            print(run_date)
            if item_level:
                print("create item qty")
                get_aggregated_data.get_ungrouped_data(cfg.offline,run_date,'all',False, True, False,True)
            if gc:
                print("create gc")
                get_aggregated_data.get_ungrouped_data(cfg.offline,run_date ,'offline',True, False, False,True)
            if mode:
                print("create mode")
                get_aggregated_data.get_mode_from_trn(cfg.offline,run_date,'all',False)
    else:
        print("create grouped units")
        get_aggregated_data.get_grouped_units_from_ungrouped(cfg.offline,'digital',)

#     pool.close()
#     pool.join()
    
if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--item_level')
    parser.add_argument('--gc')
    parser.add_argument('--grouped')
    parser.add_argument('--mode')
    args = parser.parse_args()
    print(bool(args.item_level))
    my_dict = {'item_level': bool(args.item_level),'gc': bool(args.gc),'grouped': bool(args.grouped),'mode':bool(args.mode)}
#     my_dict = {'item_level': False,'gc': False, 'grouped':True,'mode':False}
    print(my_dict)
    call_func(**my_dict)
