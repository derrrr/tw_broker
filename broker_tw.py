# coding: utf-8

import os
import re
import time
from datetime import datetime
import pandas as pd
import numpy as np
import warnings

# ignore python warnings
warnings.filterwarnings("ignore")

def cook(filelist):
    skip = 0
    skip_ticker = []
    finished = 0
    for filename in filelist:
        # get ticker and date from filename
        name_pat = "(\d{4})_(\d{8})"
        name_date = re.search(name_pat, filename)
        ticker = name_date.group(1)
        mdate = name_date.group(2)
        date = datetime.strptime(mdate, "%Y%m%d")
        # date_ad = str(int(mdate) + 19110000)
        # date = datetime.strptime(date_ad, "%Y%m%d")
        
        # set the output path
        output_folder = "./broker_tw/" + ticker
        if not os.path.exists(output_folder):
            os.makedirs(output_folder, mode=0o777)
        output_file = ticker + "_broker_"+ date.strftime("%Y-%m-%d") +".csv"
        output = output_folder + "/" + output_file

        # skip if output exists
        if os.path.exists(output):
            finished = finished + 1
            continue

        # skip if csv is empty
        if os.stat(filename).st_size == 0:
            print(filename + " is an empty file, and will be skipped.")
            skip = skip + 1
            skip_ticker.append(filename[-17:-4])
            continue

        # read the raw data by pandas
        # path = "./raw/" + mdate + "/" + filename
        df_raw = pd.read_csv(filename, header=2, encoding="cp950")
        
        # check the raw data and skip if err
        if pd.isnull(df_raw.loc[0,"券商"]):
            print(filename + " has a problem, and will be skipped.")
            skip = skip + 1
            skip_ticker.append(filename[-17:-4])
            continue
        
        # split the raw data to 2 part, rename df_right header as df_left
        df_l = df_raw[list(df_raw.columns[:5])]
        df_r = df_raw[list(df_raw.columns[-5:])].dropna()
        reheader = [w.replace(".1", "") for w in list(df_r.columns)]
        df_r.columns = reheader
        
        # remove comma in TPEX data
        df_l.loc[:,["價格", "買進股數", "賣出股數"]] = df_l[["價格", "買進股數", "賣出股數"]].replace("[^0-9.]","", regex=True).values
        df_r.loc[:,["價格", "買進股數", "賣出股數"]] = df_r[["價格", "買進股數", "賣出股數"]].replace("[^0-9.]","", regex=True).values
        
        # reset dtype of df_l and df_r
        df_l.loc[:,["序號","買進股數","賣出股數"]] = df_l[["序號", "買進股數", "賣出股數"]].astype("int64").values
        df_r.loc[:,["序號","買進股數","賣出股數"]] = df_r[["序號", "買進股數", "賣出股數"]].astype("int64").values

        # reset dtype of df_l and df_r to float64
        df_l.loc[:, "價格"] = df_l.loc[:, "價格"].astype("float64").values
        df_r.loc[:, "價格"] = df_r.loc[:, "價格"].astype("float64").values
        
        # concat and reset index
        df = pd.concat([df_l, df_r], ignore_index=True).dropna()
        
        # slice the broker code and broker
        df["broker_code"] = df["券商"].str.slice(0,4)
        df["broker"] = df["券商"].str.slice(4).str.replace("\s","").str.replace("★","犇")
        df.__delitem__("序號")
        df.__delitem__("券商")
        
        # rename header
        new_cols = ["price", "buy_share", "sell_share"]
        df.rename(columns=dict(zip(df.columns[:3], new_cols)), inplace=True)
        
        gp = df.groupby(["broker_code", "broker"])
        
        # sum share, and calculate the weight average price
        buy_wavg = lambda x: np.around(np.ma.average(x, weights=df.loc[x.index, "buy_share"]), decimals=2)
        sell_wavg = lambda x: np.around(np.ma.average(x, weights=df.loc[x.index, "sell_share"]), decimals=2)
        f = {"buy_share" : sum, "sell_share" : sum, "price" : {"buy_wavg" : buy_wavg, "sell_wavg" : sell_wavg}}
        ga = gp.agg(f).fillna(0).reset_index()
        
        # merge the MultiIndex level
        ga.columns = [" ".join(col).strip() for col in ga.columns.values]
        
        # rename header
        ga_cols = ["broker_code", "broker", "buy_share", "sell_share", "buy_wavg" , "sell_wavg"]
        ga.rename(columns=dict(zip(ga.columns, ga_cols)), inplace=True)
        
        # calculate net share
        ga["net_share"] = ga["buy_share"] - ga["sell_share"]

        # calculate the percent of buy and sell
        # volume = ga["buy_share"].sum()
        # ga["percent_of_buy"] = ga["buy_share"].apply(lambda x: x*100/volume).round(2)
        # ga["percent_of_sell"] = ga["sell_share"].apply(lambda x: x*100/volume).round(2)
        
        # turn share to k share
        ga["buy_share"] = ga["buy_share"].apply(lambda x: x/1000).round(0).astype("int64")
        ga["sell_share"] = ga["sell_share"].apply(lambda x: x/1000).round(0).astype("int64")
        ga["net_share"] = ga["net_share"].apply(lambda x: x/1000).round(0).astype("int64")
        
        # rename header to k share
        ga_cols_k = ["broker_code", "broker", "buy_k_share", "sell_k_share", "buy_wavg" , "sell_wavg", "net_k_share"]
        ga.rename(columns=dict(zip(ga.columns, ga_cols_k)), inplace=True)
        
        # move column order
        cols = list(ga)
        cols.insert(3, cols.pop(cols.index("buy_wavg")))
        # cols.insert(4, cols.pop(cols.index("percent_of_buy")))
        # cols.insert(7, cols.pop(cols.index("percent_of_sell")))
        ga = ga.loc[:, cols]
        
        # sort by buy_k_share and broker_code
        ga = ga.sort_values(by=["buy_k_share", "broker_code"], ascending=False)
        
        # insert date and ticker column
        ga.insert(0, "date", date.strftime("%Y-%m-%d"))
        ga.insert(1, "ticker", ticker + ".TW")
        
        # output the broker dataframe as csv
        ga.to_csv(path_or_buf=output, index=False, encoding="utf-8-sig")
        
        print(output_file + " done!")
    return skip, skip_ticker, finished

"""
def raw_folder_check():
    raw_folder = "./raw"
    if not os.path.exists(raw_folder):
        os.makedirs(raw_folder, mode=0o777)
"""

def raw_folder_list():
    raw_list = []
    matket_folder = ["./otc", "./TWSE"]
    for folder in matket_folder:
        for path, subdirs, files in os.walk(folder + "/raw/"):
            for name in files:
                raw_list.append(os.path.join(path, name).replace("\\","/"))
    return raw_list


start_time = time.time()
# get file list in raw folder
raw_list = raw_folder_list()

total_file = len(raw_list)
cook_return = cook(raw_list)
finished_file = total_file - cook_return[2]
time_spent = time.time() - start_time

# print("--- total %s files, %s skipped ---" % (total_file - cook_return[2], cook_return[0]))
# print("--- %s sec spent ---" % (time.time() - start_time))
print("--- total {0:,} files, {1:,} skipped ---".format(finished_file, cook_return[0]))
print("--- {} spent ---".format(time.strftime("%H:%M:%S", time.gmtime(time_spent))))
if finished_file != 0:
    print("--- Avg: {:.2f} sec spent per file---".format(time_spent / finished_file))
if cook_return[1]:
    # print(" skipped: %s" %cook_return[1])
    print(" skipped: {}".format(cook_return[1]))