import os,glob
import pandas as pd
import numpy as np
from typing import Dict
import tqdm
import dolphindb as ddb

def create_database(session,save_database,save_table):
    if session.existsDatabase(save_database):
        session.dropDatabase(save_database)
    if session.existsTable(dbUrl=save_database, tableName=save_table):
        session.dropTable(dbPath=save_database, tableName=save_table)
    columns_name = ["date", "kana_str","kana_pair", "kanji", "rensou_kanji", "rensou_kana"]
    columns_type = ["DATE", "SYMBOL", "SYMBOL", "SYMBOL", "STRING", "STRING"]
    session.run(f"""
        dbDate=database(, VALUE, 2024.01.01..2030.01.01);
        dbID=database(, HASH, [SYMBOL, 3]);
        db = database("{save_database}", COMPO, [dbDate, dbID]);
        schemaTb=table(1:0,{columns_name},{columns_type});
        t=db.createPartitionedTable(table=schemaTb,tableName="{save_table}",partitionColumns=["date","kana_str"])
    """)


def init_path(path_dir):
    "创建当前.py目录下的文件夹"
    if os.path.exists(path=path_dir)==bool(False):
        os.mkdir(path=path_dir)
    return None

def get_glob_list(path_dir):
    "返回符合条件的文件名列表"
    # return glob.glob(pathname=path_dir)
    return [os.path.basename(i) for i in glob.iglob(pathname=path_dir,recursive=False)]

def processing(df, kana_str) -> Dict:
    empty_list = [np.nan,None,float("nan")]
    col_list = df.columns.tolist()
    idx_list = [i for i in range(len(col_list)) if "漢字" in col_list[i]]
    kana_list = []
    kanji_list = []
    rensou_kanji_list = []
    rensou_kana_list = []
    for idx in idx_list:
        start_idx, end_idx = idx-1, idx+1
        start_col, end_col = col_list[start_idx], col_list[end_idx]
        slice_df = df[[start_col,col_list[idx],end_col]]
        slice_df.columns = ["kana","kanji","rensou"]
        for i,row in slice_df.iterrows():
            kana, kanji, rensou = row["kana"],row["kanji"],row["rensou"]
            if kanji not in empty_list:
                if rensou not in empty_list:
                    rensou = str(rensou).replace("\n","#")
                    slice_rensou = ""
                    for j in rensou:
                        if j == "#":
                            kana_list.append(kana)
                            kanji_list.append(kanji)
                            rensou_kana_list.append(slice_rensou[slice_rensou.index("｜")+1:])
                            rensou_kanji_list.append(slice_rensou[:slice_rensou.index("｜")])
                            slice_rensou = ""
                            continue
                        else:
                            slice_rensou+=j
                    kana_list.append(kana)
                    kanji_list.append(kanji)
                    if "｜" in slice_rensou:
                        rensou_kana_list.append(slice_rensou[slice_rensou.index("｜") + 1:])
                        rensou_kanji_list.append(slice_rensou[:slice_rensou.index("｜")])
                    else:
                        rensou_kana_list.append("")
                        rensou_kanji_list.append(slice_rensou)
                else:
                    kana_list.append(kana)
                    kanji_list.append(kanji)
                    rensou_kana_list.append("")
                    rensou_kanji_list.append("")


    result_df = pd.DataFrame({"date":[pd.Timestamp(pd.Timestamp.now()).date()]*len(kana_list),
                              "kana_str":[kana_str]*len(kana_list),
                              "kana_pair":kana_list,
                              "kanji":kanji_list,
                              "rensou_kanji":rensou_kanji_list,
                              "rensou_kana":rensou_kana_list})
    result_df["date"]=result_df["date"].apply(pd.Timestamp)
    return result_df

if __name__ == "__main__":
    session=ddb.session()
    session.connect("localhost",8848,"admin","123456")
    pool=ddb.DBConnectionPool("localhost",8848,10,"admin","123456")
    create_database(session,"dfs://JP","word")
    appender = ddb.PartitionedTableAppender(dbPath="dfs://JP",
                                            tableName="word",
                                            partitionColName="date",
                                            dbConnectionPool=pool)  # 写入数据的appender
    kana_list = sorted([i for i in get_glob_list(r".\*.xlsx") if "$" not in i])
    roma_list = [str(i)[:str(i).index(".xlsx")] for i in kana_list]
    for roma in tqdm.tqdm(roma_list,desc="processing"):
        df_dict= pd.read_excel(rf".\{roma}.xlsx",index_col=None,header=0,sheet_name=None)
        current_kana_list = list(df_dict.keys())
        for kana in current_kana_list:
            df = df_dict[kana]
            result_df = processing(df, kana_str=kana)
            if not result_df.empty:
                appender.append(result_df)
            else:
                print(roma,kana)