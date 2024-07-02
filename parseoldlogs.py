import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
import pandas as pd
import sqlite3
import time
import requests
from datetime import datetime
import schedule
import os


dbname = "db\lotw_backlog.db"
logfolder = 'outold'

def db_create_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS backlog (
      id INTEGER PRIMARY KEY
      , time DATETIME NOT NULL
      , logs INT
      , qsos INT
      , bytes BIGINT
    );
    """
    
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()

def db_ins_rec(conn, row):
    sql = f"""
    INSERT INTO backlog ( 
        TIME, 
        logs, 
        qsos, 
        bytes 
    ) 
    VALUES 
    (  
        datetime("{row['times']}")
        ,{row['logs']} 
        ,{row['qsos']} 
        ,{row['bytes']}
    );
    """
    #print(sql)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()

def db_rec_exists(conn, row):
    sql = f"""
    SELECT rowid FROM backlog WHERE time = datetime("{row['times']}");
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    return len(result)>0 

def db_get_df(conn):
    sql = f"""
    SELECT * FROM backlog ORDER BY time ASC;
    """
    df = pd.read_sql_query(sql, conn)
    df["time"] = df["time"].apply(pd.to_datetime)
    #print(df)
    return df
    

def main():
    with sqlite3.connect(dbname) as conn:
        try:
            for (dirpath, dirnames, filenames) in os.walk(logfolder):
                for filename in filenames:
                    print(filename)
                    try:
                        df = pd.read_csv(os.path.join(dirpath,filename))
                        df = df.drop(0)
                        print(df)
                        times = df.get('Status Epoch (UTC)').tolist()
                        logs = df.get('Queue Size').tolist()
                        qsos = df.get('Queue Size.1').tolist()
                        bytes = df.get('Queue Size.2').tolist()

                        newdict = {"times": times
                            ,"logs": logs
                            ,"qsos": qsos
                            ,"bytes": bytes
                            }
                        newdf = pd.DataFrame(newdict)
                        newdf["times"] = newdf["times"].apply(pd.to_datetime)
                        print(newdf)
                        for idx, row in newdf.iterrows():
                            if not db_rec_exists(conn, row):
                                db_ins_rec(conn, row)
                                print(f"record inserted for: {row['times']}")
                            else:
                                print(f"record already exists for: {row['times']}")
                    except Exception as e:
                        print(e)
                        
        except sqlite3.Error as e:
            print(e)
if __name__ == "__main__":
    main()
