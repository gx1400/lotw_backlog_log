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

outfoldername = 'out_html'

def get_df_html():
    url = "https://www.arrl.org/logbook-queue-status"
    try:
        html = requests.get(url=url, timeout=30).content
        df_list = pd.read_html(html)
        df = df_list[-1]

        tm = time.strftime("%Y%m%d-%H%M%S")
        filename = f'{outfoldername}\{tm}-lotw.csv'
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        #print (filename)
        df.to_csv(filename)


        #print(df.columns.values)

        times = df.get(('Status Epoch (UTC)','Status Epoch (UTC)')).tolist()
        logs = df.get(('Queue Size', 'Logs')).tolist()
        qsos = df.get(('Queue Size', 'QSOs')).tolist()
        bytes = df.get(('Queue Size', 'Bytes')).tolist()

        newdict = {"times": times
                ,"logs": logs
                ,"qsos": qsos
                ,"bytes": bytes
                }
        newdf = pd.DataFrame(newdict)
        newdf["times"] = newdf["times"].apply(pd.to_datetime)
    except Exception as e:
        print(e)
        return

    return newdf

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
    

def db(df):
    os.makedirs(os.path.dirname(dbname), exist_ok=True)
    
    with sqlite3.connect(dbname) as conn:
        try:
            db_create_table(conn)
            for idx, row in df.iterrows():
                #print(f"{row['times']} - {row['logs']} - {row['qsos']} - {row['bytes']}")
                if not db_rec_exists(conn, row):
                    db_ins_rec(conn, row)
                    print(f"record inserted for: {row['times']}")
                #else:
                    #print(f"record already exists for: {row['times']}")
            df = db_get_df(conn)
            return df
        except sqlite3.Error as e:
            print(e)
    
def makeplot(df):
    fig, ax = plt.subplots()
    ax2 = ax.twinx()
    plt.xticks(rotation=45, fontweight='light',  fontsize='x-small')

    dtFmt = mdates.DateFormatter('%m-%d\n%H:%M') # define the formatting
    ax.xaxis.set_major_formatter(dtFmt)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

    ax.set_xlabel('Time (UTC)', fontsize=14)
    ax.set_ylabel('Log Queue', fontsize=14)
    ax2.set_ylabel('QSO Queue', fontsize=14)

    ax.plot(df['time'], df['logs'], color='red', label='Log Queue')
    ax2.plot(df['time'], df['qsos'], color='blue', label='QSO Queue')
    ax.grid()

    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    plt.legend(lines + lines2, labels + labels2, loc='upper left')

    plt.title("LOTW Backlog")
    #plt.tight_layout()
    fig.set_size_inches(12, 8)
    tm = time.strftime("%Y%m%d-%H%M%S")
    plotfilename = f"plot\{tm}.png"
    os.makedirs(os.path.dirname(plotfilename), exist_ok=True)
    plt.savefig(plotfilename)

def main():
    df = get_df_html()
    dbdf = db(df)
    #print(dbdf)
    makeplot(dbdf)
    
schedule.every(15).minutes.do(main)

if __name__ == "__main__":
    main()
    while True:
        schedule.run_pending()
