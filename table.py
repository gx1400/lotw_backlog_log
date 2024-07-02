import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
import pandas as pd
import time
import requests
import schedule


def makeplot(showplot):
    
    url = "https://www.arrl.org/logbook-queue-status"
    try:
        html = requests.get(url=url, timeout=30).content
    except:
        return
    
    df_list = pd.read_html(html)
    df = df_list[-1]
    #print(df)
    tm = time.strftime("%Y%m%d-%H%M%S")
    filename = f'out\{tm}-lotw.csv'
    print (filename)
    df.to_csv(filename)


    #print(df.columns.values)

    times = df.get(('Status Epoch (UTC)','Status Epoch (UTC)')).tolist()
    logs = df.get(('Queue Size', 'Logs')).tolist()
    qsos = df.get(('Queue Size', 'QSOs')).tolist()
    bytes = df.get(('Queue Size', 'Bytes')).tolist()

    newdict = {"times": times
            ,"logs": logs
            ,"qsos": qsos
            #,"bytes": bytes
            }
    newdf = pd.DataFrame(newdict)
    newdf["times"] = newdf["times"].apply(pd.to_datetime)
    print(newdf)

    #ax = newdf.plot(x='times', subplots=True)
    #plt.show()
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

    ax.plot(newdf['times'], newdf['logs'], color='red', label='Log Queue')
    ax2.plot(newdf['times'], newdf['qsos'], color='blue', label='QSO Queue')
    ax.grid()

    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    plt.legend(lines + lines2, labels + labels2, loc='upper left')

    plt.title("LOTW Backlog")
    plt.tight_layout()
    fig.set_size_inches(12, 8)
    plt.savefig(f"out\{tm}.png")
    if showplot:
        plt.show()
    #return False

def runjob():
    makeplot(False)

schedule.every(15).minutes.do(runjob)

if __name__ == "__main__":
    makeplot(False)
    while True:
        schedule.run_pending()
