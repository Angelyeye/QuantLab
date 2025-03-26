# coding=utf-8
from tkinter import *
from tkinter import ttk
from PIL import Image, ImageTk
from mpl_finance import candlestick2_ohlc
from mpl_finance import volume_overlay
from datetime import datetime,timedelta
from pandas import to_datetime
import requests
import multiprocessing
import traceback
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import talib
import pandas as pd

def DateTimeDealFromTq(DateTimeFromTq):
    timenowtemp=DateTimeFromTq+8*3600000000000
    timenowbz=to_datetime(timenowtemp,format="%Y/%m/%d %H:%M:%S")
    timenowbz=str(timenowbz)+'.000000000'
    return timenowbz

def DateChangeToIntFromTq(TimeFromTq):
    time1 = TimeFromTq
    time2 = time1.split('.')[0]
    time3 = time2.split(' ')[0]
    time4 = time3.replace('-', '')
    return int(time4)

def TimeChangeToIntFromTq(TimeFromTq):
    time1 = TimeFromTq
    time2 = time1.split('.')[0]
    time3 = time2.split(' ')[-1]
    time4 = time3.replace(':', '')
    return int(time4)

def MCLPlotHistoryKLineA1(DataPath, FileOutPath, Symbol, Period, MA1Length, MA2Length, PlotBeginTime, PlotEndTime, MaxPlotLength):
    # mpl_finance.candlestick2_ochl(ax, opens, closes, highs, lows, width=4, colorup='k', colordown='r', alpha=0.75)  
    # mpl_finance.candlestick2_ohlc(ax, opens, highs, lows, closes, width=4, colorup='k', colordown='r', alpha=0.75)  +
    # mpl_finance.candlestick_ochl(ax, quotes, width=0.2, colorup='k', colordown='r', alpha=1.0)  
    # mpl_finance.candlestick_ohlc(ax, quotes, width=0.2, colorup='k', colordown='r', alpha=1.0)
    DataPathNow = os.path.join(DataPath, Symbol + '-' + Period + '.csv')
    DataNow = pd.read_csv(DataPathNow)
    PlotBeginTime = max(PlotBeginTime, DataNow['date'].iloc[0])
    if PlotEndTime == 0 or PlotEndTime > DataNow['date'].iloc[-1]:
        PlotEndTime = DataNow['date'].iloc[-1]
    NeedRows = DataNow[(DataNow['date'] >= PlotBeginTime) & (DataNow['date'] <= PlotEndTime)].index.tolist()
    DataNow = DataNow.iloc[NeedRows, :]
    DataNow.reset_index(drop=True, inplace = True)
    
    DataNow['datestr'] = [str(DataNow['date'].iloc[i]) for i in range(0, DataNow.shape[0])]
    DataNow['timestr'] = [str(DataNow['time'].iloc[i]) for i in range(0, DataNow.shape[0])]
    DataNow['datetime'] = DataNow['datestr'] + '_' + DataNow['timestr']
    DataNow['MA1'] = talib.MA(DataNow['close'], MA1Length)
    DataNow['MA2'] = talib.MA(DataNow['close'], MA2Length)
    if MaxPlotLength != 0:
        DataNow = DataNow.iloc[-MaxPlotLength:, :]
        DataNow.reset_index(drop=True, inplace = True)
    XTickStep = int(DataNow.shape[0] / 10)

    plt.rcParams['font.sans-serif'] = ['SimHei']  
    plt.rcParams['axes.unicode_minus'] = False  
    fig = plt.figure(figsize=(12, 8))
    ax1 = fig.add_subplot(2, 1, 1)
    plt.xlim([0, DataNow.shape[0]])
    plt.title(Symbol + '-' + Period + '简单版历史K线图' + ',数据范围:' + str(PlotBeginTime) + '-' + str(PlotEndTime) + ',绘图限制根数:' + str(MaxPlotLength), size=15)
    plt.plot(DataNow['MA1'], label= str(MA1Length) + '日均线')
    plt.plot(DataNow['MA2'], label= str(MA2Length) + '日均线')
    plt.legend(loc='upper left')
    candlestick2_ohlc(ax1, DataNow['open'], DataNow['high'], DataNow['low'], DataNow['close'],width=0.5, colorup='r', colordown='green', alpha=0.6)
    plt.xticks([])
    plt.grid(True)
    ax2 = fig.add_subplot(2, 1, 2)
    volume_overlay(ax2, DataNow['open'], DataNow['close'], DataNow['vol'], colorup='r', colordown='g', width=0.5, alpha=0.8)
    ax2.set_xticks(range(0, len(DataNow['datetime']), XTickStep))
    ax2.set_xticklabels(DataNow['datetime'][::XTickStep], rotation=30)
    plt.xlim([0, DataNow.shape[0]])  
    plt.grid(True)
    plt.tight_layout()
    PicOutPath = os.path.join(FileOutPath, Symbol + '-' + Period + '简单版历史K线图,数据范围' + str(PlotBeginTime) + '-' + str(PlotEndTime) + ',绘图限制根数' + str(MaxPlotLength) + '.jpg')
    plt.savefig(PicOutPath)
    # plt.show()
    return PicOutPath

def MCLPlotNowKLineA1(DataNow, FileOutPath, Symbol, Period, MA1Length, MA2Length, PlotBeginTime, MaxPlotLength):
    # mpl_finance.candlestick2_ochl(ax, opens, closes, highs, lows, width=4, colorup='k', colordown='r', alpha=0.75)  
    # mpl_finance.candlestick2_ohlc(ax, opens, highs, lows, closes, width=4, colorup='k', colordown='r', alpha=0.75)  +
    # mpl_finance.candlestick_ochl(ax, quotes, width=0.2, colorup='k', colordown='r', alpha=1.0)  
    # mpl_finance.candlestick_ohlc(ax, quotes, width=0.2, colorup='k', colordown='r', alpha=1.0)
    DataThis = DataNow.copy()
    DataThis['datestr'] = [str(DataThis['date'].iloc[i]) for i in range(0, DataThis.shape[0])]
    DataThis['timestr'] = [str(DataThis['time'].iloc[i]) for i in range(0, DataThis.shape[0])]
    DataThis['datetime'] = DataThis['datestr'] + '_' + DataThis['timestr']
    DataThis['MA1'] = talib.MA(DataThis['close'], MA1Length)
    DataThis['MA2'] = talib.MA(DataThis['close'], MA2Length)
    if MaxPlotLength != 0:
        DataThis = DataThis.iloc[-MaxPlotLength:, :]
        DataThis.reset_index(drop=True, inplace = True)
    XTickStep = int(DataThis.shape[0] / 10)

    plt.rcParams['font.sans-serif'] = ['SimHei']  
    plt.rcParams['axes.unicode_minus'] = False  
    fig = plt.figure(figsize=(12, 8))
    ax1 = fig.add_subplot(2, 1, 1)
    plt.xlim([0, DataThis.shape[0]])
    plt.title(Symbol + '-' + Period + '简单版实时K线图' + ',开始时间:' + str(PlotBeginTime) + ',绘图限制根数:' + str(MaxPlotLength)  + '\n' 
        + '最后K线 -- > date:' + str(DataThis['date'].iloc[-1]) + ',time:' + str(DataThis['time'].iloc[-1])
        + ',open:' + str(DataThis['open'].iloc[-1]) + ',high:' + str(DataThis['high'].iloc[-1])
        + ',low:' + str(DataThis['low'].iloc[-1]) + ',close:' + str(DataThis['close'].iloc[-1]), size=15)
    plt.plot(DataThis['MA1'], label= str(MA1Length) + '日均线')
    plt.plot(DataThis['MA2'], label= str(MA2Length) + '日均线')
    plt.legend(loc='upper left')
    candlestick2_ohlc(ax1, DataThis['open'], DataThis['high'], DataThis['low'], DataThis['close'],width=0.5, colorup='r', colordown='green', alpha=0.6)
    plt.xticks([])
    plt.grid(True)
    ax2 = fig.add_subplot(2, 1, 2)
    volume_overlay(ax2, DataThis['open'], DataThis['close'], DataThis['vol'], colorup='r', colordown='g', width=0.5, alpha=0.8)
    ax2.set_xticks(range(0, len(DataThis['datetime']), XTickStep))
    ax2.set_xticklabels(DataThis['datetime'][::XTickStep], rotation=30)
    plt.xlim([0, DataThis.shape[0]])  
    plt.grid(True)
    plt.tight_layout()
    PicOutPath = os.path.join(FileOutPath, Symbol + '-' + Period + '简单版实时K线图' + ',开始时间' + str(PlotBeginTime) + ',绘图限制根数' + str(MaxPlotLength) + '.jpg')
    plt.savefig(PicOutPath)
    plt.close()
    # plt.show()
    return PicOutPath

def MCLPlotKLineGUI(MyKline, PlotTypeCombobox, HistoryDataPathEnter, OutFilePathEnter, HistorySymbolEnter, HistoryPeriodEnter, NowSymbolEnter, NowPeriodEnter, MA1PeriodEnter, MA2PeriodEnter, HistoryDataBeginEnter, HistoryDataEndEnter, MaxPlotLengthEnter):
    global img_png
    PlotTypeNow = PlotTypeCombobox.get()
    if PlotTypeNow == '历史绘图':
        DataPath = HistoryDataPathEnter.get()
        FileOutPath = OutFilePathEnter.get()
        if not os.path.exists(FileOutPath):
            os.makedirs(FileOutPath)
        Symbol = HistorySymbolEnter.get()
        Period = HistoryPeriodEnter.get()
        MA1Length = int(MA1PeriodEnter.get())
        MA2Length = int(MA2PeriodEnter.get())
        PlotBeginTime = int(HistoryDataBeginEnter.get())
        PlotEndTime = int(HistoryDataEndEnter.get())
        MaxPlotLength = int(MaxPlotLengthEnter.get())
        PicOutPath = MCLPlotHistoryKLineA1(DataPath, FileOutPath, Symbol, Period, MA1Length, MA2Length, PlotBeginTime, PlotEndTime, MaxPlotLength)
        img_open = Image.open(PicOutPath)
        img_png = ImageTk.PhotoImage(img_open)
        label_img = Label(MyKline, image = img_png, width=1200, height=800)
        label_img.place(x=0, y=180)
    elif PlotTypeNow == '实时更新':
        from tqsdk import TqApi, TqAuth
        DataPath = HistoryDataPathEnter.get()
        FileOutPath = OutFilePathEnter.get()
        if not os.path.exists(FileOutPath):
            os.makedirs(FileOutPath)
        Symbol = HistorySymbolEnter.get()
        Period = HistoryPeriodEnter.get()
        SymbolNow = NowSymbolEnter.get()
        PeriodNow = NowPeriodEnter.get()
        MA1Length = int(MA1PeriodEnter.get())
        MA2Length = int(MA2PeriodEnter.get())
        PlotBeginTime = int(HistoryDataBeginEnter.get())
        PlotEndTime = int(HistoryDataEndEnter.get())
        MaxPlotLength = int(MaxPlotLengthEnter.get())
        DataPathNow = os.path.join(DataPath, Symbol + '-' + Period + '.csv')
        DataNow = pd.read_csv(DataPathNow)
        PlotBeginTime = max(PlotBeginTime, DataNow['date'].iloc[0])
        if PlotEndTime == 0 or PlotEndTime > DataNow['date'].iloc[-1]:
            PlotEndTime = DataNow['date'].iloc[-1]
        NeedRows = DataNow[(DataNow['date'] >= PlotBeginTime) & (DataNow['date'] <= PlotEndTime)].index.tolist()
        DataNow = DataNow.iloc[NeedRows, :]
        DataNow.reset_index(drop=True, inplace = True)
        api = TqApi(auth=TqAuth("信易账户", "账户密码"))
        klines = api.get_kline_serial(SymbolNow, int(PeriodNow) * 60, 200)
        while True:
            try:
                api.wait_update()
                KlinesNowCopy = klines.copy()
                C = list(map(DateTimeDealFromTq, list(KlinesNowCopy['datetime'])))
                KlinesNowCopy['datetime'] = C
                A = list(map(DateChangeToIntFromTq, list(KlinesNowCopy.datetime)))
                B = list(map(TimeChangeToIntFromTq, list(KlinesNowCopy.datetime)))
                KlinesNowCopy['date'] = A
                KlinesNowCopy['time'] = B
                KLinesNeed = KlinesNowCopy.copy()
                KLinesNeed = KLinesNeed[['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'open_oi', 'close_oi']]
                KLinesNeed.columns = ['date', 'time', 'open', 'high', 'low', 'close', 'vol', 'open_oi', 'close_oi']
                DataNow = pd.concat([DataNow, KLinesNeed])
                DataNow = DataNow.dropna(axis=0,how='any')
                DataNow = DataNow.drop_duplicates(subset=['date', 'time'], keep='last').reset_index(drop=True)
                DataNow = DataNow.reset_index(drop=True)
                DataNow.sort_values(by=['date', 'time'],inplace=True)
                DataNow = DataNow.reset_index(drop=True)
                PicOutPath = MCLPlotNowKLineA1(DataNow, FileOutPath, Symbol, Period, MA1Length, MA2Length, PlotBeginTime, MaxPlotLength)
                img_open = Image.open(PicOutPath)     
                img_png = ImageTk.PhotoImage(img_open)
                label_img = Label(MyKline, image = img_png, width=1200, height=800)
                label_img.place(x=0, y=180)
                
                MyKline.update()
            except:
                MyKline.update()
                dferror = traceback.format_exc()
                print(dferror)               

    return 0

if __name__ == '__main__':
    global img_png
    img_png = None

    MyKline = Tk()
    MyKline.title("MCL2021简单版K线绘制器V1.0.0")
    MyKline.geometry('1200x1000')                
    MyKline.resizable(width=False, height=False)

    HistoryDataPathLabel = Label(MyKline, text="历史数据路径:", bg="white", font=("宋体", 12), width=14, height=1)
    HistoryDataPathLabel.place(x=40, y=10)
    HistoryDataPathEnter = Entry(MyKline, width=50)
    HistoryDataPathEnter.place(x=180, y=10)
    HistoryDataPathEnter.insert(0, 'D:/MCLKLineDataCenter/Data/Future/TQ/Main/Complete')

    OutFilePathLabel = Label(MyKline, text="图形保存路径:", bg="white", font=("宋体", 12), width=14, height=1)
    OutFilePathLabel.place(x=640, y=10)
    OutFilePathEnter = Entry(MyKline, width=50)
    OutFilePathEnter.place(x=780, y=10)
    OutFilePathEnter.insert(0, 'C:/PlotTest')

    HistorySymbolLabel = Label(MyKline, text="历史数据代码:", bg="white", font=("宋体", 12), width=14, height=1)
    HistorySymbolLabel.place(x=40, y=50)
    HistorySymbolEnter = Entry(MyKline, width=15)
    HistorySymbolEnter.place(x=180, y=50)
    HistorySymbolEnter.insert(0, 'RB')

    HistoryPeriodLabel = Label(MyKline, text="历史数据周期:", bg="white", font=("宋体", 12), width=14, height=1)
    HistoryPeriodLabel.place(x=320, y=50)
    HistoryPeriodEnter = Entry(MyKline, width=15)
    HistoryPeriodEnter.place(x=460, y=50)
    HistoryPeriodEnter.insert(0, '60M')

    NowSymbolLabel = Label(MyKline, text="实时数据代码:", bg="white", font=("宋体", 12), width=14, height=1)
    NowSymbolLabel.place(x=600, y=50)
    NowSymbolEnter = Entry(MyKline, width=15)
    NowSymbolEnter.place(x=740, y=50)
    NowSymbolEnter.insert(0, 'KQ.m@SHFE.rb')

    NowPeriodLabel = Label(MyKline, text="实时数据周期:", bg="white", font=("宋体", 12), width=14, height=1)
    NowPeriodLabel.place(x=880, y=50)
    NowPeriodEnter = Entry(MyKline, width=15)
    NowPeriodEnter.place(x=1020, y=50)
    NowPeriodEnter.insert(0, '60')

    MA1PeriodLabel = Label(MyKline, text="均线1周期:", bg="white", font=("宋体", 12), width=14, height=1)
    MA1PeriodLabel.place(x=40, y=90)
    MA1PeriodEnter = Entry(MyKline, width=15)
    MA1PeriodEnter.place(x=180, y=90)
    MA1PeriodEnter.insert(0, '5')

    MA2PeriodLabel = Label(MyKline, text="均线2周期:", bg="white", font=("宋体", 12), width=14, height=1)
    MA2PeriodLabel.place(x=320, y=90)
    MA2PeriodEnter = Entry(MyKline, width=15)
    MA2PeriodEnter.place(x=460, y=90)
    MA2PeriodEnter.insert(0, '20')

    HistoryDataBeginLabel = Label(MyKline, text="数据开始时间:", bg="white", font=("宋体", 12), width=14, height=1)
    HistoryDataBeginLabel.place(x=600, y=90)
    HistoryDataBeginEnter = Entry(MyKline, width=15)
    HistoryDataBeginEnter.place(x=740, y=90)
    HistoryDataBeginEnter.insert(0, '20210701')

    HistoryDataEndLabel = Label(MyKline, text="数据结束时间:", bg="white", font=("宋体", 12), width=14, height=1)
    HistoryDataEndLabel.place(x=880, y=90)
    HistoryDataEndEnter = Entry(MyKline, width=15)
    HistoryDataEndEnter.place(x=1020, y=90)
    HistoryDataEndEnter.insert(0, '20210901')

    PlotTypeLabel = Label(MyKline, text="绘图类型:", bg="white", font=("宋体", 12), width=14, height=1)
    PlotTypeCombobox = ttk.Combobox(MyKline)
    PlotTypeCombobox['value'] = ('历史绘图', '实时更新')
    PlotTypeLabel.place(x=140,y=140)
    PlotTypeCombobox.place(x=280, y=140)
    PlotTypeCombobox.current(0)

    MaxPlotLengthLabel = Label(MyKline, text="最大绘图数量(写0不限制):", bg="white", font=("宋体", 12), width=25, height=1)
    MaxPlotLengthLabel.place(x=700, y=140)
    MaxPlotLengthEnter = Entry(MyKline, width=15)
    MaxPlotLengthEnter.place(x=950, y=140)
    MaxPlotLengthEnter.insert(0, '0')

    MCLPlotBtn = Button(MyKline, text="绘 制 图 形", bg="red",
        command = lambda:MCLPlotKLineGUI(MyKline, PlotTypeCombobox, HistoryDataPathEnter, OutFilePathEnter, HistorySymbolEnter, HistoryPeriodEnter, 
            NowSymbolEnter, NowPeriodEnter, MA1PeriodEnter, MA2PeriodEnter, HistoryDataBeginEnter, HistoryDataEndEnter, MaxPlotLengthEnter), 
        font=("宋体", 12, 'bold'), width=15, height=1)
    MCLPlotBtn.place(x=500,y=137)

    MyKline.mainloop()








