import os
import talib
import pandas as pd
import matplotlib.pyplot as plt
from mpl_finance import candlestick2_ohlc
from mpl_finance import volume_overlay
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
	plt.show()
	return PicOutPath

if __name__ == '__main__':
	DataPath = 'D:/MCLKLineDataCenter/Data/Future/TQ/Main/Complete'
	FileOutPath = 'C:/PlotTest'
	if not os.path.exists(FileOutPath):
		os.makedirs(FileOutPath)
	Symbol = 'RB'
	Period = '60M'
	MA1Length = 5
	MA2Length = 20
	PlotBeginTime = 20210701
	PlotEndTime = 20210901
	MaxPlotLength = 20
	PicOutPath = MCLPlotHistoryKLineA1(DataPath, FileOutPath, Symbol, Period, MA1Length, MA2Length, PlotBeginTime, PlotEndTime, MaxPlotLength)