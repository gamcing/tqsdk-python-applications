from tqsdk import TqApi, TqAccount, TqBacktest, BacktestFinished
from tqsdk.lib import TargetPosTask
from tqsdk.tafunc import ma, crossup, crossdown

__author__ = "Y.M.Wong"

"""
来自价值8000的vnpy网络课程的一个策略demo

无趣的三均线模型
15分钟级别、三均线策略

· MA120 之上：
 - MA10 上穿 MA20 ，金叉，做多
 - MA10 下穿 MA20 ，死叉，平多

· MA120 之下：
 - MA10 下穿 MA20 ，死叉，做空
 - MA10 上穿 MA20 ，金叉，平空
"""


# 实盘交易
# api =  TqApi(TqAccount("G光大期货","[账号]","[密码]"), web_gui=True)
# 回测模式
from datetime import date
api = TqApi(backtest=TqBacktest(date(2019, 7, 1), date(2019, 12, 1)), web_gui=True)
# 策略初始化
symbol = "CFFEX.IF1912"
klines = api.get_kline_serial(symbol, 60 * 15)  # 订阅15分钟K线序列
target_pos = TargetPosTask(api, symbol)
try:
    while True:
        api.wait_update()
        if api.is_changing(klines):
            # MA120的最新值
            ma120 = ma(klines.close, 120).iloc[-1]
            # MA10上穿MA20的bool序列的最新值
            up_cross = crossup(ma(klines.close, 10), ma(klines.close, 20)).iloc[-1]
            # MA10下穿MA20的bool序列的最新值
            down_cross = crossdown(ma(klines.close, 10), ma(klines.close, 20)).iloc[-1]
            # 如果最新K线收盘价在MA120上方
            if klines.close.iloc[-1] > ma120:
                # 如果MA10上穿MA20，开一手
                if up_cross:
                    target_pos.set_target_volume(1)
                # 如果MA10下穿MA20，平仓
                elif down_cross:
                    target_pos.set_target_volume(0)
            # 如果最新K线收盘价在MA120下方
            else:
                # 如果MA10下穿MA20，开一手
                if down_cross:
                    target_pos.set_target_volume(-1)
                # 如果MA10上穿MA20，平仓
                elif up_cross:
                    target_pos.set_target_volume(0)
except BacktestFinished:
    print('回测结束！')

