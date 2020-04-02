"""
    根据put-call parity原理对所有支持天勤交易的期权-期货标的进行价差套利监测。
"""
from tqsdk import TqApi, TqSim, TqReplay, TqAccount
from tqsdk.lib import TargetPosTask
from tqsdk.tafunc import time_to_datetime
from tqsdk.exceptions import BacktestFinished
from datetime import datetime, date
from contextlib import closing
from opt import TqOption, OptionTrade
import os

#套利组task
trade_dict = dict()


def subscribe_main_parity(
    api,
    future_symbol,
    future_product_id,
    option_product_id,
    long_call,
    short_call,
    min_strike,
    max_strike,
    return_threshold=0.1,
    max_margin=4000,
):
    """
        对某个基础资产进行put-call parity异步套利。
    """

    # 取主力合约的信息
    kq_m = api.get_quote("KQ.m@" + future_symbol)
    # 用主力合约的symbol订阅报价
    future = api.get_quote(kq_m.underlying_symbol)
    # 初始化期货-期权信息
    opt_api = TqOption(
        api,
        underlying_future_id=kq_m.underlying_symbol,
        option_product_id=option_product_id,
    )
    # 按照行权价范围，选择需要的期权合约组合
    _, opts = opt_api.get_future_opt_symbols(
        strike_year=future.delivery_year,       #行权年
        strike_month=future.delivery_month,     #行权月
        min_strike=min_strike,                  #最小行权价
        max_strike=max_strike,                  #最大行权价
    )
    # 设定开仓信号：put-call parity residual的开仓临界值
    trade = OptionTrade(
        api,
        opt_api,
        kq_m.underlying_symbol,                 #主力合约的symbol
        opts,
        can_trade=True,                         #是否支持交易
        save_data=False,                        #是否存储数据
        long_call_threshold=long_call,          #开仓方案一：买call、卖put、空期货的开仓临界值，可None
        long_put_threshold=short_call,          #开仓方案一：买put、卖call、多期货的开仓临界值，可None
        return_threshold=return_threshold,      #开仓方案二：按照理论收益率（=残差/保证金占用）来开仓，可None
        max_margin=max_margin,                  #最大保证金占用，可None
    )
    global trade_dict
    # 使用future_symbol可以查询这组put-call parity arbitrage对象
    trade_dict[future_product_id] = trade
    print("期货代码：" + kq_m.underlying_symbol)
    future_target_pos = TargetPosTask(api, kq_m.underlying_symbol)
    # 订阅所有的期权合约套利组
    for opt in opts.values():
        trade.parity_quote_task(opt, kq_m.underlying_symbol, future_target_pos)


api = TqApi(TqSim(), web_gui=True)
# api = TqApi(TqAccount("G光大期货", "[账号]", "[密码]"), web_gui=True)
# api = TqApi(backtest=TqBacktest(start_dt=datetime(2020, 2, 3, 9), end_dt=datetime(2020, 2, 14, 16)))

subscribe_main_parity(api, "CZCE.SR", "SR", "SR", -100, -100, None, None)
subscribe_main_parity(api, "CZCE.CF", "CF", "CF", -100, -100, 12400, 13800)
subscribe_main_parity(api, "CZCE.MA", "MA", "MA", -100, -100, 1950, 2175)
subscribe_main_parity(api, "CZCE.TA", "TA", "TA", -100, -100, 4300, 4650)
subscribe_main_parity(api, "DCE.c", "c", "c_o", -100, -100, 1820, 2000)
subscribe_main_parity(api, "DCE.i", "i", "i_o", -100, -100, None, None)
subscribe_main_parity(api, "DCE.m", "m", "m_o", -100, -100, 2500, 2850)
# subscribe_main_parity(api, "CFFEX.IF", "i", "m_o", -100, -100, 2500, 2850)


def save_all(trade_dict):
    """
        保存put-call-parity的tick数据，此时OptionTrade的save_data=True
    """
    for k, trade in trade_dict.items():
        if trade.save_data:
            print("ts:{}".format(datetime.now()))
            date_str = datetime.now().strftime("%Y%m%d")
            time_str = datetime.now().strftime("%Y%m%d%H")
            if not os.path.exists("data/{}/{}/".format(date_str, k)):
                os.mkdir("data/{}/{}/".format(date_str, k))
            trade.quote_df.to_excel("data/{}/{}/{}.xlsx".format(date_str, k, time_str))


# 主线程
while True:
    api.wait_update()
    # 每分钟调用保存数据方法
    # if datetime.now().second == 0:
    #    save_all(trade_dict)
