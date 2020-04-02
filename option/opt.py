from tqsdk import TqApi, TqSim, TqBacktest
from tqsdk.lib import TargetPosTask
from tqsdk.objs import Quote
from tqsdk.tafunc import time_to_datetime
from tqsdk.exceptions import BacktestFinished
from datetime import datetime, date
import numpy as np
import pandas as pd
from contextlib import closing
from typing import Union

class TqOption:
    """
        天勤的期权工具类
    """

    def __init__(self, api: TqApi, future_product_id:str = None, option_product_id:str = None, underlying_future_id:str = None):
        """

            Args:

                api (TqApi): 天勤Api

                future_product_id (str): 期货product_id

                option_product_id (str): 期权product_id

                underlying_future_id (str): 期货代码查期权

        """
        self._api = api
        self._now = time_to_datetime(api._backtest._current_dt) if api._backtest is not None else datetime.now()
        self._future_prod_id = future_product_id
        self._option_prod_id = option_product_id
        self._future_infoes = self._init_future_infoes() if underlying_future_id is None else [self._api.get_quote(underlying_future_id)]
        self.future_delivery_dates = self._fetch_delivery_dates()
        self._opt_infoes =  self._init_opt_infoes() if underlying_future_id is None else self._get_opt_infoes_by_underlying(underlying_future_id)
        self.strike_dates = self._fetch_strike_dates()

        self.future_opt_matched_dates = list(
            self.future_delivery_dates & self.strike_dates)  # 取期货期权同到期日
        self.future_opt_matched_dates.sort()
        self.strike_dates = list(self.strike_dates)
        self.strike_dates.sort()
        self.margin_rates = dict()  # 期权保证金率dict

    def _get_product_infoes(self, product_id: Union[str,list] = None, instrument_id:str = None) -> list:
        """
            根据product_id获取标的信息
        """
        if isinstance(product_id, str):
            product_id = [product_id]
        return [quote[1] for quote in self._api._data.get("quotes", {}).items() if not(quote[1].expired) and quote[1].product_id in product_id and datetime.fromtimestamp(quote[1].expire_datetime) > self._now]

    def _get_opt_infoes_by_underlying(self, underlying_future_id:str) -> list:
        """
            根据product_id获取标的信息
        """
        return [quote[1] for quote in self._api._data.get("quotes", {}).items() if quote[1].underlying_symbol == underlying_future_id and quote[1]["ins_class"] == "FUTURE_OPTION"]


    def _init_future_infoes(self) -> list:
        """
        初始化筛选期货合约初始信息

            Return:

                list(Quote): 期权合约信息

        """
        return self._get_product_infoes(self._future_prod_id)

    def _fetch_delivery_dates(self) -> set:
        """
        筛选出期货交割日set
        """
        return set([datetime.fromtimestamp(quote.expire_datetime) for quote in self._future_infoes])

    def _init_opt_infoes(self) -> list:
        """
        初始化筛选期权合约初始信息

        Args:

            underlying (str): 基础资产符号

        Return:

            list(Quote): 期权合约信息

        """
        opts = self._get_product_infoes([self._option_prod_id, self._option_prod_id + "C", self._option_prod_id + "P"])
        return [opt for opt in opts if len(opt["option_class"]) > 2]

    def _fetch_strike_dates(self) -> set:
        """
        筛选出未到期期权的行权日
        """
        return set([datetime.fromtimestamp(quote.expire_datetime) for quote in self._opt_infoes])

    def get_future_opt_symbols(self, strike_day: datetime = None, strike_year: int = None, strike_month: int = None, max_strike: float = None, min_strike: float = None) -> (str, dict):
        """
        根据到期日找期货、期权合约名

        Args:

            strike_day (datetime): 到期日

            strike_year (int): 到期年份（如果无法明确到期日）

            strike_money (int): 到期月份（如果无法明确到期日）

            max_strike (float): 最大行权价

            min_strike (float): 最小行权价

        Return:

            (期货合约代码, {行权价:{K:行权价,c:认购合约代码,p:认沽合约代码}})

        """
        if strike_day is None:
            temp_opt_infoes = pd.DataFrame(
                [quote for quote in self._opt_infoes if quote.delivery_year == strike_year and quote.delivery_month == strike_month])
        else:  # 用strike_year, strike_month查期权列表
            temp_opt_infoes = pd.DataFrame([quote for quote in self._opt_infoes if datetime.fromtimestamp(
                quote.expire_datetime) == strike_day])
        strike_prices = list(
            set([quote for quote in temp_opt_infoes['strike_price']]))
        if min_strike is not None:
            strike_prices = [p for p in strike_prices if p>= min_strike]
        if max_strike is not None:
            strike_prices = [p for p in strike_prices if p<= max_strike]
        strike_prices.sort()
        temp_future_id = ""
        first_opt = temp_opt_infoes.iloc[0]
        if first_opt["underlying_symbol"] == "":
            temp_future_id = [quote for quote in self._future_infoes if datetime.fromtimestamp(
                quote.expire_datetime) == strike_day][0].instrument_id
        else:
            temp_future_id = first_opt.underlying_symbol
        return (temp_future_id,
                {
                    strike_price:
                    {
                        'K': strike_price,
                        'c': temp_opt_infoes.loc[(temp_opt_infoes["strike_price"] == strike_price) & (temp_opt_infoes["option_class"] == "CALL")]["instrument_id"].iloc[0],
                        'p': temp_opt_infoes.loc[(temp_opt_infoes["strike_price"] == strike_price) & (temp_opt_infoes["option_class"] == "PUT")]["instrument_id"].iloc[0]
                    } for strike_price in strike_prices
                }
                )

    def get_opt_symbols(self, expire_date: datetime) -> list:
        """
            根据行权日筛选期权id
        """
        return [opt.instrument_id for opt in self._opt_infoes if opt.expire_datetime == expire_date]

    def get_margin_rate(self, quote: Quote) -> float:
        """
            计算合约的保证金率

            Return:
                (float) 保证金率

        """
        instrument_id = quote.instrument_id
        margin_rate = self.margin_rates.get(instrument_id, None)
        margin_from_dict = self._api._data["quotes"][instrument_id].get("margin", None)
        if margin_rate is not None and margin_rate > 0:
            return margin_rate
        elif margin_from_dict is not None and margin_from_dict > 0:
            self.margin_rates[instrument_id] = margin_rate
            return margin_from_dict
        elif quote.product_id == "IO_o":
            margin_rate = self._cal_io_margin_rate(
                quote.option_class, quote.strike_price, quote.pre_settlement, quote.pre_close, quote.volume_multiple)
            self.margin_rates[instrument_id] = margin_rate
            return margin_rate
        elif quote.ins_class == "FUTURE_OPTION":
            #future_quote = self._api.get_quote(quote.underlying_symbol)
            #future_margin = future_quote["margin"]
            future_margin = self._api._data["quotes"][quote.underlying_symbol].get("margin")
            margin_rate = self._cal_future_opt_margin_rate(
                quote.option_class, quote.strike_price, quote.pre_settlement, quote.volume_multiple, future_margin)
            self.margin_rates[instrument_id] = margin_rate
            return margin_rate
        else:
            return None

    def _cal_io_margin_rate(self, call_or_put: str, strike_price: float, pre_settle: float, pre_close: float, multiplier: float, margin_adj_factor: float = 0.1, min_risk_factor: float = 0.5) -> float:
        """
            计算IO的保证金率

            Args:

                call_or_put (str): call 或 put

                strike_price (float): 行权价

                pre_settle (float): 前结算价

                pre_close (float): 前收盘价

                multiplier (float): 合约乘数

                margin_adj_factor (float): 合约保证金调整系数

                min_risk_factor (float): 最低保障系数

        """

        if call_or_put == "CALL":
            # 虚值程度的计算
            in_money_rate = max(strike_price-pre_close, 0)
            # 每手保证金
            return multiplier*(pre_settle + max(pre_close*margin_adj_factor-in_money_rate, min_risk_factor*pre_close*margin_adj_factor))
        else:
            in_money_rate = max(pre_close-strike_price, 0)
            return multiplier*(pre_settle + max(pre_close*margin_adj_factor-in_money_rate, min_risk_factor*strike_price*margin_adj_factor))

    def _cal_future_opt_margin_rate(self, call_or_put: str, strike_price: float, pre_settle: float,  multiplier: float, future_margin:float) -> float:
        """
            计算商品期货期权的保证金率

                看涨期权的虚值额 = Max（期权合约行权价格 - 标的期货合约结算价，0）× 标的期货合约交易单位；
                看跌期权的虚值额 = Max（标的期货合约结算价 - 期权合约行权价格，0）× 标的期货合约交易单位。

                第四十五条 期权交易实行保证金制度。期权卖方交易保证金的收取标准为下列两者中较大者：
                （一）期权合约结算价 × 标的期货合约交易单位 + 标的期货合约交易保证金－（1/2）× 期权虚值额；
                （二）期权合约结算价 × 标的期货合约交易单位 +（1/2）× 标的期货合约交易保证金。

            Args:

                call_or_put (str): CALL 或 PUT

                strike_price (float): 行权价

                pre_settle (float): 前结算价

                pre_close (float): 前收盘价

                multiplier (float): 合约乘数



        """
        if pre_settle == 0 or np.isnan(pre_settle):
            return np.nan
        in_money_val = 0
        if call_or_put == "CALL":
            # 虚值额的计算
            in_money_val = max(strike_price-pre_settle, 0) * multiplier
        else:
            in_money_val = max(pre_settle-strike_price, 0) * multiplier
        return pre_settle * multiplier + max(future_margin - 0.5*in_money_val, future_margin * 0.5)

    def get_implied_risk_free(self, future_quote: Quote, strike_price: int, call_quote: Quote, put_quote: Quote) -> dict:
        """
            TODO: 根据put-call parity计算“隐含无风险收益率”

            Return:
                下述方程中分别解出risk_free之解
                last 成交价计算：call_last - put_last = (future_last - strike) * exp(-ttm * risk_free)
                mid 中间价计算：call_mid - put_mid = (future_mid - strike) * exp(-ttm * risk_free)
                long_call 买call、卖put、空期货策略：call_ask - put_bid = (future_bid - strike) * exp(-ttm * risk_free)
                short_call 买put、卖call、多期货策略：call_bid - put_ask = (future_ask - strike) * exp(-ttm * risk_free)

        """
        ttm = (time_to_datetime(call_quote.expire_datetime) -
               time_to_datetime(future_quote.datetime)).days  # 到期日
        call_quote_mid = (call_quote.ask_price1 + call_quote.bid_price1) / 2
        put_quote_mid = (put_quote.ask_price1 + put_quote.bid_price1) / 2
        future_mid = (future_quote.ask_price1 + future_quote.bid_price1) / 2
        risk_free_last = np.log((call_quote.last_price - put_quote.last_price) / (
            future_quote.last_price - strike_price)) / (-ttm) * 365
        risk_free_mid = np.log(
            (call_quote_mid - put_quote_mid) / (future_mid - strike_price)) / (-ttm) * 365
        risk_free_long_call = np.log((call_quote.ask_price1 - put_quote.bid_price1) / (
            future_quote.bid_price1 - strike_price)) / (-ttm) * 365
        risk_free_short_call = np.log((call_quote.bid_price1 - put_quote.ask_price1) / (
            future_quote.ask_price1 - strike_price)) / (-ttm) * 365
        return {'last': risk_free_last, 'mid': risk_free_mid, 'long_call': risk_free_long_call, 'short_call': risk_free_short_call}

    def get_parity_residual(self, future_quote:Quote, strike_price: float, call_quote:Quote, put_quote:Quote, risk_free: float = 0.0208) -> dict:
        """
            TODO: 计算折溢价

            Return:

                premium_last 成交价计算：call_last - put_last - (future_last - strike) * exp(-ttm * risk_free)
                premium_mid 中间价计算：call_mid - put_mid - (future_mid - strike) * exp(-ttm * risk_free)
                premium_call < 0: 买call、卖put、空期货策略：call_ask - put_bid - (future_bid - strike) * exp(-ttm * risk_free)
                premium_put < 0: 买put、卖call、多期货策略：-(call_bid - put_ask - (future_ask - strike) * exp(-ttm * risk_free))

        """
        ttm = (time_to_datetime(future_quote.expire_datetime) -
               time_to_datetime(future_quote.datetime)).days / 365  # 到期日
        call_quote_mid = (call_quote.ask_price1 + call_quote.bid_price1) / 2
        put_quote_mid = (put_quote.ask_price1 + put_quote.bid_price1) / 2
        future_mid = (future_quote.ask_price1 + future_quote.bid_price1) / 2
        residual_last = call_quote.last_price - put_quote.last_price - \
            (future_quote.last_price - strike_price) * np.exp(-ttm*risk_free)
        residual_mid = call_quote_mid - put_quote_mid - \
            (future_mid - strike_price) * np.exp(-ttm*risk_free)
        call_premium = call_quote.ask_price1 - put_quote.bid_price1 - \
            (future_quote.bid_price1 - strike_price) * np.exp(-ttm*risk_free)
        #long call 策略的理论到行权日的年化收益率
        long_call_cost =  call_quote.bid_price1 * call_quote.volume_multiple + self.get_margin_rate(put_quote) + self.get_margin_rate(future_quote)
        long_call_return = max(0, -call_premium*call_quote.volume_multiple / long_call_cost) / ttm
        put_premium = -(call_quote.bid_price1 - put_quote.ask_price1 - (future_quote.ask_price1 - strike_price) * np.exp(-ttm*risk_free))
        long_put_cost = put_quote.bid_price1 * put_quote.volume_multiple + self.get_margin_rate(call_quote) + self.get_margin_rate(future_quote)
        long_put_return = max(0, -put_premium*put_quote.volume_multiple / long_put_cost) / ttm
        return {
            'tq_time': future_quote.datetime,
            'premium_last': residual_last,
            'premium_mid': residual_mid,
            'premium_call': call_premium,
            'premium_put': put_premium,
            'long_call_cost': long_call_cost,
            'long_call_return': long_call_return,
            'long_put_cost': long_put_cost,
            'long_put_return':long_put_return,
            'ttm': ttm,
            'rf': risk_free,
            'strike': strike_price
        }


class OptionTrade:
    def __init__(self, api: TqApi, opt_api: TqOption, future_id: str, opts: dict, option_multiplier:int = 1, save_data: bool = False, can_trade: bool = False, long_call_threshold: float = 0, long_put_threshold: float = 0, return_threshold:float = 0.03, max_margin:float = None):
        """
            Args:

                option_multiplier: 期权/期货套利乘数，如IO和IF组=3

        """
        
        self.api = api
        self.opt_api = opt_api
        self.future_id = future_id
        self.quote_df = pd.DataFrame()
        self.can_trade = can_trade
        self._option_multiplier = option_multiplier
        self.save_data = save_data
        self.long_call_threshold = long_call_threshold
        self.long_put_threshold = long_put_threshold
        self.return_threshold = return_threshold
        self.max_margin = max_margin
        #self._window = window
        self._put_vols = dict()    #{认沽行权价:头寸}

    def on_quote(self, future_quote: Quote, strike_price: float, call_quote: Quote, put_quote: Quote):
        """策略部分：该方法处理截面推过来的期权、期货报价数据"""
        res = self.opt_api.get_parity_residual(
            future_quote, strike_price, call_quote, put_quote)
        res.update({'future_id':future_quote.instrument_id})
        # 如果理论收益率 > 临界值，则打印
        if res['long_call_return'] > self.return_threshold or res['long_put_return'] > self.return_threshold:
            print (str(res))        
        if self.save_data:
            res.update({
                'future_dt': future_quote.datetime,
                'future_last': future_quote.last_price,
                'future_bid': future_quote.bid_price1,
                'future_ask': future_quote.ask_price1,
                #'future_margin': future_quote["margin"],
                'call_dt': call_quote.datetime,
                'call_last': call_quote.last_price,
                'call_bid': call_quote.bid_price1,
                'call_ask': call_quote.ask_price1,
                #'call_margin': call_quote["margin"],
                'put_dt': put_quote.datetime,
                'put_last': put_quote.last_price,
                'put_bid': put_quote.bid_price1,
                'put_ask': put_quote.ask_price1,
                #'put_margin': put_quote["margin"],
            })
            self.quote_df = self.quote_df.append(res, ignore_index=True)
        if res['premium_call'] < self.long_call_threshold or res['long_call_return'] > self.return_threshold and (res['long_call_cost'] < self.max_margin or self.max_margin is None):
            return 1
        elif res['premium_put'] < self.long_put_threshold or res['long_put_return'] > self.return_threshold and (res['long_put_cost'] < self.max_margin or self.max_margin is None):
            return -1
        else:
            return 0

    def trade_group(self, strike_price: float, future_vol: int, future: TargetPosTask, call: TargetPosTask, put: TargetPosTask):
        """
            交易期权、期货组合套利

            Args:

                future_vol > 0, 对应long call + short put + short期货

        """
        call.set_target_volume(future_vol * self._option_multiplier)
        put.set_target_volume(-future_vol * self._option_multiplier)
        self._put_vols[strike_price] = -future_vol * self._option_multiplier
        future_target_vol = int(sum(self._put_vols.values()) / self._option_multiplier)
        future.set_target_volume(future_target_vol)
        

    async def quote_watcher(self, future_quote: Quote, strike_price: int, call_quote: Quote, put_quote: Quote, future_target_pos: TargetPosTask, call_target_pos: TargetPosTask, put_target_pos: TargetPosTask):
        """该task异步处理各个行权价格的推送"""
        # 当 quote 有更新时会发送通知到 update_chan 上
        async with self.api.register_update_notify([call_quote, put_quote, future_quote]) as update_chan:
            while True:
                async for _ in update_chan:  # 当从 update_chan 上收到行情更新通知时，运行...
                    if any([self.api.is_changing(quote, 'datetime') for quote in [future_quote, call_quote, put_quote]]):
                        trade_direction = self.on_quote(
                            future_quote, strike_price, call_quote, put_quote) 
                        if self.can_trade and trade_direction != 0:
                            self.trade_group(strike_price,
                                trade_direction, future_target_pos, call_target_pos, put_target_pos)

    def parity_quote_task(self, opt: dict, future_id: str, future_target_pos):
        call_quote = self.api.get_quote(opt['c'])
        put_quote = self.api.get_quote(opt['p'])
        future_quote = self.api.get_quote(future_id)
        #future_target_pos = TargetPosTask(self.api, future_id)
        call_target_pos = TargetPosTask(self.api, opt['c'])
        put_target_pos = TargetPosTask(self.api, opt['p'])
        put_position = self.api.get_position(opt['p'])
        self._put_vols[opt['K']] = put_position.pos
        self.api.create_task(self.quote_watcher(future_quote, opt['K'], call_quote, put_quote, future_target_pos, call_target_pos, put_target_pos))
        print('Quote subscribed ' + str(opt))