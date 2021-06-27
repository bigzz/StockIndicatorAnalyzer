import json
import time

import pandas as pd
import requests

def stock_industry_detail() -> pd.DataFrame:
    """
    """
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "20",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "b:BK0729 f:!50",
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152,f45",
        "cb": "jQuery112402116588773191168_1624780111857",
        "_": int(time.time() * 1000),
    }
    r = requests.get(url, params=params)
    text_data = r.text
    json_data = json.loads(text_data[text_data.find("{") : -2])
    temp_df = pd.DataFrame(json_data["data"]["diff"])
    temp_df.columns = [
        "f1:-",
        "f2:最新价",
        "f3:涨跌幅",
        "f4:涨跌额",
        "f5:成交量",
        "f6:成交额",
        "f7:震幅",
        "f8:换手率",
        "f9:动态市盈率",
        "f10:量比",
        "f11:-",
        "f12:股票代号",
        "f13:-",
        "f14:股票名称",
        "f15:最高",
        "f16:最低",
        "f17:今开",
        "f18:昨收",
        "f20:总市值",
        "f21:流通市值",
        "f22:涨速",
        "f23:市净率",
        "f24:-",
        "f25:今年以来累计涨幅",
        "f45:净利润",
        "f62:今日主力净流入",
        "f115:滚动市盈率",
        "f128:-",
        "f140:-",
        "f141:-",
        "f136:-",
        "f152:-",
    ]
    temp_df = temp_df[
        [
            "f12:股票代号",
            "f14:股票名称",
            "f115:滚动市盈率",
            "f23:市净率",
            "f20:总市值",
            "f21:流通市值",
            "f45:净利润",
            "f25:今年以来累计涨幅",
        ]
    ]
    return temp_df


def stock_industry_list() -> pd.DataFrame:
    """
    东方财富网-行情中心-沪深板块-行业板块
    http://quote.eastmoney.com/center/boardlist.html#industry_board
    :param indicator: choice of {"今日", "3日", "5日", "10日"}
    :type indicator: str
    :return: 指定 indicator 资金流向排行
    :rtype: pandas.DataFrame
    """

    url = "http://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "20",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:90 t:2 f:!50",
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222",
        "cb": "jQuery1124025006705373630944_1624787835343",
        "_": int(time.time() * 1000),
    }
    r = requests.get(url, params=params)
    text_data = r.text
    json_data = json.loads(text_data[text_data.find("{") : -2])
    temp_df = pd.DataFrame(json_data["data"]["diff"])
    temp_df.columns = [
        "f1:-",
        "f2:最新价",
        "f3:涨跌幅",
        "f4:涨跌额",
        "f5:成交量",
        "f6:成交额",
        "f7:震幅",
        "f8:换手率",
        "f9:市盈率",
        "f10:量比",
        "f11:-",
        "f12:板块代号",
        "f13:-",
        "f14:板块名称",
        "f15:最高",
        "f16:最低",
        "f17:今开",
        "f18:昨收",
        "f20:总市值",
        "f21:流通市值",
        "f22:涨速",
        "f23:-",
        "f24:-",
        "f25:-",
        "f26:-",
        "f33:-",
        "f62:主力净流入",
        "f104:涨家数",
        "f105:跌家数",
        "f107:-",
        "f115:-",
        "f124:-",
        "f128:领涨股票名称",
        "f136:涨幅",
        "f140:领涨股票代码",
        "f141:-",
        "f152:-",
        "f207:领跌股票名称",
        "f208:领跌股票代码",
        "f209:-",
        "f222:跌幅",
    ]
    temp_df = temp_df[
        [
        "f12:板块代号",
        "f14:板块名称",
        "f3:涨跌幅",
        "f6:成交额",
        "f8:换手率",
        "f9:市盈率",
        "f20:总市值",
        "f21:流通市值",
        "f62:主力净流入",
        "f104:涨家数",
        "f105:跌家数",
        "f128:领涨股票名称",
        "f140:领涨股票代码",
        "f136:涨幅",
        "f207:领跌股票名称",
        "f208:领跌股票代码",
        "f222:跌幅",
        ]
    ]
    return temp_df


if __name__ == "__main__":
    industry_list = stock_industry_list()
    print(industry_list)
    detail = stock_industry_detail()
    print(detail)
