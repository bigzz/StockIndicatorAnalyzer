import baostock as bs
import pandas as pd
import datetime
from sqlalchemy import create_engine
import common


def prepare_fetch_data():
    # 获取当前年/季度
    current_date = datetime.date.today().strftime('%Y-%m-%d')
    current_year = datetime.date.today().year
    current_month = datetime.date.today().month
    current_quarter = (current_month - 1) // 3 + 1

    hour = datetime.datetime.now().hour
    # baostock data maybe not update right after the trade market close.
    if hour < 20:
        dd = datetime.date.today() + datetime.timedelta(-1)
        current_date = dd.strftime('%Y-%m-%d')

    # 计算最后一个交易日时间
    start_date = "2006-01-01"
    data_list = []
    bs.login()
    rs = bs.query_trade_dates(start_date=start_date, end_date=current_date)
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    bs.logout()
    lasttradedate = current_date
    for i in range(len(data_list) - 1, -1, -1):
        if data_list[i][1] == '1':
            lasttradedate = data_list[i][0]
            break

    print('start query day:' + start_date + ', last query data:' + lasttradedate)

    bs.login()
    stock_rs = bs.query_all_stock(day=lasttradedate)
    stock_df = stock_rs.get_data()
    bs.logout()

    return stock_df, current_year, current_quarter


# 季频盈利能力 query_profit_data
"""
参数名称    	参数描述	                算法说明
code	    证券代码	
pubDate	    公司发布财报的日期	
statDate	财报统计的季度的最后一天
roeAvg	净资产收益率(平均)(%)	        归属母公司股东净利润/[(期初归属母公司股东的权益+期末归属母公司股东的权益)/2]*100%
npMargin	销售净利率(%)	        净利润/营业收入*100%
gpMargin	销售毛利率(%)	        毛利/营业收入*100%=(营业收入-营业成本)/营业收入*100%
netProfit	净利润(元)	
epsTTM	    每股收益	                归属母公司股东的净利润TTM/最新总股本
MBRevenue	主营营业收入(元)	
totalShare	总股本	
liqaShare	流通股本	
"""


def fetch_profit_data(stocklist, start_year=2007, current_year=2020, current_quarter=4):
    bs.login()
    db_conn = create_engine(common.db_path_sqlalchemy)
    for code in stocklist["code"]:
        profit_list = []
        if code.startswith("sh.6") | code.startswith("sz.00") | code.startswith("sz.300"):
            for year in range(start_year, current_year + 1):
                start_quarter = 1
                if year < current_year:
                    end_quarter = 4
                else:
                    end_quarter = current_quarter - 1
                for quarter in range(start_quarter, end_quarter + 1):
                    # 查询季频估值指标盈利能力
                    print(code + ' profit: ' + year.__str__() + 'Q' + quarter.__str__())
                    rs_profit = bs.query_profit_data(code=code, year=year, quarter=quarter)
                    while (rs_profit.error_code == '0') & rs_profit.next():
                        profit_list.append([year, quarter] + rs_profit.get_row_data())

            if len(profit_list) > 0:
                db_conn.execute(r'''INSERT OR REPLACE INTO stock_profit_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                                profit_list)
            print(code + ' fetch profit data finish and write database')

    bs.logout()


# 季频营运能力 query_operation_data
"""
参数名称	    参数描述    	                    算法说明
code	    证券代码	
pubDate	    公司发布财报的日期	
statDate	财报统计的季度的最后一天	
NRTurnRatio	应收账款周转率(次)	            营业收入/[(期初应收票据及应收账款净额+期末应收票据及应收账款净额)/2]
NRTurnDays	应收账款周转天数(天)	            季报天数/应收账款周转率(一季报：90天，中报：180天，三季报：270天，年报：360天)
INVTurnRatio 存货周转率(次)	                营业成本/[(期初存货净额+期末存货净额)/2]
INVTurnDays	存货周转天数(天)	                季报天数/存货周转率(一季报：90天，中报：180天，三季报：270天，年报：360天)
CATurnRatio	流动资产周转率(次)	            营业总收入/[(期初流动资产+期末流动资产)/2]
AssetTurnRatio	总资产周转率	                营业总收入/[(期初资产总额+期末资产总额)/2]
"""


def fetch_operation_data(stocklist, start_year=2007, current_year=2020, current_quarter=4):
    bs.login()
    db_conn = create_engine(common.db_path_sqlalchemy)
    for code in stocklist["code"]:
        operation_list = []
        if code.startswith("sh.6") | code.startswith("sz.00") | code.startswith("sz.300"):
            for year in range(start_year, current_year + 1):
                start_quarter = 1
                if year < current_year:
                    end_quarter = 4
                else:
                    end_quarter = current_quarter - 1
                for quarter in range(start_quarter, end_quarter + 1):
                    # 查询季频估值指标盈利能力
                    print(code + ' operation: ' + year.__str__() + 'Q' + quarter.__str__())
                    rs_operation = bs.query_operation_data(code=code, year=year, quarter=quarter)
                    while (rs_operation.error_code == '0') & rs_operation.next():
                        operation_list.append([year, quarter] + rs_operation.get_row_data())

            if len(operation_list) > 0:
                db_conn.execute(r'''INSERT OR REPLACE INTO stock_operation_data VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                                operation_list)
            print(code + ' fetch operation data finish and write database')

    bs.logout()


# 季频成长能力 query_growth_data
"""
参数名称	参数描述	                        算法说明
code	    证券代码	
pubDate	    公司发布财报的日期	
statDate	财报统计的季度的最后一天	
YOYEquity	净资产同比增长率	            (本期净资产-上年同期净资产)/上年同期净资产的绝对值*100%
YOYAsset	总资产同比增长率	            (本期总资产-上年同期总资产)/上年同期总资产的绝对值*100%
YOYNI	    净利润同比增长率	            (本期净利润-上年同期净利润)/上年同期净利润的绝对值*100%
YOYEPSBasic	基本每股收益同比增长率	        (本期基本每股收益-上年同期基本每股收益)/上年同期基本每股收益的绝对值*100%
YOYPNI	归属母公司股东净利润同比增长率	    (本期归属母公司股东净利润-上年同期归属母公司股东净利润)/上年同期归属母公司股东净利润的绝对值*100%
"""


def fetch_growth_data(stocklist, start_year=2007, current_year=2020, current_quarter=4):
    bs.login()
    db_conn = create_engine(common.db_path_sqlalchemy)
    for code in stocklist["code"]:
        growth_list = []
        if code.startswith("sh.6") | code.startswith("sz.00") | code.startswith("sz.300"):
            for year in range(start_year, current_year + 1):
                start_quarter = 1
                if year < current_year:
                    end_quarter = 4
                else:
                    end_quarter = current_quarter - 1
                for quarter in range(start_quarter, end_quarter + 1):
                    # 查询季频估值指标盈利能力
                    print(code + ' growth: ' + year.__str__() + 'Q' + quarter.__str__())
                    rs_growth = bs.query_growth_data(code=code, year=year, quarter=quarter)
                    while (rs_growth.error_code == '0') & rs_growth.next():
                        growth_list.append([year, quarter] + rs_growth.get_row_data())

            if len(growth_list) > 0:
                db_conn.execute(r'''INSERT OR REPLACE INTO stock_growth_data VALUES (?,?,?,?,?,?,?,?,?,?)''',
                                growth_list)
            print(code + ' fetch growth data finish and write database')

    bs.logout()


# 季频偿债能力 query_balance_data
"""
参数名称    	    参数描述    	            算法说明
code	        证券代码	
pubDate	        公司发布财报的日期	
statDate	    财报统计的季度的最后一天	
currentRatio	流动比率	                流动资产/流动负债
quickRatio	    速动比率	                (流动资产-存货净额)/流动负债
cashRatio	    现金比率	                (货币资金+交易性金融资产)/流动负债
YOYLiability	总负债同比增长率	        (本期总负债-上年同期总负债)/上年同期中负债的绝对值*100%
liabilityToAsset	资产负债率	        负债总额/资产总额
assetToEquity	权益乘数	                资产总额/股东权益总额=1/(1-资产负债率)
"""


def fetch_balance_data(stocklist, start_year=2007, current_year=2020, current_quarter=4):
    bs.login()
    db_conn = create_engine(common.db_path_sqlalchemy)
    for code in stocklist["code"]:
        balance_list = []
        if code.startswith("sh.6") | code.startswith("sz.00") | code.startswith("sz.300"):
            for year in range(start_year, current_year + 1):
                start_quarter = 1
                if year < current_year:
                    end_quarter = 4
                else:
                    end_quarter = current_quarter - 1
                for quarter in range(start_quarter, end_quarter + 1):
                    # 查询季频估值指标盈利能力
                    print(code + ' balance: ' + year.__str__() + 'Q' + quarter.__str__())
                    rs_balance = bs.query_balance_data(code=code, year=year, quarter=quarter)
                    while (rs_balance.error_code == '0') & rs_balance.next():
                        balance_list.append([year, quarter] + rs_balance.get_row_data())

            if len(balance_list) > 0:
                db_conn.execute(r'''INSERT OR REPLACE INTO stock_balance_data VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                                balance_list)
            print(code + ' fetch balance data finish and write database')

    bs.logout()


# 季频现金流量 query_cash_flow_data
"""
参数名称    	    参数描述    	            算法说明
code	    证券代码	
pubDate	    公司发布财报的日期	
statDate	财报统计的季度的最后一天
CAToAsset	流动资产除以总资产	
NCAToAsset	非流动资产除以总资产	
tangibleAssetToAsset	有形资产除以总资产	
ebitToInterest	已获利息倍数              息税前利润/利息费用
CFOToOR	经营活动产生的现金流量净额除以营业收入	
CFOToNP	经营性现金净流量除以净利润	
CFOToGr	经营性现金净流量除以营业总收入	
"""


def fetch_cash_flow_data(stocklist, start_year=2007, current_year=2020, current_quarter=4):
    bs.login()
    db_conn = create_engine(common.db_path_sqlalchemy)
    for code in stocklist["code"]:
        cash_flow_list = []
        if code.startswith("sh.6") | code.startswith("sz.00") | code.startswith("sz.300"):
            for year in range(start_year, current_year + 1):
                start_quarter = 1
                if year < current_year:
                    end_quarter = 4
                else:
                    end_quarter = current_quarter - 1
                for quarter in range(start_quarter, end_quarter + 1):
                    # 查询季频估值指标盈利能力
                    print(code + ' cash_flow: ' + year.__str__() + 'Q' + quarter.__str__())
                    rs_cash_flow = bs.query_cash_flow_data(code=code, year=year, quarter=quarter)
                    while (rs_cash_flow.error_code == '0') & rs_cash_flow.next():
                        cash_flow_list.append([year, quarter] + rs_cash_flow.get_row_data())

            if len(cash_flow_list) > 0:
                db_conn.execute(r'''INSERT OR REPLACE INTO stock_cash_flow_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                                cash_flow_list)
            print(code + ' fetch cash_flow data finish and write database')

    bs.logout()


# 季频杜邦指数 query_dupont_data
"""
参数名称	参数描述	
code	证券代码	
pubDate	公司发布财报的日期	
statDate	财报统计的季度的最后一天, 比如2017-03-31, 2017-06-30	
dupontROE	净资产收益率;归属母公司股东净利润/[(期初归属母公司股东的权益+期末归属母公司股东的权益)/2]*100%
dupontAssetStoEquity	权益乘数，反映企业财务杠杆效应强弱和财务风险;平均总资产/平均归属于母公司的股东权益
dupontAssetTurn	总资产周转率，反映企业资产管理效率的指标; 营业总收入/[(期初资产总额+期末资产总额)/2]
dupontPnitoni	归属母公司股东的净利润/净利润，反映母公司控股子公司百分比。如果企业追加投资，扩大持股比例，则本指标会增加。	
dupontNitogr	净利润/营业总收入，反映企业销售获利率	
dupontTaxBurden	净利润/利润总额，反映企业税负水平，该比值高则税负较低。净利润/利润总额=1-所得税/利润总额	
dupontIntburden	利润总额/息税前利润，反映企业利息负担，该比值高则税负较低。利润总额/息税前利润=1-利息费用/息税前利润
dupontEbittogr	息税前利润/营业总收入，反映企业经营利润率，是企业经营获得的可供全体投资人（股东和债权人）分配的盈利占企业全部营收收入的百分比
"""


def fetch_dupont_data(stocklist, start_year=2007, current_year=2020, current_quarter=4):
    bs.login()
    db_conn = create_engine(common.db_path_sqlalchemy)
    for code in stocklist["code"]:
        dupont_list = []
        if code.startswith("sh.6") | code.startswith("sz.00") | code.startswith("sz.300"):
            for year in range(start_year, current_year + 1):
                start_quarter = 1
                if year < current_year:
                    end_quarter = 4
                else:
                    end_quarter = current_quarter - 1
                for quarter in range(start_quarter, end_quarter + 1):
                    # 查询季频估值指标盈利能力
                    print(code + ' dupont: ' + year.__str__() + 'Q' + quarter.__str__())
                    rs_dupont = bs.query_dupont_data(code=code, year=year, quarter=quarter)
                    while (rs_dupont.error_code == '0') & rs_dupont.next():
                        dupont_list.append([year, quarter] + rs_dupont.get_row_data())

            if len(dupont_list) > 0:
                db_conn.execute(r'''INSERT OR REPLACE INTO stock_dupont_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                                dupont_list)
            print(code + ' fetch dupont data finish and write database')

    bs.logout()


# 季频公司业绩快报 query_performance_express_report
"""
参数名称	参数描述
code	证券代码
performanceExpPubDate	业绩快报披露日
performanceExpStatDate	业绩快报统计日期
performanceExpUpdateDate	业绩快报披露日(最新)
performanceExpressTotalAsset	业绩快报总资产
performanceExpressNetAsset	业绩快报净资产
performanceExpressEPSChgPct	业绩每股收益增长率
performanceExpressROEWa	业绩快报净资产收益率ROE-加权
performanceExpressEPSDiluted	业绩快报每股收益EPS-摊薄
performanceExpressGRYOY	业绩快报营业总收入同比
performanceExpressOPYOY	业绩快报营业利润同比
"""


def fetch_performance_express_report(stocklist, start_year=2007, current_year=2020, current_quarter=4):
    return


# 季频业绩预告 query_forcast_report
"""
参数名称	参数描述
code	证券代码
profitForcastExpPubDate	业绩预告发布日期
profitForcastExpStatDate	业绩预告统计日期
profitForcastType	业绩预告类型
profitForcastAbstract	业绩预告摘要
profitForcastChgPctUp	预告归属于母公司的净利润增长上限(%)
profitForcastChgPctDwn	预告归属于母公司的净利润增长下限(%)
"""


def fetch_forcast_report():
    return


if __name__ == '__main__':
    stock_df, current_year, current_quarter = prepare_fetch_data()

    start_year = 2007

    # fetch_profit_data(stocklist=stock_df, start_year=start_year, current_year=current_year, current_quarter=current_quarter)
    # fetch_operation_data(stocklist=stock_df, start_year=start_year, current_year=current_year, current_quarter=current_quarter)
    # fetch_growth_data(stocklist=stock_df, start_year=start_year, current_year=current_year, current_quarter=current_quarter)
    # fetch_balance_data(stocklist=stock_df, start_year=start_year, current_year=current_year, current_quarter=current_quarter)
    # fetch_cash_flow_data(stocklist=stock_df, start_year=start_year, current_year=current_year, current_quarter=current_quarter)
    fetch_dupont_data(stocklist=stock_df, start_year=start_year, current_year=current_year,
                      current_quarter=current_quarter)
