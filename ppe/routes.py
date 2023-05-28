from flask import render_template, Flask, request, jsonify
from datetime import datetime, timedelta
from ppe import app
from ppe.models import Market, Treasury_yields, Tips_yields

@app.route("/", methods=['GET', 'POST'])
def homepage():

    market = Market.query.order_by(Market.date.desc()).first()
    last_update_date = market.date.strftime("%Y-%m-%d")
    market_price = market.sp500_close
    market_cash_flows = market.dps_ttm
    market_growth_rate= market.growth_rate


    #compute latet BEI
    treasury_yields = Treasury_yields.query.filter_by(date=last_update_date).first()
    _10y_treasury = treasury_yields._10y
    tips_yields = Tips_yields.query.filter_by(date=last_update_date).first()
    _10y_tips = tips_yields._10y
    _10y_bei = ((1+ (_10y_treasury / 100) ) / (1+ (_10y_tips / 100 )) - 1) * 100



    market_minus_7 = None
    treasury_yields_minus_7 = None
    tips_yields_minus_7 = None
    week = 7
    while(market_minus_7 == None) or (treasury_yields_minus_7 == None) or (tips_yields_minus_7 == None):
        last_update_date_minus_7 = (datetime.strptime(last_update_date, "%Y-%m-%d") - timedelta(days=week)).strftime("%Y-%m-%d")
        market_minus_7 = Market.query.filter_by(date=last_update_date_minus_7).first()
        treasury_yields_minus_7 = Treasury_yields.query.filter_by(date=last_update_date_minus_7).first()
        tips_yields_minus_7 = Tips_yields.query.filter_by(date=last_update_date_minus_7).first()
        week = week + 1
    market_price_minus_7 = market_minus_7.sp500_close
    market_cash_flows_minus_7 = market_minus_7.dps_ttm
    market_growth_rate_minus_7 = market_minus_7.growth_rate
    _10y_treasury_minus_7 = treasury_yields_minus_7._10y
    _10y_tips_minus_7 = tips_yields_minus_7._10y
    _10y_bei_minus_7 = ((1+ (_10y_treasury_minus_7 / 100) ) / (1+ (_10y_tips_minus_7 / 100 )) - 1) * 100

    market_minus_30 = None
    treasury_yields_minus_30 = None
    tips_yields_minus_30 = None
    month = 30
    while(market_minus_30 == None) or (treasury_yields_minus_30 == None) or (tips_yields_minus_30 == None):
        last_update_date_minus_30 = (datetime.strptime(last_update_date, "%Y-%m-%d") - timedelta(days=month)).strftime("%Y-%m-%d")
        market_minus_30 = Market.query.filter_by(date=last_update_date_minus_30).first()
        treasury_yields_minus_30 = Treasury_yields.query.filter_by(date=last_update_date_minus_30).first()
        tips_yields_minus_30 = Tips_yields.query.filter_by(date=last_update_date_minus_30).first()
        month = month + 1
    market_price_minus_30 = market_minus_30.sp500_close
    market_cash_flows_minus_30 = market_minus_30.dps_ttm
    market_growth_rate_minus_30 = market_minus_30.growth_rate
    _10y_treasury_minus_30 = treasury_yields_minus_30._10y
    _10y_tips_minus_30 = tips_yields_minus_30._10y
    _10y_bei_minus_30 = ((1+ (_10y_treasury_minus_30 / 100) ) / (1+ (_10y_tips_minus_30 / 100 )) - 1) * 100

    market_minus_365 = None
    treasury_yields_minus_365 = None
    tips_yields_minus_365 = None
    year = 365
    while(market_minus_365 == None) or (treasury_yields_minus_365 == None) or (tips_yields_minus_365 == None):
        last_update_date_minus_365 = (datetime.strptime(last_update_date, "%Y-%m-%d") - timedelta(days=year)).strftime("%Y-%m-%d")
        market_minus_365 = Market.query.filter_by(date=last_update_date_minus_365).first()
        treasury_yields_minus_365 = Treasury_yields.query.filter_by(date=last_update_date_minus_365).first()
        tips_yields_minus_365 = Tips_yields.query.filter_by(date=last_update_date_minus_365).first()
        year = year + 1
    market_price_minus_365 = market_minus_365.sp500_close
    market_cash_flows_minus_365 = market_minus_365.dps_ttm
    market_growth_rate_minus_365 = market_minus_365.growth_rate
    _10y_treasury_minus_365 = treasury_yields_minus_365._10y
    _10y_tips_minus_365 = tips_yields_minus_365._10y
    _10y_bei_minus_365 = ((1+ (_10y_treasury_minus_365 / 100) ) / (1+ (_10y_tips_minus_365 / 100 )) - 1) * 100

    # price change calcs
    market_price_week_over_week = (market_price - market_price_minus_7) / market_price_minus_7
    market_price_month_over_month = (market_price - market_price_minus_30) / market_price_minus_30
    market_price_year_over_year = (market_price - market_price_minus_365) / market_price_minus_365

    # cash flow change calcs
    market_cash_flows_week_over_week = (market_cash_flows - market_cash_flows_minus_7) / market_cash_flows_minus_7
    market_cash_flows_month_over_month = (market_cash_flows - market_cash_flows_minus_30) / market_cash_flows_minus_30
    market_cash_flows_year_over_year = (market_cash_flows - market_cash_flows_minus_365) / market_cash_flows_minus_365

    # growth rate changes calc
    market_growth_rate_week_over_week = (market_growth_rate - market_growth_rate_minus_7) / market_growth_rate_minus_7
    market_growth_rate_month_over_month = (market_growth_rate - market_growth_rate_minus_30) / market_growth_rate_minus_30
    market_growth_rate_year_over_year = (market_growth_rate - market_growth_rate_minus_365) / market_growth_rate_minus_365

    # nominal rate changes calc
    _10y_treasury_week_over_week = (_10y_treasury - _10y_treasury_minus_7) / _10y_treasury_minus_7
    _10y_treasury_month_over_month = (_10y_treasury - _10y_treasury_minus_30) / _10y_treasury_minus_30
    _10y_treasury_year_over_year = (_10y_treasury - _10y_treasury_minus_365) / _10y_treasury_minus_365

    # real rate changes calc
    _10y_tips_week_over_week = (_10y_tips - _10y_tips_minus_7) / _10y_tips_minus_7
    _10y_tips_month_over_month = (_10y_tips - _10y_tips_minus_30) / _10y_tips_minus_30
    _10y_tips_year_over_year = (_10y_tips - _10y_tips_minus_365) / _10y_tips_minus_365

    _10y_bei_week_over_week = (_10y_bei - _10y_bei_minus_7) / _10y_bei_minus_7
    _10y_bei_month_over_month = (_10y_bei - _10y_bei_minus_30) / _10y_bei_minus_30
    _10y_bei_year_over_year = (_10y_bei - _10y_bei_minus_365) / _10y_bei_minus_365


    if request.method == 'POST':
        option = request.json['option']
        print(option)
        if option == "w/w":
            market_data = [
                {"Market":"Price", "Lvl": "$" + str(round(market.sp500_close,2)), "%∆":str(round(market_price_week_over_week,4))+"%"},
                {"Market":"Cash Flows", "Lvl": "$" + str(round(market.dps_ttm,2)), "%∆": str(round(market_cash_flows_week_over_week, 4))+"%"},
                {"Market":"Growth", "Lvl":str(round((100*market.growth_rate),2))+"%", "%∆":str(round(market_growth_rate_week_over_week, 4))+"%"},
                {"Market": "Inflation", "Lvl":str(round(_10y_bei,2))+"%", "%∆":str(round(_10y_bei_week_over_week , 4))+"%"},
                {"Market":"Last Updated", "Lvl":market.date.strftime("%Y-%m-%d"), "%∆":""}
            ]
        elif option == "m/m":
            market_data = [
                {"Market":"Price", "Lvl": "$" + str(round(market.sp500_close,2)), "%∆":str(round(market_price_month_over_month,4))+"%"},
                {"Market":"Cash Flows", "Lvl": "$" + str(round(market.dps_ttm,2)), "%∆": str(round(market_cash_flows_month_over_month, 4))+"%"},
                {"Market":"Growth", "Lvl":str(round((100*market.growth_rate),2))+"%", "%∆":str(round(market_growth_rate_month_over_month, 4))+"%"},
                {"Market": "Inflation", "Lvl":str(round(_10y_bei,2))+"%", "%∆":str(round(_10y_bei_month_over_month , 4))+"%"},
                {"Market":"Last Updated", "Lvl":market.date.strftime("%Y-%m-%d"), "%∆":""}
            ]
            print(market_data)
        elif option == "y/y":
            market_data = [
                {"Market":"Price", "Lvl": "$" + str(round(market.sp500_close,2)), "%∆":str(round(market_price_year_over_year,4))+"%"},
                {"Market":"Cash Flows", "Lvl": "$" + str(round(market.dps_ttm,2)), "%∆": str(round(market_cash_flows_year_over_year, 4))+"%"},
                {"Market":"Growth", "Lvl":str(round((100*market.growth_rate),2))+"%", "%∆":str(round(market_growth_rate_year_over_year, 4))+"%"},
                {"Market": "Inflation", "Lvl":str(round(_10y_bei, 2))+"%", "%∆":str(round(_10y_bei_year_over_year, 4))+"%"},
                {"Market":"Last Updated", "Lvl":market.date.strftime("%Y-%m-%d"), "%∆":""}
            ]
        return jsonify({"rows": market_data})
    else:
        market_data = [
            {"Market":"Price", "Lvl": "$" + str(round(market.sp500_close,2)), "%∆":str(round(market_price_week_over_week,4))+"%"},
            {"Market":"Cash Flows", "Lvl": "$" + str(round(market.dps_ttm,2)), "%∆": str(round(market_cash_flows_week_over_week, 4))+"%"},
            {"Market":"Growth", "Lvl":str(round((100*market.growth_rate),2))+"%", "%∆":str(round(market_growth_rate_week_over_week, 4))+"%"},
            {"Market": "Inflation", "Lvl":str(round(_10y_bei, 2))+"%", "%∆":str(round(_10y_bei_week_over_week , 4))+"%"},
            {"Market":"Last Updated", "Lvl":market.date.strftime("%Y-%m-%d"), "%∆":""}
        ]
        return render_template('index.html', rows=market_data)




#
# @app.route("/docs")
# def docs():
#     return render_template("index.html", title="docs page")
#
# @app.route("/about")
# def about():
#     return render_template("index.html", title="about page")
