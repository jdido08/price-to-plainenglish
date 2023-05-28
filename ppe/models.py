from ppe import db

class Market(db.Model):
    __tablename__ = 'market_measures'
    date = db.Column(db.DateTime, primary_key=True)
    marketcap = db.Column(db.Float)
    dividends_ttm = db.Column(db.Float)
    non_gaap_earnings_ttm = db.Column(db.Float)
    sp500_close = db.Column(db.Float)
    dps_ttm = db.Column(db.Float)
    non_gaap_eps_ttm = db.Column(db.Float)
    payout_ratio = db.Column(db.Float)
    risk_free_rates = db.Column(db.String)
    risk_premium_rates = db.Column(db.String)
    growth_rate = db.Column(db.Float)

    # def __repr__(self):
    #     return f"Market('{self.date}', '{self.sp500_close}', '{self.growth_rate}')"

class Tips_yields(db.Model):
    __tablename__ = 'tips_yields'
    date = db.Column(db.DateTime, primary_key=True)
    _5y = db.Column(db.Float)
    _7y = db.Column(db.Float)
    _10y = db.Column(db.Float)
    _20y = db.Column(db.Float)
    _30y = db.Column(db.Float)

class Treasury_yields(db.Model):
    __tablename__ = 'treasury_yields'
    date = db.Column(db.DateTime, primary_key=True)
    _1y = db.Column(db.Float)
    _2y = db.Column(db.Float)
    _3y = db.Column(db.Float)
    _5y = db.Column(db.Float)
    _7y = db.Column(db.Float)
    _10y = db.Column(db.Float)
    _20y = db.Column(db.Float)
    _30y = db.column(db.Float)

# class Company(db.Model):
#     __tablename__ = 'company_measures'
#     ticker = db.Column(db.String)
#     name = db.Column(db.String)
#     sector = db.Column(db.String)
#     date = db.Column(db.DateTime)
#     close_price = db.Column(db.Float)
#     adjusted_price = db.Column(db.Float)
#     marketcap = db.Column(db.Float)
#     dps_ttm = db.Column(db.Float)
#     non_gaap_eps_ttm = db.Column(db.Float)
#     payout_ratio = db.Column(db.Float)
#     dividend_yield = db.Column(db.Float)
#     earnings_yield = db.Column(db.Float)
