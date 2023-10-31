import datetime
import pandas as pd
import os #to get file path
from google.cloud import secretmanager # Import the  Secret Manager client library.
import google_crc32c
import sqlalchemy as db #to save to db
import pandas_datareader as web # to get fred data
import requests
import time
import io
import json
from functions.calc_functions import calc_company_measures


### FUNCTIONS THAT SUPPORTING GRABBING RAW DATA FROM THE WORLD ###


################################################################################
######################## SUPPORTING INFRASTRUCTURE #############################
################################################################################

def get_secret(project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    #for local dev -- set google app credentials
    # google_application_credentials_file_path = os.path.dirname(os.path.abspath(__file__)) + "/mister-market-project-6e485429eb5e.json"
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_application_credentials_file_path

    #link: https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets
    #follow instruction here to run locally: https://cloud.google.com/docs/authentication/production#create-service-account-gcloud



    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Verify payload checksum.
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        print("Data corruption detected.")
        return response
    else:
        payload = response.payload.data.decode("UTF-8")
        return payload

alpha_vantage_api_key = get_secret("mister-market-project", "alphavantage_special_key", "1")
connection_name = "mister-market-project:us-central1:mister-market-db"
driver_name = 'mysql+pymysql'
query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})
db_user = "root"
db_name = "raw_data"
db_password = get_secret("mister-market-project", "db_password", "1")
# db_hostname = get_secret("mister-market-project", "db_hostname", "1")                  #for local dev
# db_port = "3306"                                                                       #for local dev
# db_ssl_ca_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/server-ca.pem'     #for local dev
# db_ssl_cert_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/client-cert.pem' #for local dev
# db_ssl_key_path = os.path.dirname(os.path.abspath(__file__)) + '/ssl/client-key.pem'   #for local dev

engine = db.create_engine(
  db.engine.url.URL.create(
    drivername=driver_name,
    username=db_user,
    password=db_password,
    database=db_name,
    query=query_string,                  #for cloud function
    # host=db_hostname,  # e.g. "127.0.0.1" #for local dev
    # port=db_port,  # e.g. 3306            #for local dev
  ),
  pool_size=5,
  max_overflow=2,
  pool_timeout=30,
  pool_recycle=1800
  # ,                                   #for local dev
  # connect_args = {                    #for local dev
  #     'ssl_ca': db_ssl_ca_path ,      #for local dev
  #     'ssl_cert': db_ssl_cert_path,   #for local dev
  #     'ssl_key': db_ssl_key_path      #for local dev
  #     }                               #for loval dev
)

connection = engine.connect()
metadata = db.MetaData()


def read_write_df_sql(function, df=None, table_name=None, if_exists=None):
    if(function == "read"):
        try:
            print("SUCCESS: Read ", table_name, "!")
            return pd.read_sql_table(table_name, engine)
        except Exception as e:
            print("ERROR: Can't read ", table_name ,"! DETAILS:", e)
    elif(function == "write"):
        try:
            df.to_sql(table_name, engine, if_exists=if_exists, index=False, chunksize=500)
            print("SUCCESS: ", table_name, " updated!")
        except Exception as e:
            print("ERROR: Can't update ", table_name ,"! DETAILS:", e)


#fundamental data
def get_alpha_vantage_fundamental_data(function, ticker, json_object, alpha_vantage_api_key):
    base_url = 'https://www.alphavantage.co/query?'
    params = {'function': function,
             'symbol': ticker,
             "datatype": 'json',
             'apikey': alpha_vantage_api_key}
    response = requests.get(base_url, params=params)
    response = response.json()
    if(json_object != None):
        response = response[json_object]
    df = pd.json_normalize(response)
    df['ticker'] = ticker #create column with ticker
    return df

#time series data
def get_alpha_vantage_stock_time_series_data(function, ticker, outputsize, alpha_vantage_api_key):
    base_url = 'https://www.alphavantage.co/query?'
    params = {'function': function,
             'symbol': ticker,
             "datatype": 'csv',
             'outputsize': outputsize, #output size options: full, compact
             'apikey': alpha_vantage_api_key}
    response = requests.get(base_url, params=params)
    df = pd.read_csv(io.StringIO(response.text))
    df['ticker'] = ticker #create column with ticker
    df['timestamp'] = pd.to_datetime(df['timestamp'] , format="%Y-%m-%d", utc=True) #change format type
    df = df.rename(columns={'timestamp':'date'}) #standardize naming convention
    df = df.sort_values(by='date', ascending=True)

    return df

# secondary functions -- for convience
def get_av_quarterly_earnings(ticker, alpha_vantage_api_key):
    df = get_alpha_vantage_fundamental_data('EARNINGS', ticker, 'quarterlyEarnings', alpha_vantage_api_key)
    df = df.rename(columns = {'reportedDate':'date'}) #standardize naming convention
    df['date'] = pd.to_datetime(df['date'] , format="%Y-%m-%d", utc=True) #change format time
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'] , format="%Y-%m-%d", utc=True) #change format time
    df = df.sort_values(by='date', ascending=True)
    return df

def get_av_quarterly_income_statements(ticker, alpha_vantage_api_key):
    df = get_alpha_vantage_fundamental_data('INCOME_STATEMENT', ticker, 'quarterlyReports', alpha_vantage_api_key)
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'] , format="%Y-%m-%d", utc=True) #change format time
    df = df.sort_values(by='fiscalDateEnding', ascending=True)
    return df

def get_av_quarterly_balance_sheets(ticker, alpha_vantage_api_key):
    df = get_alpha_vantage_fundamental_data('BALANCE_SHEET', ticker, 'quarterlyReports', alpha_vantage_api_key)
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'] , format="%Y-%m-%d", utc=True) #change format time
    df.loc[df['commonStockSharesOutstanding'] == 'None', 'commonStockSharesOutstanding'] = None
    df = df.sort_values(by='fiscalDateEnding', ascending=True)
    df = df[['fiscalDateEnding','reportedCurrency','totalAssets','totalLiabilities','totalShareholderEquity','commonStock','commonStockSharesOutstanding','ticker']]
    return df

def get_av_quarterly_cash_flow_statements(ticker, alpha_vantage_api_key):
    df = get_alpha_vantage_fundamental_data('CASH_FLOW', ticker, 'quarterlyReports', alpha_vantage_api_key)
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'] , format="%Y-%m-%d", utc=True) #change format time
    df = df.sort_values(by='fiscalDateEnding', ascending=True)
    return df

################################################################################
###############################  SP500 DATA ####################################
################################################################################

#this function is only meant to be run locally
def reset_sp500_constituents_history():
    start_date = datetime.datetime(1996, 1, 2).strftime("%Y-%m-%d") #start date from https://github.com/fja05680/sp500
    end_date = datetime.datetime(2022, 7, 23).strftime("%Y-%m-%d") #this potentially needs to be updated if the underlying csv updates
    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='date')
    date_df['date'] = pd.to_datetime(date_df['date'], utc=True) #formatting


    historical_sp_500_constituents_csv_path = os.path.dirname(os.path.abspath(__file__)) + '/data/sp500_constituents_history_through_7_23_2022.csv'
    df = pd.read_csv(historical_sp_500_constituents_csv_path)
    df['date'] = pd.to_datetime(df['date'], utc=True) #formatting

    df = pd.merge(date_df, df, on='date', how='left')
    df = df.sort_values(by='date')
    df = df.fillna(method='ffill')

    read_write_df_sql(function = "write", df=df, table_name = "sp500_constituents", if_exists = "replace")

#supporting function
def get_sp500_constituents_today():
    data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S&P_500_companies')

    # Get current S&P table and set header column
    df = data[0].iloc[1:,[0]] #get certain columns in first table on wiki page
    columns = ['ticker']
    df.columns = columns

    tickers_list = list(set(df['ticker'])) #remove any accidnetal duplicates
    tickers_list = sorted(tickers_list)
    tickers_str = ','.join(tickers_list)

    #reformat
    date = datetime.datetime.combine(datetime.date.today(), datetime.time(), tzinfo=datetime.timezone.utc)

    df = pd.DataFrame(columns= {'date', 'tickers'}) #crate empty dataframe
    df = df.append({'date':date, 'tickers':tickers_str}, ignore_index = True)
    df['date'] = pd.to_datetime(df['date'], utc=True)

    return df

# this is what you want to run daily
def update_sp500_constituents():
    sp500_constituents_df = read_write_df_sql(function = "read", table_name = "sp500_constituents") #read in current sp500_constituents table
    sp500_constituents_df['date'] = pd.to_datetime(sp500_constituents_df['date'], utc=True)

    #get dates from end of existing sp500_constituents dataframe and today
    start_date = sp500_constituents_df['date'].max() + datetime.timedelta(days=1) #find first date which there is no data for
    end_date = datetime.datetime.combine(datetime.date.today(), datetime.time(), tzinfo=start_date.tzinfo)
    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='date') #list of dates from last date of current table and today

    #get sp_500_constituents for today
    sp500_constituents_today_df = get_sp500_constituents_today()

    #append sp500_constituents dataframe with constituent from today
    sp500_constituents_today_df = pd.merge(date_df, sp500_constituents_today_df, on='date', how='left')
    sp500_constituents_df = pd.concat([sp500_constituents_df, sp500_constituents_today_df], ignore_index=True)

    #format
    sp500_constituents_df = sp500_constituents_df.sort_values(by='date') #order by dates
    sp500_constituents_df = sp500_constituents_df.drop_duplicates(subset=['date']) #drop any duplicates dates'; this unccessary now
    sp500_constituents_df = sp500_constituents_df.fillna(method='ffill') #fill forward for any misisng dataframes

    read_write_df_sql(function = "write", df = sp500_constituents_df, table_name = "sp500_constituents", if_exists = "replace")

def update_sp500_prices():
     sp500_fred_start_date = datetime.datetime(2013, 1, 1).strftime("%Y-%m-%d") #FRED provides last 10 years of data; will need to loop back at this at some point
     today = datetime.date.today().strftime("%Y-%m-%d") #get most up to date data on FRED website
     df= web.DataReader(['SP500'], 'fred', sp500_fred_start_date, today)

     #reformat dataframe
     df = df.reset_index()
     df = df.rename(columns={"DATE":"date", "SP500":"sp500_close"})
     df['sp500_close'] = pd.to_numeric(df['sp500_close'], errors='coerce')
     df['date'] =  pd.to_datetime(df['date'], format='%Y-%m-%d', utc=True)

     read_write_df_sql(function = "write", df = df, table_name = "sp500_prices", if_exists = "replace")




################################################################################
###############################  TREASURY DATA #################################
################################################################################

def update_treasury_yields():
    treasury_fred_start_date = datetime.datetime(2000, 1, 1).strftime("%Y-%m-%d") #rates go back to 1962 but just starting with 2000
    today = datetime.date.today().strftime("%Y-%m-%d") #get most up to date data on FRED website
    df = web.DataReader(['DGS1', 'DGS2','DGS3','DGS5','DGS7','DGS10','DGS20','DGS30'], 'fred',treasury_fred_start_date, today)

    #reformat dataframe
    df = df.reset_index()
    df = df.rename(columns={
        "DATE":"date",
        "DGS1":"_1y",
        "DGS2":"_2y",
        "DGS3":"_3y",
        "DGS5":"_5y",
        "DGS7":"_7y",
        "DGS10":"_10y",
        "DGS20":"_20y",
        "DGS30":"_30y"})
    df = df.sort_values(by='date')
    column_names = ['_1y','_2y','_3y','_5y','_7y','_10y','_20y','_30y']
    df[column_names] = df[column_names].apply(pd.to_numeric, errors='coerce')
    df['date'] =  pd.to_datetime(df['date'], utc=True)

    read_write_df_sql(function = "write", df = df, table_name = "treasury_yields", if_exists = "replace")

def update_tips_yields():
    tips_fred_start_date = datetime.datetime(2003, 1, 2).strftime("%Y-%m-%d") #start date on FRED wesbite
    today = datetime.date.today().strftime("%Y-%m-%d") #get most up to date data on FRED website
    df = web.DataReader(['DFII5','DFII7','DFII10','DFII20','DFII30'], 'fred',tips_fred_start_date, today)

    #reformat dataframe
    df = df.reset_index()
    df = df.rename(columns={
        "DATE":"date",
        "DFII5":"_5y",
        "DFII7":"_7y",
        "DFII10":"_10y",
        "DFII20":"_20y",
        "DFII30":"_30y"})
    df = df.sort_values(by='date')
    column_names = ['_5y','_7y','_10y','_20y','_30y']
    df[column_names] = df[column_names].apply(pd.to_numeric, errors='coerce')
    df['date'] =  pd.to_datetime(df['date'], format='%Y-%m-%d', utc=True)

    read_write_df_sql(function = "write", df = df, table_name = "tips_yields", if_exists = "replace")

################################################################################
########################### COMPANY FUNDAMENTALS DATA ##########################
################################################################################



def reset_company_status():
    #create table that will be used to facilate daily update of all underlying company data

    #for today our universe of companies is the sp500
    df = read_write_df_sql(function = "read", table_name = "sp500_constituents") #read in current sp500_constituents table

    #FOR TESTING
    df = df.loc[df['date'] >= '2021-01-01'] #filter for only the dates you want; will delete later

    #get unique tickers
    tickers = df['tickers'].str.cat(sep=',') #add all tickers into one string
    tickers = tickers.split(",") #create list of all SP_500_companies
    tickers = list(set(tickers)) #find all unique companies
    tickers = sorted(tickers) #sort company list alphabetically

    #convert list of unique tickers into dataframe
    df = pd.DataFrame (tickers, columns = ['ticker'])

    #add columns to dataframe
    df["update_status"] = "NOT STARTED"

    df["stock_update_status"] = "NOT STARTED"
    df["last_stock_update_date"] = "#N/A"

    df["adjusted_stock_update_status"] = "NOT STARTED"
    df["last_adjusted_stock_update_date"] = "N/A"

    df["earnings_update_status"] = "NOT STARTED"
    df["last_earnings_update_date"] = "N/A"

    df["balance_sheet_update_status"] = "NOT STARTED"
    df["last_balance_sheet_update_date"] = "N/A"

    df["calc_company_measures_status"] = "NOT STARTED"
    df["calc_company_measures_update_date"] = "#N/A"


    #FOR TESTING only
    #df = df.head(5)

    #create table that will keep track of daily updates for companies
    read_write_df_sql(function = "write", df = df, table_name = "company_data_status", if_exists = "replace")

    #replace existing company stock tables w/ empty table
    company_stock_df = pd.DataFrame(columns = ['date', 'open','high','low', 'close', 'volume', 'ticker'])
    #read_write_df_sql(function = "write", df = company_stock_df, table_name = "company_stock", if_exists = "replace")
    company_stock_df.to_sql('company_stock', engine, if_exists='replace', index=False, chunksize=500,
        dtype={'open':Float(),
        'high':Float(),
        'low':Float(),
        'close':Float(),
        'volume':Float()})


    #replace existing adjusted company stock tables w/ empty table
    company_adjusted_stock_df.to_sql(columns = ['date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume', 'dividend_amount', 'split_coefficient', 'ticker'])
    #read_write_df_sql(function = "write", df = company_adjusted_stock_df, table_name = "company_adjusted_stock", if_exists = "replace")
    company_adjusted_stock_df.to_sql('company_adjusted_stock', engine, if_exists='replace', index=False, chunksize=500,
        dtype={'open':Float(),
        'high':Float(),
        'low':Float(),
        'close':Float(),
        'adjusted_close':Float(),
        'volume':Float(),
        'dividend_amount':Float(),
        'split_coefficient':Float()})

    #replace existing company earnings tables w/ empty table
    company_earnings_df = pd.DataFrame(columns = ['fiscalDateEnding', 'date', 'reportedEPS', 'estimatedEPS', 'surprise', 'surprisePercentage', 'ticker'])
    #read_write_df_sql(function = "write", df = company_earnings_df, table_name = "company_earnings", if_exists = "replace")
    company_earnings_df.to_sql('company_earnings', engine, if_exists='replace', index=False, chunksize=500,
        dtype={'reportedEPS':Float(),
        'estimatedEPS':Float(),
        'surprise':Float(),
        'surprisePercentage':Float()})


    #replace existing company earnings tables w/ empty table
    company_balance_sheet_df = pd.DataFrame(columns = ['fiscalDateEnding','reportedCurrency','totalAssets','totalLiabilities','totalShareholderEquity','commonStock','commonStockSharesOutstanding','cash','ticker'])
    #read_write_df_sql(function = "write", df = company_balance_sheet_df, table_name = "company_balance_sheet", if_exists = "replace")
    company_balance_sheet_df.to_sql('company_balance_sheet', engine, if_exists='replace', index=False, chunksize=500,
        dtype={'totalAssets':Float(),
        'totalLiabilities':Float(),
        'totalShareholderEquity':Float(),
        'commonStock':Float(),
        'commonStockSharesOutstanding':Float(),
        'cash':Float()})

    #replace existing company earnings tables w/ empty table
    company_measures_df = pd.DataFrame(columns = ['ticker','date','close_price','adjusted_close_price','shares_outstanding','dps_ttm','dividends_ttm', 'non_gaap_eps_ttm','non_gaap_earnings_ttm','sector','industry','exchange','currency','name','marketcap','payout_ratio','dividend_yield','earnings_yield'])
    #read_write_df_sql(function = "write", df = company_measures_df, table_name = "company_measures", if_exists = "replace")
    company_measures_df.to_sql('company_measures', engine, if_exists='replace', index=False, chunksize=500,
        dtype={'close_price':Float(),
        'adjusted_close_price':Float(),
        'shares_outstanding':Float(),
        'dps_ttm':Float(),
        'dividends_ttm':Float(),
        'non_gaap_eps_ttm':Float(),
        'non_gaap_earnings_ttm':Float(),
        'marketcap':Float(),
        'payout_ratio':Float(),
        'dividend_yield':Float(),
        'earnings_yield':Float()})



def update_company_stock_data(ticker):

    #connect to company_data_status table
    company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)

    try:

        #for ticker get company data and append it to company_stock table
        company_stock_df = get_alpha_vantage_stock_time_series_data('TIME_SERIES_DAILY', ticker, 'full', alpha_vantage_api_key)
        company_stock_df.to_sql('company_stock', engine, if_exists='append', index=False, chunksize=500)

        #set status of query
        update_status_query = db.update(company_data_status).values(stock_update_status = "COMPLETE").where(company_data_status.columns.ticker == ticker)
        #logging.info('SUCCESS: %s stock data updated', ticker)
        print('SUCCESS: ', ticker, ' stock data updated!')

    except Exception as e:
        #set status of query
        update_status_query = db.update(company_data_status).values(stock_update_status = "ERROR").where(company_data_status.columns.ticker == ticker)
        #logging.error('ERROR: Cant update company stock data -- Error: ', e)
        print('ERROR: Cant update ', ticker, ' stock data! DETAILS: ', e)


    #update status
    connection.execute(update_status_query)
    now = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    update_date_query = db.update(company_data_status).values(last_stock_update_date = now).where(company_data_status.columns.ticker == ticker)
    connection.execute(update_date_query)
    time.sleep(15)


#update company adjusted stock data
def update_company_adjusted_stock_data(ticker):
    #connect to company_data_status table
    company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)

    try:

        # *** NOTE PREMIUM API KEY TO ACCESS ADJUSTED STOCK DATA ***
        company_adjusted_stock_df = get_alpha_vantage_stock_time_series_data('TIME_SERIES_DAILY_ADJUSTED', ticker, 'full', alpha_vantage_api_key)
        company_adjusted_stock_df.to_sql('company_adjusted_stock', engine, if_exists='append', index=False, chunksize=500)

        #set status update
        company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)
        update_status_query = db.update(company_data_status).values(adjusted_stock_update_status = "COMPLETE").where(company_data_status.columns.ticker == ticker)
        #logging.info('SUCCESS: %s adjusted stock data updated', ticker)
        print('SUCCESS: ', ticker, ' adjusted stock data updated!')

    except Exception as e:
        update_status_query = db.update(company_data_status).values(adjusted_stock_update_status = "ERROR").where(company_data_status.columns.ticker == ticker)
        #logging.error('Cant update company adjusted stock data -- Error: ', e)
        print('ERROR: Cant update ', ticker, 'company adjust stock data! DETAILS: ', e)


    #update status
    connection.execute(update_status_query)
    now = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    update_date_query = db.update(company_data_status).values(last_adjusted_stock_update_date = now).where(company_data_status.columns.ticker == ticker)
    connection.execute(update_date_query)
    time.sleep(15)

#update company earnings data
def update_company_earnings_data(ticker):
    #connect to company_data_status table
    company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)

    try:
        company_earnings_df = get_av_quarterly_earnings(ticker, alpha_vantage_api_key)
        company_earnings_df.to_sql('company_earnings', engine, if_exists='append', index=False, chunksize=500)

        #connect to company_data_status table
        company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)
        update_status_query = db.update(company_data_status).values(earnings_update_status = "COMPLETE").where(company_data_status.columns.ticker == ticker)
        #logging.info('SUCCESS: %s earnings data updated', ticker)
        print('SUCCESS: ', ticker, ' earnings data updated!')

    except Exception as e:
        update_status_query = db.update(company_data_status).values(earnings_update_status = "ERROR").where(company_data_status.columns.ticker == ticker)
        #logging.error('Cant update company earnings data -- Error: ', e)
        print('ERROR: Cant update ', ticker, ' company earnings data! DETAILS: ', e)


    #make update
    connection.execute(update_status_query)
    now = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    update_date_query = db.update(company_data_status).values(last_earnings_update_date = now).where(company_data_status.columns.ticker == ticker)
    connection.execute(update_date_query)
    time.sleep(15)

#update company
def update_company_balance_sheet_data(ticker):

    #connect to company_data_status table
    company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)

    try:
        company_balance_sheet_df = get_av_quarterly_balance_sheets(ticker, alpha_vantage_api_key)
        company_balance_sheet_df.to_sql('company_balance_sheet', engine, if_exists='append', index=False, chunksize=500)

        #connect to company_data_status table
        company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)
        update_status_query = db.update(company_data_status).values(balance_sheet_update_status = "COMPLETE").where(company_data_status.columns.ticker == ticker)
        #logging.info('SUCCESS: %s balance sheet data updated', ticker)
        print('SUCCESS: ', ticker, ' balance sheet data updated!')

    except Exception as e:
        update_status_query = db.update(company_data_status).values(balance_sheet_update_status = "ERROR").where(company_data_status.columns.ticker == ticker)
        #logging.error('Cant update company balance sheet data-- Error: ', e)
        print('ERROR: Cant update ',ticker, ' balance sheet data! DETAILS: ', e)

    #make update
    connection.execute(update_status_query)
    now = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    update_date_query = db.update(company_data_status).values(last_balance_sheet_update_date = now).where(company_data_status.columns.ticker == ticker)
    connection.execute(update_date_query)
    time.sleep(15)


def update_company_data():
    #https://towardsdatascience.com/sql-on-the-cloud-with-python-c08a30807661


    #connect to company_data_status table
    company_data_status = db.Table('company_data_status', metadata, autoload=True, autoload_with=engine)

    #query for first ticker that's not started
    find_ticker_query = db.select([company_data_status]).where(company_data_status.columns.update_status == "NOT STARTED")
    result = connection.execute(find_ticker_query).first()

    if(result):
        ticker = result['ticker']

        #update update_status for that specific ticker at hand
        update_status_query = db.update(company_data_status).values(update_status = "IN PROGRESS").where(company_data_status.columns.ticker == ticker)
        connection.execute(update_status_query)

        #get company data
        update_company_stock_data(ticker)
        update_company_adjusted_stock_data(ticker)
        update_company_earnings_data(ticker)
        update_company_balance_sheet_data(ticker)

        #calc company measures
        calc_company_measures(ticker)

        #update update_status for that specific ticker at hand
        update_status_query = db.update(company_data_status).values(update_status = "COMPLETE").where(company_data_status.columns.ticker == ticker)
        connection.execute(update_status_query)

        #### MOVING ON FROM THIS NOW - MAYBE WILL TRY AGAIN IN THE FUTURE ####
        # google_application_credentials_file_path = os.path.dirname(os.path.abspath(__file__)) + "/mister-market-project-353264e22939.json"
        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_application_credentials_file_path
        #response = requests.post('https://us-central1-mister-market-project.cloudfunctions.net/update_company_data_cloud_function')
        #print(response)
        #print(json.dumps(response.json(),indent=2))

    else:
        print("NOTHING ELSE TO UPDATE!")
