# Seeting up project environment and API keys
from dotenv import load_dotenv
import os
import requests as rq
import json
import pandas as pd
import numpy as np
import warnings
import pytz
import datetime
import time
from IPython.display import clear_output
from supabase import create_client, Client
load_dotenv('.env') 

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)


pd.set_option('display.precision', 4,
              'display.colheader_justify', 'center')
PUB_URL=os.getenv("COINGECKO_API_URL")
use_demo = {
    "accept": "application/json",
    "x-cg-demo-api-key" : os.getenv("COINGECKO_API_KEY")
}

def convert_to_local_tz(old_ts, desired_timezone="Asia/Singapore"):
    """
        This method converts the date to a proper timezone
    """
    new_tz = pytz.timezone(desired_timezone)
    old_tz = pytz.timezone("UTC")
    
    format = "%Y-%m-%dT%H:%M:%S+00:00"
    datetime_obj = datetime.datetime.strptime(old_ts, format)
    
    localized_ts = old_tz.localize(datetime_obj)
    new_ts = localized_ts.astimezone(new_tz)
    
    return new_ts

def get_response(endpoint, headers, params, URL):
    """ 
    Method to fetch data from external APIs
    Parameters:
        - endpoint (str): The API endpoint to fetch data from.
        - headers (dict): The headers to include in the request.
        - params (dict): The parameters to include in the request.
        - URL (str): The base URL of the API.

    Returns:
        - dict: The JSON response from the API if the request is successful.
        - None: If the request fails, prints an error message.
    """
    url = "".join((URL, endpoint))
    response = rq.get(url, headers = headers, params = params)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Failed to fetch data, check status code {response.status_code}")

def get_trade_exchange(id, base_curr, target_curr):
    """
        This method information about a specific exchange basing on

        Parameters:
            - id : id of the exchange
            - bass_curr : the token of interest
            - target_curr : the user currency
    """
    exchange_ticker_response = get_response(f"/exchanges/{id}/tickers",
                                            use_demo,
                                            {},
                                            PUB_URL)
    
    found_match = ""
    
    for ticker in exchange_ticker_response["tickers"]:
        if ticker["base"] == base_curr and ticker["target"] == target_curr:
            found_match = ticker
            break
            
    # if found_match == "":
    #     warnings.warn(f"No data found for {base_curr}-{target_curr} pair in {id}")
    
    return found_match

def get_trade_exchange_per_country(country,
                                   base_curr,
                                   target_curr,
                                   df_ex_subset:pd.DataFrame):
    """
        Get cypto ticker data for multiple exchanges


        The bid-ask spread percentage is the difference between the lowest price asked for an asset by a seller and the highest bid made by a potential buyer. A lower value of the spread indicates higher liquidity and trading volume for the given asset on the exchange. Conversely, higher spread usually indicates lower liquidity. This metric can therefore be used to judge whether a particular exchange should be considered for executing arbitrage trades or not
    """
    
    df_all = df_ex_subset[(df_ex_subset["country"] == country)]    
    
    exchanges_list = df_all["id"]
    ex_all = []    
       
    for exchange_id in exchanges_list:
        found_match = get_trade_exchange(exchange_id, base_curr, target_curr)
        if found_match == "":
            continue
        else:
            temp_dict = dict(
                             exchange = exchange_id,
                             last_price = found_match["last"],
                             last_vol   = found_match["volume"],
                             spread     = found_match["bid_ask_spread_percentage"],
                             trade_time = convert_to_local_tz(found_match["last_traded_at"],os.getenv('TIMEZONE'))
                             )
            ex_all.append(temp_dict)
            
    return pd.DataFrame(ex_all)

def get_exchange_rate(base_curr):
    """
        Get bitcoin exchange rates for multiple currencies
    """
    
    # This returns current BTC to base_curr exchange rate    
    exchange_rate_response = get_response(f"/exchange_rates",
                                          use_demo,
                                          {},
                                          PUB_URL)
    rate = ""
    try:
        rate = exchange_rate_response["rates"][base_curr.lower()]["value"]
    except KeyError as ke:
        print("Currency not found in the exchange rate API response:", ke)
        
    return rate  

def get_vol_exchange(id:str, days:int, base_curr:str):
    """
    Parameters:
        - id (str): ID of the exchange
        - days (number): The history data you want in days.
        - base_curr (string): The base currency
    """
    
    vol_params = {"days": days}
    
    exchange_vol_response = get_response(f"/exchanges/{id}/volume_chart",
                                         use_demo,
                                         vol_params,
                                         PUB_URL)
    
    time, volume = [], []
    
    # Get exchange rate when base_curr is not BTC
    ex_rate = 1.0
    if base_curr != "BTC":
        ex_rate = get_exchange_rate(base_curr)
        
        # Give a warning when exchange rate is not found
        if ex_rate == "":
            print(f"Unable to find exchange rate for {base_curr}, vol will be reported in BTC")
            ex_rate = 1.0
    
    for i in range(len(exchange_vol_response)):
        # Convert to seconds
        s = exchange_vol_response[i][0] / 1000
        time.append(datetime.datetime.fromtimestamp(s).strftime('%Y-%m-%d'))
        
        # Default unit for volume is BTC
        volume.append(float(exchange_vol_response[i][1]) * ex_rate)
                      
    df_vol = pd.DataFrame(list(zip(time, volume)), columns = ["date", "volume"])
    
    # Calculate SMA for a specific window
    df_vol["volume_SMA"] = df_vol["volume"].rolling(7).mean()
    
    return df_vol.sort_values(by = ["date"], ascending = False).reset_index(drop = True)

def highlight_max_min(x, color):
    
    return np.where((x == np.nanmax(x.to_numpy())) |
                    (x == np.nanmin(x.to_numpy())),
                    f"color: {color};",
                    None)

def agg_per_exchange(df_ex_all, base_curr):
    # Group data and calculate statistics per exchange    
    df_agg = (
        df_ex_all.groupby("exchange").agg
        (        
            trade_time_min = ("trade_time", 'min'),
            trade_time_latest = ("trade_time", 'max'),
            last_price_mean = ("last_price", 'mean'),
            last_vol_mean = ("last_vol", 'mean'),
            spread_mean = ("spread", 'mean'),
            num_trades = ("last_price", 'count')
        )
    )
    
    # Get time interval over which statistics have been calculated    
    df_agg["trade_time_duration"] = df_agg["trade_time_latest"] - df_agg["trade_time_min"]
    
    # Reset columns so that we can access exchanges below
    df_agg = df_agg.reset_index()
    
    # Calculate % of total volume for all exchanges
    last_vol_pert = []
    for i, row in df_agg.iterrows():
        try:
            df_vol = get_vol_exchange(row["exchange"], 30, base_curr)
            current_vol = df_vol["volume_SMA"][0]
            vol_pert = (row["last_vol_mean"] / current_vol) * 100
            last_vol_pert.append(vol_pert)
        except:
            last_vol_pert.append("")
            continue
            
    # Add % of total volume column
    df_agg["last_vol_pert"] = last_vol_pert
    
    # Remove redundant column
    df_agg = df_agg.drop(columns = ["trade_time_min"])
    
    # Round all float values
    # (seems to be overwritten by style below)
    df_agg = df_agg.round({"last_price_mean": 2,
                           "last_vol_mean": 2,
                           "spread_mean": 2
                          })
    return df_agg

def display_agg_per_exchange(df_ex_all, base_curr):
    df_agg=agg_per_exchange(df_ex_all, base_curr)
    display(df_agg.style.apply(highlight_max_min,
                               color = 'green',
                               subset = "last_price_mean")
           )
           
def run_bot(country:str,
            base_curr:str,
            target_curr:str):
    """
        This runs the bots collectiig arbitrage opportunities

        Parameters:
            - country: the target country for which markets the user participants in
            - base_curr : interested token
            - target_curr : the user currency
    """
    # list all crypto exchanges
    exchange_params = {
        "per_page": 250,
        "page": 1
    }
    exchange_list_response = get_response("/exchanges", use_demo, exchange_params, PUB_URL)
    df_ex = pd.DataFrame(exchange_list_response)

    # list exchanges by trading volume
    df_ex_subset = df_ex[["id", "name", "country", "trade_volume_24h_btc"]]
    df_ex_subset = df_ex_subset.sort_values(by = ["trade_volume_24h_btc"], ascending = False)
    
    df_ex_all = get_trade_exchange_per_country(country, base_curr, target_curr, df_ex_subset)
    
    # Collect data every minute    
    while True:
        time.sleep(60)
        df_new = get_trade_exchange_per_country(country, base_curr, target_curr,df_ex_subset)
        
        # Merge to existing DataFrame
        df_ex_all = pd.concat([df_ex_all, df_new])
        
        # Remove duplicate rows based on all columns
        df_ex_all = df_ex_all.drop_duplicates()

        aggr = agg_per_exchange(df_ex_all,base_curr)

        json_data = json.loads(aggr.to_json(orient='records'))
        addData(json_data)
        print("Data has been saved to db")
        
        # Clear previous display once new one is available
        # clear_output(wait = True)
        # display_agg_per_exchange(df_ex_all, base_curr)        
        
    return None

def addData(data):
    response = (
        supabase.table("crypto_arb_opportunities")
        .insert({"data": data})
        .execute()
    )
def getData():
    addData()
    response = (supabase.table("crypto_arb_opportunities").select("*").execute())
    return response


run_bot("United States","ETH","USDT")

# Get cypto ticker data for multiple exchanges
# df_ex_all= get_trade_exchange_per_country("United States","ETH","USD")

