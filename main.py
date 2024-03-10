import json
import time
import requests
import json
import hmac
import hashlib
from datetime import datetime
from multiprocessing.pool import Pool
from itertools import repeat
import configparser
import traceback

class SpikeTrader:
    global g_klines_rate_limit
    g_klines_rate_limit=[]

    def run(self):
        config = configparser.RawConfigParser()
        config.read('/home/thomas/PycharmProjects/SpikeTrader/spike_settings.cfg')
        api_key=config['variablen']['api_key']
        symbols=config['variablen']['symbols'].split(",")
        amount=config['variablen']['amount'].split(",")
        spike_size=config['variablen']['spike_size'].split(",")
        precision_amount=config['variablen']['precision_amount'].split(",")
        precision_price=config['variablen']['precision_price'].split(",")
        secret=config['variablen']['secret']
        pool=Pool(len(symbols))
        pool.starmap(test.sell_on_spike_and_buy_on_recover,zip(symbols,amount,spike_size,repeat(api_key),repeat(secret),precision_price,precision_amount))
        print("STOPPED")

    def get_signature(self,params,secret_key):
            query_string = '&'.join(["{}={}".format(d, params[d]) for d in params])
            return hmac.new(secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
    


    def send_api_request(self,method, endpoint,session,api_key="",secret_key="",base_url="",secure=True, params={}):
        url = base_url + endpoint
        if secure:
            params['timestamp'] = int(time.time() * 1000)
            params['recvWindow'] = 5000
            params['signature'] = self.get_signature(params,secret_key)
            headers = {'X-MBX-APIKEY': api_key}
        else:
            headers = {}
        response = getattr(session, method)(url, headers=headers, params=params)
        return json.loads(response.text)
    
    def gather_klines(self,symbol,startTime,session,base_url): 
            global g_klines_rate_limit
            g_prices={}
            endTime=int(time.time())*1000
            endTime = (endTime// 14400000)*14400000
            prices={symbol:[]}
            while startTime<endTime:
                g_klines_rate_limit.append(time.time())
                g_klines_rate_limit=[rate for rate in  g_klines_rate_limit if rate >= time.time()-60]
                while len(g_klines_rate_limit)>120:
                    g_klines_rate_limit=[rate for rate in  g_klines_rate_limit if rate >= time.time()-60]
                    time.sleep(0.1)
                klines = self.send_api_request('get', '/api/v3/klines',session=session,base_url=base_url,secure=False,
                    params={'symbol':symbol,'startTime':startTime,'endTime':endTime,'interval':'1m','limit':1000})
                if 'code' in klines:
                    if klines['code']==1102:
                        break
                startTime=klines[-1][0]
                prices[symbol].extend(klines)
            g_prices = list(map(lambda lst: [lst[0], lst[2]],  prices[symbol]))
            return(g_prices)

    def get_symbol_price(self,symbol):
        result={}
        session = requests.Session()
        base_url = "https://api.binance.com"
        result = self.send_api_request('get', '/api/v3/ticker/price',session=session,base_url=base_url,secure=False,params={'symbol':symbol})
        return result

    def gather_historic_spikes(self,symbol,startTime):
        session=requests.Session()
        base_url="https://api.binance.com"
        prices=test.gather_klines(symbol,startTime,session,base_url)
        prev_price=None
        spikes=[]
        spike_size=4
        for price in prices:
            if float(price[1])>0.43:
                 print("test")
            if prev_price==None:
                prev_price=price[1]
                continue
            spike_difference =  (float(prev_price)*100)/float(price[1])
            spike_height = 100-spike_difference
            if spike_height>=spike_size:
                spikes.append({'time':price[0],'spike':spike_height,'price':float(price[1]),'prev_price':prev_price})
            prev_price=price[1]
        #return spikes
        return prices
    
    def get_available_qty(self,symbol,api_key,secret,amount):
        base_url="https://api.binance.com"
        session=requests.Session()
        asset_info=self.send_api_request('get', '/api/v3/exchangeInfo',session=session,base_url=base_url,secure=False, params={'symbol':symbol})
        symbol=asset_info['symbols'][0]['baseAsset']
        assets = self.send_api_request('post', '/sapi/v3/asset/getUserAsset',session,api_key,secret,base_url,secure=True, params={'asset':symbol})
        qty = float(assets[0]['free'])*(amount/100)
        return qty

    def sell_on_spike_and_buy_on_recover(self,symbol,amount,spike_size,api_key,secret_key,prec_price,prec_amount):     
        amount=int(amount)
        i=0
        prec_price=int(prec_price)
        prec_amount=int(prec_amount)
        qty=self.get_available_qty(symbol,api_key,secret_key,amount)
        base_url="https://api.binance.com"
        session=requests.Session()
        prev_price=None
        spike=None
        buy_price=None
        spike_size=int(spike_size)
        self.write_to_log(symbol,"Starte Handel mit \n Qantity: "+str(round(qty,prec_amount))+"\n Amount: " + str(amount) +"%\n Spike Size: " + str(spike_size) + "%\n")
        while True:
            try:
                if i % 2 == 0:
                    qty=self.get_available_qty(symbol,api_key,secret_key,amount)
                #for current_price in prices:
                current_price=self.get_symbol_price(symbol)
                try:
                    current_price=float(current_price['price'])
                except Exception:
                    time.sleep(50)
                    continue
                if prev_price!=None:
                    #if prev_price!=current_price:
                    spike_difference =  (float(prev_price)*100)/float(current_price)
                    prev_price=current_price
                else:
                    prev_price=current_price
                    spike_difference =  (float(prev_price)*100)/float(current_price)       
                spike_height = 100-spike_difference
                self.write_to_log(symbol,"Spike Größe: "+ str(round(spike_height,2)))
                if spike_height>=spike_size:
                    self.write_to_log(symbol,"Spike entdeckt")
                    now = datetime.now()
                    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
                    spike={'time':timestamp,'spike':spike_height,'price':float(current_price),'prev_price':prev_price}
                    self.write_to_log(symbol,str(spike))
                    sell=self.file_order(symbol,round(float(current_price),prec_price),round(qty,prec_amount),'sell',api_key,secret_key)
                    self.write_to_log(symbol,"Verkaufsorder gesetzt")
                    self.write_to_log(symbol,str(sell))
                    buy_price = prev_price
                    while True:
                        order = self.send_api_request('get', "/api/v3/order",session,api_key,secret_key,base_url,secure=True, params={'symbol':symbol,'orderId':sell['orderId']})
                        if order['status']=="FILLED":
                        #if order['status']=='CANCELED':
                            self.write_to_log(symbol,"Verkaufsorder erfüllt")
                            self.write_to_log(symbol,str(order))
                            break
                        time.sleep(5)
                    buy=self.file_order(symbol,round(float(buy_price),prec_price),round(float(order['executedQty']),prec_amount),'buy',api_key,secret_key)
                    #buy=self.file_order(symbol,float(buy_price),33,'buy',api_key,secret_key)
                    self.write_to_log(symbol,"Kaufsorder gesetzt")
                    while True:
                        order = self.send_api_request('get', "/api/v3/order",session,api_key,secret_key,base_url,secure=True, params={'symbol':symbol,'orderId':buy['orderId']})
                        if order['status']=="FILLED":
                        #if order['status']=='CANCELED':
                            self.write_to_log(symbol,"Kaufsorder erfüllt")
                            self.write_to_log(symbol,str(order))
                            break
                        time.sleep(5)
                    time.sleep(5)
                else:
                    time.sleep(60)
                    if i>120:
                        i=0
                        #self.write_to_log(symbol,"ich leb noch und werde bei Gelegenheit " +str(qty) +" "+symbol +" verkaufen")
                    else:
                        i=i+1
            except Exception as e:
                 error_traceback = traceback.format_exc()
                 self.write_to_log(symbol,error_traceback)
                
        


    def write_to_log(self,symbol, message):
        log_file=f'/home/thomas/PycharmProjects/SpikeTrader/{symbol}trade.log'
        with open(log_file, 'a') as file:
            now = datetime.now()
            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
            file.write(f'{timestamp} {symbol} {message}\n')

    def file_order(self,symbol,price,qty,side,api_key,secret_key):
        base_url="https://api.binance.com"
        session=requests.Session()
        order = self.send_api_request('post', "/api/v3/order/test",session,api_key,secret_key,base_url,secure=True, 
                                      params={'symbol':symbol,
                                              'side':side,
                                              'type':'LIMIT',
                                              'quantity':qty,
                                              'timeInForce':'GTC',
                                              'price':str(price)
                                              })
        return order

#try:
test=SpikeTrader()
test.run()
# except Exception as e:
#     error=SpikeTrader()
#     error.write_to_log("",e)
#     print(e)