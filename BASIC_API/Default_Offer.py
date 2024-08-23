from Offer_acticvavtion_api import api_call
from OracleConnect import FetchOracleData

#Connect to data base and 
obj = FetchOracleData()
msisdns = obj.fetch_data()
print(msisdns)
mobile_numbers =[]



for msisdn in (msisdns):
    #Call EIA offer activation API
    api = api_call()
    print(msisdn[0])
    #api.call_offer_activation(msisdn[0])
    
