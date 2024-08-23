from lxml import etree
from  zeep import  Client
from requests import  Session
from requests.auth import HTTPBasicAuth
#from zeep.utils import findall_multiple_ns
from zeep.wsse.username import UsernameToken
 
from ast import literal_eval
import json
import requests
import  xmltodict
from xml.etree import ElementTree as ET
from collections import OrderedDict
 
from bs4 import BeautifulSoup
 
 
entity = ''
# url="http://10.14.32.26:8001/"
url="http://10.40.0.197:8001/"
#headers = {'content-type': 'application/soap+xml'}
headers = {'content-type': 'text/xml'}
body = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:rmv="http://www.huawei.com/HLR9820/RMV_KI">
   <soapenv:Header>
      <rmv:Username>Pro</rmv:Username>
      <rmv:Password>Anawrania!1</rmv:Password>
   </soapenv:Header>
   <soapenv:Body>
      <rmv:RMV_KI>
         <!--You may enter the following 6 items in any order-->
         <!--Optional:-->
         <rmv:IMSI>{entity}</rmv:IMSI>    
      </rmv:RMV_KI>
   </soapenv:Body>
</soapenv:Envelope>"""
 
 
response = requests.post(url,data=body,headers=headers,verify=False)
 
data = (response.content.decode('utf8'))
print(response)