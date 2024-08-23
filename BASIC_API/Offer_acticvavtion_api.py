import requests
from xml.etree import ElementTree as ET
from ConfData import eia_username, eia_password, eia_url
from CurrentTimeFormatd import time_formats

# Define the SOAP request XML
headers = {
    "Content-Type": "text/xml;charset=UTF-8",
    "SOAPAction": "http://www.openuri.org/clientRequest",  # You may need to provide the SOAPAction header if required by the service
}


class api_call():
    def call_offer_activation(self,msisdn):
    # Define the SOAP request body
        print(f"Calling EIA API with :{msisdn}\n")
        #print(f"Coming here :{self}")
        time = time_formats()
        #print(time.eia_formated_time())
        xml_data = f"""
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:env="http://schema.concierge.com/Envelope" xmlns:sch="http://schema.concierge.com">
        <soapenv:Header xmlns:auth="http://schemas.eia.org/middleware/AuthInfo">
            <auth:authentication>
                <auth:user>{eia_username}</auth:user>
                <auth:password>{eia_password}</auth:password>
                <auth:type>PLAIN</auth:type>
            </auth:authentication>
        </soapenv:Header>
        <soapenv:Body>
            <sch:clientRequest>
                <env:EaiEnvelope>
                    <env:Service>ServiceRequest</env:Service>
                    <env:UserId>admin</env:UserId>
                    <env:Sender>USSD</env:Sender>
                    <env:MessageId>123454567890</env:MessageId>
                    <env:SentTimeStamp>{time.eia_formated_time()}</env:SentTimeStamp>
                    <env:Payload>
                    <ser:ServiceRequest xmlns:ser="http://schemas.eia.org/ServiceRequest">
                        <ser:Request>
                            <ser:Operation_Name>changeVasDetails</ser:Operation_Name>
                            <ser:ChangeType>AddVAS</ser:ChangeType>
                            <ser:Code>500MBquota</ser:Code>
                            <ser:MobileNumber>{msisdn}</ser:MobileNumber>
                            <ser:Channel>USSDh</ser:Channel>
                            <ser:DiscountMultiplier></ser:DiscountMultiplier>
                        </ser:Request>
                    </ser:ServiceRequest>
                    </env:Payload>
                </env:EaiEnvelope>
            </sch:clientRequest>
        </soapenv:Body>
        </soapenv:Envelope>
        """
        print(xml_data)
        # Send the POST request
       
        response = requests.post(eia_url, data=xml_data, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
          
            response_xml = ET.fromstring(response.content)
            namespace = {"ser": "http://schemas.eia.org/ServiceRequest"}
            result_code_element = response_xml.find(".//ser:resultCode", namespaces=namespace)
            result_message_element = response_xml.find(".//ser:resultMessage", namespaces=namespace)
            result_code_element = response_xml.find(".//ser:resultCode", namespaces=namespace)

            result_message_element = response_xml.find(".//ser:resultMessage", namespaces=namespace)

    

            if result_code_element is not None and result_message_element is not None:

                result_code = result_code_element.text

                result_message = result_message_element.text

                print(f"Result Code: {result_code}")

                print(f"Result Message: {result_message}")

            else:

                result_code_element = response_xml.find(".//{http://ws.apache.org/ns/synapse}resultCode")

                result_message_element = response_xml.find(".//{http://ws.apache.org/ns/synapse}resultMessage")

    

                if result_code_element is not None and result_message_element is not None:

                    result_code = result_code_element.text

                    result_message = result_message_element.text

                    print(f"Result Code: {result_code}")

                    print(f"Result Message: {result_message}")

                else:

                    print("Result code or result message element not found in the SOAP response.")
                
        else:
            print(f"Request failed with status code: {response.status_code}")
