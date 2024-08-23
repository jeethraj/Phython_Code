import pymysql
import cx_Oracle
#import file as file
from datetime import datetime
from datetime import timedelta
from prettytable import PrettyTable
#from ConfData import db_config
#from ConfData import username, password, host, port, service_name
from ProdConf import db_config
from ProdConf import username, password, host, port, service_name


# Connect to MySQL
mysql_connection = pymysql.connect(**db_config)
mysql_cursor = mysql_connection.cursor()


tariffID=10054
# Fetch data from MySQL
mysql_query = f"""
SELECT B.EXTERNAL_PAYER_ID_V, A.PRODUCT_ID_N,A.START_DATE_DT
FROM PAYER_TARIFFS A, PAYERS B
WHERE A.PRODUCT_ID_N IN ({tariffID})
AND A.PAYER_ID_N = B.PAYER_ID_N
AND A.START_DATE_DT < SYSDATE()
#AND A.START_DATE_DT not like '2024-02-27%'
AND A.EVENT_REQUEST_NO_N is Null
AND A.STATUS_N = '1'
AND concat(A.PAYER_ID_N,A.PRODUCT_ID_N) not in (SELECT concat(A.PAYER_ID_N,A.PRODUCT_ID_N)  from PAYER_TARIFFS
WHERE A.STATUS_N = '5' AND START_DATE_DT<sysdate())
"""

mysql_cursor.execute(mysql_query)
mysql_data = mysql_cursor.fetchall()

# Close MySQL connection
mysql_cursor.close()
mysql_connection.close()

# Connect to Oracle
cx_Oracle.init_oracle_client(lib_dir="C:\oracle_instant_client")
oracle_dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
oracle_connection_string = f"{username}/{password}@{oracle_dsn}"
oracle_connection = cx_Oracle.connect(oracle_connection_string)
oracle_cursor = oracle_connection.cursor()

table = PrettyTable()
table.field_names = ["MSISDN", "Tariff ID" , "Start Date"]

cbsOfferCode =  f"select OFFER_CODE_V,SCHEME_REF_CODE_N from CB_OFFERS where APPLY_TARIFF_CODE_V={tariffID} AND STATUS_OPTN_V='A'"

oracle_cursor.execute(cbsOfferCode)
cbsOfferCodeResult = oracle_cursor.fetchone()
cbsOfferCodeResult_1= cbsOfferCodeResult[0]
cbsSchemeRefResult_1= cbsOfferCodeResult[1]
print(cbsOfferCodeResult_1)
print(cbsSchemeRefResult_1)

# Process MySQL data and execute Oracle query
for row in mysql_data:
    external_payer_id = row[0]
    product_id = row[1]
    start_date = row[2] 

    # Modify MSISDN and Tariff ID as mentioned
    modified_msisdn = str(external_payer_id).replace('00211', '')
    tariff_id = int(product_id)

    # Use bind variables in the Oracle query
    oracle_query = """
    SELECT B.APPLY_TARIFF_CODE_V, C.MOBL_NUM_VOICE_V
    FROM CB_SUBS_OFFER_DETAILS A, CB_OFFERS B, GSM_SERVICE_MAST C
    WHERE C.ACCOUNT_LINK_CODE_N = A.ACCOUNT_LINK_CODE_N
    AND A.OFFER_CODE_V = B.OFFER_CODE_V
    AND C.MOBL_NUM_VOICE_V = :modified_msisdn
    AND B.APPLY_TARIFF_CODE_V = :tariff_id
    AND A.STATUS_OPTN_V = 'A'
    AND A.START_DATE_D<sysdate
    -- AND A.START_DATE_D like '25-02-24%'
    AND B.APPLY_TARIFF_CODE_V NOT IN ('10024')
    ORDER BY A.START_DATE_D DESC
    """

    # Use bind variables to avoid SQL injection
    oracle_cursor.execute(oracle_query, modified_msisdn=modified_msisdn, tariff_id=tariff_id)
    oracle_data = oracle_cursor.fetchall()
    
    formatted_start_date = ""
    formatted_end_date = ""
  
    # Check if the MSISDN is not present in Oracle data
    if not oracle_data:
        table.add_row([modified_msisdn, tariff_id, start_date])
    
        #print(f"modified_msisdn : {modified_msisdn}")
    
        # Fetch ACCOUNT_LINK_CODE_N for the missing modified_msisdn
        fetch_account_link_code_query = f"SELECT ACCOUNT_LINK_CODE_N FROM GSM_SERVICE_MAST WHERE MOBL_NUM_VOICE_V = '{modified_msisdn}'"
    


    # Execute the query to fetch ACCOUNT_LINK_CODE_N
        oracle_cursor.execute(fetch_account_link_code_query)
        account_link_code_result = oracle_cursor.fetchone()

    # Check if the result is not empty
        if account_link_code_result:
            account_link_code = account_link_code_result[0]
        
            # print(f"Modified MSISDN: {modified_msisdn}")
            print(f"Account Link Code: {account_link_code}")

         # Check if start_date is already a datetime object
            if isinstance(start_date, datetime):
                start_date_dt = start_date
            else:
            # Convert the string to a datetime object
                start_date_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        # Add 30 days to the start_date_dt
        end_date_dt = start_date_dt + timedelta(days=30)
        #end_date_dt = start_date_dt + timedelta(days=7)


        # Format the datetime object as per your desired format
        formatted_start_date = start_date_dt.strftime('%d-%m-%y %I:%M:%S.%f %p %z')
        formatted_end_date = end_date_dt.strftime('%d-%m-%y %I:%M:%S.%f %p %z')

        # print("Inside IF")
        # print(f"{start_date_dt=}")
        # print(f"{formatted_start_date=}")
        # print(f"{end_date_dt=}")
        # print(f"{formatted_end_date=}")

        # Use account_link_code in the update statement
        InsertStatement = f"""--MSISDN : {modified_msisdn}  account_link_code : {account_link_code} tariff_id : {tariff_id} cbsOfferCodeResult_1: {cbsOfferCodeResult_1} \n
        INSERT INTO CB_SUBS_OFFER_DETAILS (ACCOUNT_LINK_CODE_N,OFFER_CODE_V,START_DATE_D,
        END_DATE_D,CANCEL_DATE_D,USER_CODE_N,STATUS_OPTN_V,REQUESTED_DATE_D,SCHEME_REF_CODE_N,FACILITY_REF_ID_V,
        NARRATION_V,TARIFF_FLAG_V,MOVE_CHARGE_TO_ACC_V,ORDER_REF_NO,ADDNL_OFFER_FLG_V,EXTENDED_COUNT_N,
        EXTENSION_DATE_D,LAST_EXTENDED_DATE_D,QUANTITY_N,LAST_MODIFIED_DATE_D,ADDN_ATTR_V_1,ADDN_ATTR_V_2,
        TECH_ATRIBUTE_V,EXPIRY_DATE_D,SERIAL_NUM_V,AUTO_RENEW_V,CONTRACT_END_DT_D) values ({account_link_code},
        '{cbsOfferCodeResult_1}',to_timestamp_tz('{formatted_start_date}+02:00','DD-MM-RR HH12:MI:SSXFF AM TZR'),
        to_timestamp_tz('{formatted_end_date}+02:00','DD-MM-RR HH12:MI:SSXFF AM TZR'),null,null,'A',to_timestamp_tz
        ('06-02-24 01:00:00.133494000 PM +02:00','DD-MM-RR HH12:MI:SSXFF AM TZR'),
        {cbsSchemeRefResult_1},1,null,'Y',null,null,null,null,null,null,1,to_timestamp_tz('02-06-23 06:17:02.133494000 PM +02:00',
        'DD-MM-RR HH12:MI:SSXFF AM TZR'),null,null,null,null,null,null,null);
        """

        #fileName = f"InsertStatementMonthly_{tariff_id}.txt"
        fileName = f"InsertStatementWeekly_{tariff_id}.txt"
        with open(fileName, "a") as file:
                file.write(InsertStatement + "\n")

       # print(InsertStatement)  # Print or execute the update statement as needed
# Close Oracle connection
oracle_cursor.close()
oracle_connection.close()

table_str = str(table)
if table_str:
    print("Missing Details:")
    print(table)
else:
    print("No missing details found.")