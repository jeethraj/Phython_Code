import cx_Oracle
import pymysql
import pandas as pd
from ProdConf import db_config
from ProdConf import username, password, host, port, service_name


cx_Oracle.init_oracle_client(lib_dir="C:\oracle_instant_client")
dns = cx_Oracle.makedsn(host, port, service_name=service_name)
connectionString = f"{username}/{password}@{dns}"

# MySQL database connection details
mysql_username = "docsprodzain"
mysql_password = "ZainSS#Prod$123#innoDB"
mysql_host = "172.168.101.201"
mysql_database = "docs_mysql_prod_zain"
mysql_port = 6446

mysql_cursor = None
mysql_connection = None
oracle_cursor = None
oracle_connection = None

try:
   # Connect to Oracle database
    oracle_connection = cx_Oracle.connect(connectionString)
    oracle_cursor = oracle_connection.cursor()

    cbs_query =f"""SELECT A.OFFER_DESC_V,A.APPLY_TARIFF_CODE_V,B.AMOUNT_N/100 FROM  CB_OFFERS A, CB_SCHEME_CATEGORY_CHRG B
                WHERE B.SUBS_CATEGORY_CODE_V='INDVP' AND
                A.SCHEME_REF_CODE_N=B.SCHEME_REF_CODE_N AND 
                A.APPLY_TARIFF_CODE_V IS NOT NULL AND
                A.CONTRACT_TYPE_N='P' AND A.STATUS_OPTN_V='A'"""
    
    oracle_cursor.execute(cbs_query)
    oracle_result = oracle_cursor.fetchall()

    oracle_cursor.close()
    oracle_connection.close()

    # Connect to MySQL
    mysql_connection = pymysql.connect(**db_config)
    mysql_cursor = mysql_connection.cursor(),

    for row in oracle_result:
        apply_tariff_code = row[0]
        
        mysql_query="""select pm.PRODUCT_ID_N ,  pm.DESCRIPTION_V , SUBSTR( ta.ATTRIBUTE_VALUE_V,1,INSTR(ta.ATTRIBUTE_VALUE_V,':')-1)  TT,
                        SUBSTR( ta.ATTRIBUTE_VALUE_V,INSTR(ta.ATTRIBUTE_VALUE_V,':')+1) TT_VALUE, wm.DESCRIPTION_V , uct.DESCRIPTION_V , wm.WALLET_ID_N ,
                        Case when  uct.DESCRIPTION_V  = 'Voice'
                        then ra.CREDIT_UNITS_N /60
                        when  uct.DESCRIPTION_V  = 'Data'
                        then ra.CREDIT_UNITS_N /1024
                        END WALLET_VALUE_MINS_MB
                        from TARIFF_ATTRIBUTES ta, PRODUCT_MASTER pm , WALLET_MASTER wm ,  UI_CREDIT_TYPE uct , REPLENISHMENT_ATTRIBUTE ra
                        where  ta.PRODUCT_ID_N  >=1 -- 10085
                        and  pm.PRODUCT_ID_N  = ta.PRODUCT_ID_N
                        and ra.CREDIT_WALLET_ID_N  = wm.WALLET_ID_N
                        and wm.PRODUCT_ID_N =pm.PRODUCT_ID_N
                        and uct.KEY_N =wm.CREDIT_TYPE_N
                        and pm.PRODUCT_ID_N= %s
                        """
        mysql_cursor.execute(mysql_query, (apply_tariff_code))
        mysql_data = mysql_cursor.fetchall()
    
    

    #FileName = "Offer_dump.csv"
    #df1.to_csv(FileName, index=False)
except Exception as e:
    print(f"Error: {str(e)}")

finally:
    if mysql_cursor is not None:
        mysql_cursor.close()
    if mysql_connection is not None:
        mysql_connection.close()
    if oracle_cursor is not None:
        oracle_cursor.close()
    if oracle_connection is not None:
        oracle_connection.close()