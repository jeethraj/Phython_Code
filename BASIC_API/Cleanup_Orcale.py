import pymysql
import cx_Oracle
from prettytable import PrettyTable
from ProdConf import db_config
from ProdConf import username, password, host, port, service_name

# Connect to Oracle
cx_Oracle.init_oracle_client(lib_dir="C:\TT - Software\instantclient-basic-windows.x64-21.11.0.0.0dbru\instantclient_21_11")
oracle_dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
oracle_connection_string = f"{username}/{password}@{oracle_dsn}"
oracle_connection = cx_Oracle.connect(oracle_connection_string)
oracle_cursor = oracle_connection.cursor()

# Fetch data from Oracle
oracle_query = """
SELECT B.APPLY_TARIFF_CODE_V, C.MOBL_NUM_VOICE_V
FROM CB_SUBS_OFFER_DETAILS A, CB_OFFERS B, GSM_SERVICE_MAST C
WHERE C.ACCOUNT_LINK_CODE_N = A.ACCOUNT_LINK_CODE_N
AND A.OFFER_CODE_V = B.OFFER_CODE_V
AND A.STATUS_OPTN_V = 'A'
AND B.APPLY_TARIFF_CODE_V NOT IN ('10024')
--AND B.APPLY_TARIFF_CODE_V ='10027'
ORDER BY A.START_DATE_D DESC
"""

oracle_cursor.execute(oracle_query)
oracle_data = oracle_cursor.fetchall()

# Close Oracle connection
oracle_cursor.close()
oracle_connection.close()


# Connect to MySQL
mysql_connection = pymysql.connect(**db_config)
mysql_cursor = mysql_connection.cursor()

table = PrettyTable()
table.field_names = ["MSISDN", "Tariff ID"]

# Process Oracle data and execute MySQL query
for row in oracle_data:
    apply_tariff_code = row[0]
    mobil_num_voice = row[1]

    modified_msisdn = '00211' + mobil_num_voice
    # Use placeholders in the MySQL query
    mysql_query = """
    SELECT B.EXTERNAL_PAYER_ID_V, A.PRODUCT_ID_N
    FROM PAYER_TARIFFS A, PAYERS B
    WHERE ##A.PRODUCT_ID_N IN (10027)AND 
    A.PAYER_ID_N = B.PAYER_ID_N
    AND A.STATUS_N = '1'
    AND B.EXTERNAL_PAYER_ID_V = %s
    AND A.PRODUCT_ID_N = %s
    """

    # Use a tuple to pass values
    mysql_cursor.execute(mysql_query, (modified_msisdn, apply_tariff_code))
    mysql_data = mysql_cursor.fetchall()

    # Check if the MSISDN is not present in MySQL data
    if not mysql_data:
        table.add_row([modified_msisdn, apply_tariff_code])

# Close MySQL connection
mysql_cursor.close()
mysql_connection.close()


table_str = str(table)
if table_str:
    print("Missing Details:")
    print(table)
else:
    print("No missing details found.")