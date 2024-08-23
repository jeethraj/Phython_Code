import cx_Oracle
#import mysql.connector
import pymysql
from ProdConf import db_config
from ProdConf import username, password, host, port, service_name

cx_Oracle.init_oracle_client(lib_dir="C:\oracle_instant_client")
# cx_Oracle.init_oracle_client(lib_dir="/home/oracle/app/oracle/product/19.0.0/client_1/lib")
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

    # Execute Oracle query
    oracle_query = """
    SELECT '00211'||SERVICE_ID_V FROM CB_SUBS_PROVISIONING
    WHERE TRUNC(ACTION_DATE_DT)  = TO_DATE(SYSDATE) AND ACTION_CODE_V='DURP' AND STATUS_V='R'
    ORDER BY ACTION_DATE_DT DESC
    """
    oracle_cursor.execute(oracle_query)
    oracle_result = oracle_cursor.fetchall()

    # Process Oracle result
    values_for_mysql = [str(row[0]) for row in oracle_result]
    print(f"{values_for_mysql=}")
    

    if values_for_mysql:

        # Connect to MySQL database
        # mysql_connection = mysql.connector.connect(
        #     user=mysql_username,
        #     password=mysql_password,
        #     host=mysql_host,
        #     port=mysql_port,
        #     database=mysql_database
        # )
        mysql_connection= pymysql.connect(**db_config)
        mysql_cursor = mysql_connection.cursor()

        # Execute MySQL query
        mysql_query = "SELECT CONCAT(SUBSTR(EXTERNAL_PAYER_ID_V, 6, 10)) FROM PAYERS WHERE PAYER_STATUS_N = 1 AND EXTERNAL_PAYER_ID_V IN (" + ",".join(["%s"] * len(values_for_mysql)) + ")"
        mysql_cursor.execute(mysql_query, values_for_mysql)
        mysql_result = mysql_cursor.fetchall()

        # Process MySQL result
        values_for_oracle = [str(row[0]) for row in mysql_result]
        print("Mysql Result" + str(values_for_oracle))

        oracle_query_2 = "SELECT PROCESS_NO_N FROM CB_SUBS_PROVISIONING WHERE ACTION_CODE_V='DURP' AND SERVICE_ID_V IN (" + ",".join([f":param{i+1}" for i in range(len(values_for_oracle))]) + ")"
        oracle_cursor.execute(oracle_query_2,values_for_oracle)
        oracle_result_2 = oracle_cursor.fetchall()
        print (f"{oracle_result_2=}")

        # Process Number to Oracle
        processNo_for_oracle = [str(row[0]) for row in oracle_result_2]
        print("Process Number" + str(processNo_for_oracle))
        
        # Connect back to Oracle database
        #for processNo_for_oracle in processNo_for_oracle:
            # Execute PL/SQL block
        plsql_block = f"""
            DECLARE
              IP_PROCESS_SEQ_NO_N NUMBER;
              IP_RESPONSE_STRING_V CLOB;
              OP_SUCCESS_FLAG_N NUMBER;
              OP_SCHDL_LINK_CODE_N NUMBER;
              IP_SYSTEM_ID VARCHAR2(200);
            BEGIN
            -- Loop through the values in the array
            FOR i IN 1..:num_values
            LOOP
                -- Assign the current value from the array to IP_PROCESS_SEQ_NO_N
                IP_PROCESS_SEQ_NO_N := :process_values(i);
                
                -- Define other variables as needed
                IP_RESPONSE_STRING_V := '<ROOT><status>success</status></ROOT>';
                IP_SYSTEM_ID := '0';

                -- Call your procedure with the current value
                UPDT_CAI_RESP_PRC(
                IP_PROCESS_SEQ_NO_N => IP_PROCESS_SEQ_NO_N,
                IP_RESPONSE_STRING_V => IP_RESPONSE_STRING_V,
                OP_SUCCESS_FLAG_N => OP_SUCCESS_FLAG_N,
                OP_SCHDL_LINK_CODE_N => OP_SCHDL_LINK_CODE_N,
                IP_SYSTEM_ID => IP_SYSTEM_ID
                );

                -- You can add any additional logic or processing here
                END LOOP;
            END;
            """
            #print (plsql_block)
        
        oracle_cursor.execute(plsql_block, num_values=len(processNo_for_oracle), process_values=processNo_for_oracle)
        print("Execution done")
        
    print ("Execution is completed "+str(values_for_mysql))
    
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
