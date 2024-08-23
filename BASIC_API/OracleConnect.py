import logging
import oracledb as cx_Oracle  # Use the imported name

from ConfData import username, password, host, port, service_name
#from ProdConf import username, password, host, port, service_name

cx_Oracle.init_oracle_client(lib_dir="C:\oracle_instant_client")

dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
connectionString = f"{username}/{password}@{dsn}"

class FetchOracleData():
    def fetch_data(self):
        
        logging.debug("Starting fetch_data method")
        try:
            connection = cx_Oracle.connect(connectionString)
            #sql_query ="SELECT C.MoBL_NUM_VOICE_V FROM CB_SCHEDULES A , GSM_SERVICE_MAST C WHERE A.ACTION_PARM_STRG_V LIKE '%:CLM' AND A.SERV_ACC_LINK_CODE_N =C.ACCOUNT_LINK_CODE_N AND A.SERVICE_KEY_CODE_V = 'DURP' AND A.STATUS_OPTN_V = 'A'  AND TRUNC(A.PROCESS_ON_DATE_D) = TRUNC(SYSDATE) AND NOT EXISTS (SELECT 1 FROM CB_SUBS_OFFER_DETAILS B WHERE A.SERV_ACC_LINK_CODE_N = B.ACCOUNT_LINK_CODE_N AND B.OFFER_CODE_V = 'O160019')"
            sql_query ="SELECT C.MoBL_NUM_VOICE_V FROM CB_SCHEDULES A , GSM_SERVICE_MAST C WHERE  A.SERV_ACC_LINK_CODE_N =C.ACCOUNT_LINK_CODE_N AND A.STATUS_OPTN_V = 'A'  AND TRUNC(A.PROCESS_ON_DATE_D) = TRUNC(SYSDATE-200) AND NOT EXISTS (SELECT 1 FROM CB_SUBS_OFFER_DETAILS B WHERE A.SERV_ACC_LINK_CODE_N = B.ACCOUNT_LINK_CODE_N AND B.OFFER_CODE_V = 'O160019')"
            cursor = connection.cursor()
            cursor.execute(sql_query)
            result = cursor.fetchall()
            connection.close()
            return result
            
        except cx_Oracle.DatabaseError as e:
            logging.error(f"Error connecting to the database: {e}")
            return e

