import cx_Oracle
import pymysql
import pandas as pd
from ProdConf import db_config
from ProdConf import username, password, host, port, service_name
import pysftp 
import os
from datetime import datetime, time, timedelta

current_date = datetime.now()
date = current_date.strftime('%Y-%m-%d')

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

    cbs_query = """SELECT A.OFFER_DESC_V,A.APPLY_TARIFF_CODE_V,TRUNC(A.OFFER_ST_DATE_D),A.OFFER_CODE_V,TRUNC(A.OFFER_END_DATA_D),A.SERVICE_CODE_V,A.SCHEME_REF_CODE_N,A.CONTRACT_TYPE_N,A.USER_CODE_N,A.STATUS_OPTN_V,A.LAST_ACCESSED_BY_N,TRUNC(A.LAST_ACCESSED_DATE_D),A.LAUNCHED_BY_N,TRUNC(A.LAUNCHED_ON_D),A.VOID_BY_N,TRUNC(A.VOID_ON_D),A.VOID_COMMENT_V,A.PROFIT_CENTER_CODE_V,A.CARRIER_FLAG_V,A.CHECK_DEPENDANTS_FLAG_V,A.CHECK_IMMICAL_FLAG_V,A.OFFER_TYPE_V,A.OFFER_GROUP_CODE_V,A.SHORT_DESC_IN_BILL_V,A.TARIFF_FLAG_V,A.APPLY_FOR_SERVICE_TYPES_V,A.TARIFF_PRIORITY_N,A.DESTINATION_SERVICE_TYPE_V,A.FAX_DATA_VOICE_SMS_V,A.OFFER_FLAG_V,A.OFFER_DESC_LN_V,A.SHORT_DESC_IN_BILL_LN_V,A.BOLT_DISCOUNT_N,A.LYT_OFFER_FLAG_V,A.VALIDITY_IN_MONTHS,A.SEQ_NO_N,A.VALIDITY_MONTHS_DAYS_V,A.BOLTON_CREDIT_AMT_N,A.DETAIL_DESCRIPTION_V,A.DETAIL_DESCRIPTION_LN_V,A.ADDITIONAL_OFFER_FLAG_V,A.DISPLAY_DESCRIPTION_V,A.DISPLAY_DESCRIPTION_LN_V,A.BOLTON_TYPE_V,A.GIFT_BOLTON_FLAG_V,A.MVPN_OFFER_FLAG_V,A.BOLTON_SPECIFICATION_N,A.OFFER_KEY_CODE_V,A.ADDNL_OFFER_FLG_V,A.START_IP_RANGE_V,A.END_IP_RANGE_V,A.OFFER_CONTRACT_TYPE_V,A.ITEMISED_FLAG_V,A.ITEMISED_CALL_DETAILS_V,A.UPFRONT_BILLING_DISCOUNT_V,A.DISC_AUTHORIZATION_FLG_V,A.WLL_TARIFF_FLAG_V,A.WLL_ZONE_CODE_V,A.CURRENCY_CODE_V,A.TOLL_FREE_FLG_V,A.TOLL_ID_V,A.APN_FLG_V,A.APN_CODE_V,A.F_F_OFFER_V,A.TECH_INFO_V,A.GEN_TYPE_V,A.DATA_VOICE_FLG_V,A.ADDN_CHAR_1_V,A.ADDN_CHAR_2_V,A.ADDN_CHAR_3_V,A.VALIDITY_TYPE_V,TRUNC(A.HARD_TERMINATION_DATE_D),A.OFFER_CLASS_V,A.CONSIDER_DISCOUNT_V,A.DEST_RATING_ZONE_CODE_V,A.MAX_COUNT_N,A.RATE_BY_V,A.SERVICE_TYPE_V,A.SUB_SERVICE_CODE_V,A.SUB_SERV_SPECIFIC_V,A.TARF_FRML_PERC_V,A.PROFILE_ID_V,A.VOIP_OFFER_FLAG_V,A.VOIP_SERV_KEY_V,A.ADDN_CHAR_4_V,A.ADDN_CHAR_5_V,A.ADDN_CHAR_6_V,A.DISP_PRIOR_SEQ_NO_N,A.BUNDLE_SUB_CATG_V,A.SEGMENT_CODE_V,A.SMS_OFFER_CODE_V,A.IVR_SEQUENCE_N,A.BUNDLE_TYPE_V,A.AUTO_RENEWAL_V,A.SDP_CODE_V,A.CYCLIC_PERIOD_N,A.BUSINESS_ID_V,COALESCE(B.AMOUNT_N/100,0) FROM CB_OFFERS A LEFT JOIN CB_SCHEME_CATEGORY_CHRG B
                ON A.SCHEME_REF_CODE_N=B.SCHEME_REF_CODE_N 
                AND B.SUBS_CATEGORY_CODE_V in ('INDVP','DATACARD')
                WHERE 
                A.APPLY_TARIFF_CODE_V IS NOT NULL AND
                A.FAX_DATA_VOICE_SMS_V IS NOT NULL AND
                A.CONTRACT_TYPE_N='P' AND A.STATUS_OPTN_V='A'"""

    oracle_cursor.execute(cbs_query)
    oracle_result = oracle_cursor.fetchall()
    #print (oracle_result)
    # Connect to MySQL
    mysql_connection = pymysql.connect(**db_config)
    mysql_cursor = mysql_connection.cursor()

    combined_data = []
    

    for row in oracle_result:
        apply_tariff_code = row[1]  # Assuming APPLY_TARIFF_CODE_V is at index 2
        
        mysql_query = """select pm.PRODUCT_ID_N ,  pm.DESCRIPTION_V , SUBSTR( ta.ATTRIBUTE_VALUE_V,1,INSTR(ta.ATTRIBUTE_VALUE_V,':')-1)  VALIDITY_TYPE,
                        SUBSTR( ta.ATTRIBUTE_VALUE_V,INSTR(ta.ATTRIBUTE_VALUE_V,':')+1) VALIDITY, wm.DESCRIPTION_V , uct.DESCRIPTION_V , wm.WALLET_ID_N ,
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

        for mysql_row in mysql_data:
            combined_row = list(row) + list(mysql_row)
            combined_data.append(combined_row)

        # Flatten the dictionary values into a single list
    #combined_data = [item for sublist in combined_data_dict.values() for item in sublist]


    # Creating a DataFrame from the combined data
    columns = ["OFFER_DESC_V","TARIFF_CODE_V","OFFER_ST_DATE_D","OFFER_CODE_V","OFFER_END_DATA_D","SERVICE_CODE_V","SCHEME_REF_CODE_N","CONTRACT_TYPE_N","USER_CODE_N","STATUS_OPTN_V","LAST_ACCESSED_BY_N","LAST_ACCESSED_DATE_D","LAUNCHED_BY_N","LAUNCHED_ON_D","VOID_BY_N","VOID_ON_D","VOID_COMMENT_V","PROFIT_CENTER_CODE_V","CARRIER_FLAG_V","CHECK_DEPENDANTS_FLAG_V","CHECK_IMMICAL_FLAG_V","OFFER_TYPE_V","OFFER_GROUP_CODE_V","SHORT_DESC_IN_BILL_V","TARIFF_FLAG_V","APPLY_FOR_SERVICE_TYPES_V","TARIFF_PRIORITY_N","DESTINATION_SERVICE_TYPE_V","FAX_DATA_VOICE_SMS_V","OFFER_FLAG_V","OFFER_DESC_LN_V","SHORT_DESC_IN_BILL_LN_V","BOLT_DISCOUNT_N","LYT_OFFER_FLAG_V","VALIDITY_IN_MONTHS","SEQ_NO_N","VALIDITY_MONTHS_DAYS_V","BOLTON_CREDIT_AMT_N","DETAIL_DESCRIPTION_V","DETAIL_DESCRIPTION_LN_V","ADDITIONAL_OFFER_FLAG_V","DISPLAY_DESCRIPTION_V","DISPLAY_DESCRIPTION_LN_V","BOLTON_TYPE_V","GIFT_BOLTON_FLAG_V","MVPN_OFFER_FLAG_V","BOLTON_SPECIFICATION_N","OFFER_KEY_CODE_V","ADDNL_OFFER_FLG_V","START_IP_RANGE_V","END_IP_RANGE_V","OFFER_CONTRACT_TYPE_V","ITEMISED_FLAG_V","ITEMISED_CALL_DETAILS_V","UPFRONT_BILLING_DISCOUNT_V","DISC_AUTHORIZATION_FLG_V","WLL_TARIFF_FLAG_V","WLL_ZONE_CODE_V","CURRENCY_CODE_V","TOLL_FREE_FLG_V","TOLL_ID_V","APN_FLG_V","APN_CODE_V","F_F_OFFER_V","TECH_INFO_V","GEN_TYPE_V","DATA_VOICE_FLG_V","ADDN_CHAR_1_V","ADDN_CHAR_2_V","ADDN_CHAR_3_V","VALIDITY_TYPE_V","HARD_TERMINATION_DATE_D","OFFER_CLASS_V","CONSIDER_DISCOUNT_V","DEST_RATING_ZONE_CODE_V","MAX_COUNT_N","RATE_BY_V","SERVICE_TYPE_V","SUB_SERVICE_CODE_V","SUB_SERV_SPECIFIC_V","TARF_FRML_PERC_V","PROFILE_ID_V","VOIP_OFFER_FLAG_V","VOIP_SERV_KEY_V","ADDN_CHAR_4_V","ADDN_CHAR_5_V","ADDN_CHAR_6_V","DISP_PRIOR_SEQ_NO_N","BUNDLE_SUB_CATG_V","SEGMENT_CODE_V","SMS_OFFER_CODE_V","IVR_SEQUENCE_N","BUNDLE_TYPE_V","AUTO_RENEWAL_V","SDP_CODE_V","CYCLIC_PERIOD_N","BUSINESS_ID_V", "CHARGE_AMOUNT",
               "PRODUCT_ID_N", "DESCRIPTION_V", "VALIDITY_TYPE", "VALIDITY", "DESCRIPTION_V", "WALLET_TYPE", "WALLET_ID_N", "WALLET_VALUE_MINS_MB"]
    df_combined = pd.DataFrame(combined_data, columns=columns)

    csv_filename= f"OfferWalletDump_{date}.csv"
    downloads_folder = os.path.join(os.path.expanduser("~"), "C:\\Users\\poojaje\\Downloads\\OFFER_DUMP")
    csv_filename_path = os.path.join(downloads_folder, csv_filename)


    df_combined.to_csv(csv_filename_path, index=False)
    csv_filename_path = f"C:\\Users\\poojaje\\Downloads\\OFFER_DUMP\\{csv_filename}"
    print("File Generated Successfully")

    # # SFTP connection details
    # sftp_host = '172.168.101.108'
    # sftp_port = 22  # Default SFTP port
    # sftp_username = 'cbsapp'
    # sftp_password = 'tecnoapp#2024'
    # sftp_remote_dir = '/home/cbsapp/OFFER_DUMP/'

    # SFTP connection 3PP
    # sftp_host = '172.16.112.103'
    # sftp_port = 22  # Default SFTP port
    # sftp_username = 'tlnddatasource'
    # sftp_password = '#4C6p3tl6ev'
    # sftp_remote_dir = '/data01/data_lz/ftpin/WALLET_DUMP/'

    # Connect to SFTP server
    
    # cnopts = pysftp.CnOpts()
    # cnopts.hostkeys = None

    # with pysftp.Connection(sftp_host, username=sftp_username, password=sftp_password, port=sftp_port) as sftp:
    #     # Change to the remote directory on the SFTP server
    #     sftp.chdir(sftp_remote_dir)

    #     # Upload the CSV file to the SFTP server
    #     sftp.put(csv_filename_path)

    #     print(f"CSV file '{csv_filename_path}' uploaded to SFTP successfully.")

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
