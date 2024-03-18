import sys
import pyspark
import boto3
import json
import time
import requests
import pymysql.cursors

from pyspark.sql.functions import * # For lit,col
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import count
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, BooleanType, DecimalType, DoubleType, NumericType, TimestampType
from datetime import datetime
from botocore.exceptions import ClientError
from functools import reduce
from operator import or_

teams_webhook_url = 'https://stacspl.webhook.office.com/webhookb2/762fb81c-96fc-460f-9507-a2b6c39ee968@5f287fe2-70f4-424d-b19b-63ef2d9941f4/IncomingWebhook/37507fd0c1944b15a918b0877bf48c22/819eeaf9-edd7-4e5b-96b6-af92fc061469'

print(sys.argv[1])



## Connection info / Redshift & RDS
def get_connection_info(DBType,region_name,credential_secret_name,parameter_secret_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_credential_secret_value_response = client.get_secret_value(
            SecretId=credential_secret_name
        )
        get_parameter_secret_value_response = client.get_secret_value(
            SecretId=parameter_secret_name
        )


    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    credential_secret = json.loads(get_credential_secret_value_response['SecretString'])
    parameter_secret = json.loads(get_parameter_secret_value_response['SecretString']) 
    if DBType == 'Redshift':
        # Create Redshift connection
        credential = {
            'dbname':parameter_secret['rsdbname'],
            'port':credential_secret['port'],
            'user':credential_secret['username'],
            'password':credential_secret['password'],
            'host':credential_secret['host'],
        }
    if DBType == 'RDS':
        try:
            get_credential_secret_value_response = client.get_secret_value(
                SecretId=credential_secret_name
            )
            get_parameter_secret_value_response = client.get_secret_value(
                SecretId=parameter_secret_name
            )

        except ClientError as e:
            raise e
        
        # Decrypts secret using the associated KMS key.
        credential_secret = json.loads(get_credential_secret_value_response['SecretString'])
        parameter_secret = json.loads(get_parameter_secret_value_response['SecretString']) 
        
        credential = {
            'Cluester':'',
            'dbname':credential_secret['dbname'],
            'port':credential_secret['port'],
            'user':credential_secret['username'],
            'password':credential_secret['password'],
            'host_url':credential_secret['host'],
        }
    return credential
region_name = 'ap-southeast-1'
RDS_credential_secret = "rds/esgpedia/datasync"
RDS_parameter_secret = "rds/esgpedia/datasync"

## get RDS credential
rds_credential = get_connection_info('RDS',region_name,RDS_credential_secret,RDS_parameter_secret)



## Read Data from Databricks test env
tableName = 'company_fact'
tableName_temp = f'{tableName}_temp'
layer = 'fact'
env = 'esgpedia_staging'
appName= "hive_pyspark"
master= "local"

spark = SparkSession.builder \
	.master(master).appName(appName).enableHiveSupport().getOrCreate()
df = spark.sql(f"select * from {env}.{layer}.{tableName}")

## Create a ID for backend
windowSpec = Window.orderBy('company_id','last_update_time')
df_target = df.withColumn('id',row_number().over(windowSpec))

## Define a Specific Schema for Dataframe/RDS
mySchema = StructType(
    [
        StructField("organization_id"                            ,StringType(),True),
        StructField("company_id"                                 ,StringType(),True),
        StructField("company_name"                               ,StringType(),True),
        StructField("company_sector_duns"                        ,StringType(),True),
        StructField("company_sector_naics"                       ,StringType(),True),
        StructField("company_sector_naics_code"                  ,StringType(),True),
        StructField("company_sector_isic"                        ,StringType(),True),
        StructField("company_sector_isic_code"                   ,StringType(),True),
        StructField("company_sector_isic_level_2"                ,StringType(),True),
        StructField("company_sector_isic_level_2_code"           ,StringType(),True),
        StructField("company_segment"                            ,StringType(),True),
        StructField("company_address"                            ,StringType(),True),
        StructField("company_address_latitude"                   ,DecimalType(18,8),True),
        StructField("company_address_longitude"                  ,DecimalType(18,8),True),
        StructField("company_country_of_incorporation"           ,StringType(),True),
        StructField("company_region"                             ,StringType(),True),
        StructField("parent_company_id"                          ,StringType(),True),
        StructField("parent_company_name"                        ,StringType(),True),
        StructField("company_registration_number"                ,StringType(),True),
        StructField("company_registration_number_type"           ,StringType(),True),
        StructField("company_duns_number"                        ,StringType(),True),
        StructField("company_lei_number"                         ,StringType(),True),
        StructField("company_lei_valid_date"                     ,TimestampType(),True),
        StructField("company_website"                            ,StringType(),True),
        StructField("company_phone"                              ,StringType(),True),
        StructField("company_size"                               ,StringType(),True),
        StructField("company_revenue"                            ,StringType(),True),
        StructField("company_revenue_currency"                   ,StringType(),True),
        StructField("profile_process_status"                     ,StringType(),True),
        StructField("profile_verification_type"                  ,StringType(),True),
        StructField("profile_priority"                           ,StringType(),True),
        StructField("last_update_time"                           ,TimestampType(),True),
        StructField("profile_rank"                               ,IntegerType(),True),
        StructField("id"                                         ,IntegerType(),True),

    ])

df_sync = spark.createDataFrame(df_target.rdd,mySchema)


try:
    # Connect to the database
    connection = pymysql.connect(
        host=rds_credential.get('host_url'),
                                user=rds_credential.get('user'),
                                password=rds_credential.get('password'),
                                database=rds_credential.get('dbname'),
                                cursorclass=pymysql.cursors.DictCursor
                                )
except Exception as e:
    # print(e)
    message = {
    "title":str(f"{rds_credential.get('dbname')}:{sys.argv[1]}"), 
    "text": "<strong style='color:red;'>MariaDB Connection Info: "+str(e)+"</strong>"
    }
    #convert to json format
    json_message = json.dumps(message)
    #send post to webhook URL
    response = requests.post(teams_webhook_url, data=json_message, headers={'Content-Type': 'application/json'})
    #check status code if success
    if response.status_code == 200:
        print('Message sent successfully')
    else:
        print(f'Error sending message: {response.text}')
    raise e

# Create a cursor object
mycursor = connection.cursor()


drop_temp=f"drop table if exists {rds_credential.get('dbname')}.{tableName_temp};"
create_temp=f"""create table if not exists {rds_credential.get('dbname')}.{tableName_temp} 
            (
                 organization_id                    varchar(256)
                ,company_id                         varchar(256)
                ,company_name                       text
                ,company_sector_duns                varchar(256)
                ,company_sector_naics               text
                ,company_sector_naics_code          varchar(256)
                ,company_sector_isic                varchar(256)
                ,company_sector_isic_code           varchar(256)
                ,company_sector_isic_level_2        varchar(256)
                ,company_sector_isic_level_2_code   varchar(256)
                ,company_segment                    varchar(256)
                ,company_address                    text
                ,company_address_latitude           decimal(18,6)
                ,company_address_longitude          decimal(18,6)
                ,company_country_of_incorporation   varchar(256)
                ,company_region                     varchar(512)
                ,parent_company_id                  varchar(256)
                ,parent_company_name                varchar(256)
                ,company_registration_number        varchar(256)
                ,company_registration_number_type   varchar(256)
                ,company_duns_number                varchar(256)
                ,company_lei_number                 varchar(256)
                ,company_lei_valid_date             Datetime
                ,company_website                    varchar(256)
                ,company_phone                      varchar(256)
                ,company_size                       varchar(256)
                ,company_revenue                    varchar(256)
                ,company_revenue_currency           varchar(256)
                ,profile_process_status             varchar(256)
                ,profile_verification_type          varchar(256)
                ,profile_priority                   varchar(1)
                ,last_update_time                   Datetime
                ,profile_rank                       integer
                ,id                                 integer

                ,PRIMARY KEY (id)     
            )
            ;
            """      
mycursor.execute(drop_temp)
mycursor.execute(create_temp)

## RDS info
driver = "org.mariadb.jdbc.Driver"
database_host = rds_credential.get('host_url')
database_port = "3306" # update if you use a non-default port
database_name = rds_credential.get('dbname')
table = f"{tableName}_temp"
user = rds_credential.get('user')
password = rds_credential.get('password')
url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}"

## Save Data into RDS
save_table = (df_sync.write
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", table)
  .option("user", user)
  .option("password", password)
  .option("forceSchema", mySchema)
  .mode('append').save()
)

add_index=f"create index {tableName}_index on {rds_credential.get('dbname')}.{tableName_temp}(company_id);"
drop_query=f"drop table if exists {rds_credential.get('dbname')}.{tableName};"
rename_query=f"RENAME TABLE {rds_credential.get('dbname')}.{tableName_temp} TO {rds_credential.get('dbname')}.{tableName};"
try :
    mycursor.execute(add_index)
    mycursor.execute(drop_query)
    mycursor.execute(rename_query)
    status = mycursor.execute(f"SHOW TABLES like '%{tableName}';")
    print(status)
    if status ==1:
        message = {
        "title":str(f"{rds_credential.get('dbname')}:{sys.argv[1]}"), 
        "text": "<strong style='color:green;'>DataSync Status: SUCCESS</strong>"
        }
        #convert to json format
        json_message = json.dumps(message)
        #send post to webhook URL
        response = requests.post(teams_webhook_url, data=json_message, headers={'Content-Type': 'application/json'})
        # Close database connection
        connection.commit()
        mycursor.close()
        connection.close()
    else:
        message = {
        "title":str(f"{rds_credential.get('dbname')}:{sys.argv[1]}"), 
        "text": "<strong style='color:yellow;'>DataSync Status: Fail: "+str(status)+"</strong>"
        }
        #convert to json format
        json_message = json.dumps(message)
        #send post to webhook URL
        response = requests.post(teams_webhook_url, data=json_message, headers={'Content-Type': 'application/json'})
        # Close database connection
        mycursor.close()
        connection.close()
except Exception as e:
    message = {
    "title":str(f"{rds_credential.get('dbname')}:{sys.argv[1]}"), 
    "text": "<strong style='color:red;'>DataSync Status:"+str(e)+"</strong>"
    }
    #convert to json format
    json_message = json.dumps(message)
    #send post to webhook URL
    response = requests.post(teams_webhook_url, data=json_message, headers={'Content-Type': 'application/json'})
    raise e
