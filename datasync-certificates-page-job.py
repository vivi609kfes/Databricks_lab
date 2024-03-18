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
tableName_cert = 'certificates_fact'
tableName_cert_temp = f'{tableName_cert}_temp'
tableName_company = 'company_fact'
tableName_company_temp = f'{tableName_company}_temp'
tableName = 'certificates_page'
tableName_temp = f'{tableName}_temp'
layer = 'fact'
env = 'esgpedia_staging'
appName= "hive_pyspark"
master= "local"

spark = SparkSession.builder \
	.master(master).appName(appName).enableHiveSupport().getOrCreate()
df_cert = spark.sql(f"select * from {env}.{layer}.{tableName_cert}")

spark = SparkSession.builder \
	.master(master).appName(appName).enableHiveSupport().getOrCreate()
df_company = spark.sql(f"select * from {env}.{layer}.{tableName_company}")
## Create a ID for backend

df = df_cert.join(df_company,df_cert.company_id ==  df_company.company_id,"left").select(df_cert["*"],df_company["company_name"])
windowSpec = Window.orderBy('esgpedia_certificate_id','esgpedia_certificate_start_date')
df_target = df.withColumn('id',row_number().over(windowSpec))

## Define a Specific Schema for Dataframe/RDS
mySchema = StructType(
    [
        StructField("company_id"                                  ,StringType(),True),
        StructField("company_name_wip"                            ,StringType(),True),
        StructField("original_certificate_id"                     ,StringType(),True),
        StructField("esgpedia_certificate_id"                     ,StringType(),True),
        StructField("certificate_framework"                       ,StringType(),True),
        StructField("certificate_sector"                          ,StringType(),True),
        StructField("certificate_type"                            ,StringType(),True),
        StructField("certificate_level"                           ,StringType(),True),
        StructField("certificate_scope"                           ,StringType(),True),
        StructField("certificate_esg_category"                    ,StringType(),True),
        StructField("asset"                                       ,StringType(),True),
        StructField("asset_type"                                  ,StringType(),True),
        StructField("asset_address"                               ,StringType(),True),
        StructField("asset_address_latitude"                      ,DoubleType(),True),
        StructField("asset_address_longitude"                     ,DoubleType(),True),
        StructField("certificate_country_name"                    ,StringType(),True),
        StructField("certificate_country_code"                    ,StringType(),True),
        StructField("certificate_issuer"                          ,StringType(),True),
        StructField("original_certificate_status"                 ,StringType(),True),
        StructField("esgpedia_certificate_status"                 ,StringType(),True),
        StructField("original_certificate_start_date"             ,TimestampType(),True),
        StructField("original_certificate_expiry_date"            ,TimestampType(),True),
        StructField("esgpedia_certificate_start_date"             ,TimestampType(),True),
        StructField("esgpedia_certificate_expiry_date"            ,TimestampType(),True),
        StructField("company_name"                                ,StringType(),True),
        StructField("id"                                          ,IntegerType(),True),
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
                 company_id                       varchar(256)  
                ,company_name_wip                 text 
                ,original_certificate_id          text
                ,esgpedia_certificate_id          varchar(256)  
                ,certificate_framework            varchar(256)  
                ,certificate_sector               varchar( 20)  
                ,certificate_type                 varchar(256)  
                ,certificate_level                varchar(256)  
                ,certificate_scope                varchar( 27)  
                ,certificate_esg_category         varchar(256)  
                ,asset                            text          
                ,asset_type                       text          
                ,asset_address                    text          
                ,asset_address_latitude           decimal(18,8) 
                ,asset_address_longitude          decimal(18,8) 
                ,certificate_country_name         varchar(256)  
                ,certificate_country_code         varchar(500)  
                ,certificate_issuer               text          
                ,original_certificate_status      text          
                ,esgpedia_certificate_status      varchar(23 )  
                ,original_certificate_start_date  Datetime     
                ,original_certificate_expiry_date Datetime     
                ,esgpedia_certificate_start_date  Datetime     
                ,esgpedia_certificate_expiry_date Datetime     
                ,company_name                     text
                ,id                               integer(11) 
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
