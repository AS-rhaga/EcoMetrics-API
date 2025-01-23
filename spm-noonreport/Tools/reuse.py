from typing import Tuple
import zipfile
import urllib
import boto3
import json
import os
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from botocore.exceptions import ClientError

logger = logging.getLogger('LoggingTest')

class S3ConfigTools:
    def __init__(self) -> None:
        self.s3_client = boto3.client("s3")
    
    def s3_put_event_catch(self,event: dict) -> Tuple[str,str]:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='cp932')
        return bucket, key
        
    def s3_file_download(self,bucket: str, key: str, extension: str) -> None:
        self.s3_client.download_file(bucket, key, f'/tmp/file.{extension}')
        
    def zipfile_search(self) -> list:
        zip_f = zipfile.ZipFile('/tmp/file.zip')

        # ZIPの中身を取得
        lst = zip_f.namelist()
        
        return lst
    
    def zip_unpacking_selectfile(self,filename: str) -> None:
        with zipfile.ZipFile("/tmp/file.zip") as zf:
            zf.extract(filename, "/tmp")
            
    def s3_zip_create(self, out_key: str, out_name: str) -> str:
        with zipfile.ZipFile(f"/tmp/{out_key}", "w", compression=zipfile.ZIP_DEFLATED) as f:
            f.write(f"/tmp/{out_name}.dat", arcname=f"{out_name}.dat")
        
        return f"/tmp/{out_key}"
    
    def s3_zip_output(self,out_bucket: str, out_key: str, out_name: str) -> None:
        self.s3_zip_create(out_key, out_name)
        
        with open(f"/tmp/{out_key}", "rb") as f:
            self.s3_client.upload_fileobj(f, out_bucket, f"{out_key}")
    
class SnsConfigTools:
    def sns_s3_put_event_catch(self,event: dict) -> Tuple[str,str]:
        message = json.loads(event['Records'][0]['Sns']['Message'])
        bucket = message['Records'][0]['s3']['bucket']['name']
        key = message['Records'][0]['s3']['object']['key']
        
        return bucket, key

class SesConfigTools:
    def ses_send_email(self,to_address: str, path: str, mail_users: str) -> None:
        myapi_domain = "@massa-config-api.link"
        SENDER = mail_users + myapi_domain
        RECIPIENT = to_address
        CONFIGURATION_SET = ""
        AWS_REGION = "ap-northeast-1"
        SUBJECT = "reply mail"
        ATTACHMENT = path
        BODY_TEXT = ""
        BODY_HTML = """\
        <html>
        <head></head>
        <body>
        </body>
        </html>
        """
        CHARSET = "utf-8"
        client = boto3.client('ses',region_name=AWS_REGION)
        msg = MIMEMultipart('mixed')
        msg['Subject'] = SUBJECT 
        msg['From'] = SENDER 
        msg['To'] = RECIPIENT
        msg_body = MIMEMultipart('alternative')
    
        textpart = MIMEText(BODY_TEXT.encode(CHARSET), 'plain', CHARSET)
        htmlpart = MIMEText(BODY_HTML.encode(CHARSET), 'html', CHARSET)
    
        msg_body.attach(textpart)
        msg_body.attach(htmlpart)
        
        att = MIMEApplication(open(path, 'rb').read())
    
        att.add_header('Content-Disposition','attachment',filename=os.path.basename(ATTACHMENT))
    
        msg.attach(msg_body)
        
        msg.attach(att)
    
        try:
            response = client.send_raw_email(
                Source=SENDER,
                Destinations=[
                    RECIPIENT
                ],
                RawMessage={
                    'Data':msg.as_string(),
                },
                ConfigurationSetName=CONFIGURATION_SET
            )
        # Display an error if something goes wrong.	
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])

class DynamoDBConfigTools:
    def __init__(self, tablename: str) -> None:
        self.dynamodb = boto3.resource("dynamodb")
        self.table = self.dynamodb.Table(tablename)
        
    def dynamo_get_item(self,key: dict) -> str:
        try:
            response = self.table.get_item(
                Key=key
            )
        except ClientError as err:
            logger.error(
                "Couldn't get movie %s from table %s. Here's why: %s: %s",
                key,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return response
    
    def dynamo_put_item(self,item: dict) -> None:
        try:            
            self.table.put_item(
                Item = item
            )
        except ClientError as err:
            logger.error(
                "Couldn't add movie %s to table %s. Here's why: %s: %s",
                item,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    
    def dynamo_update_item(self,item: dict) -> str:
        try:
            response = self.table.update_item(
                Item = item
            )
        except ClientError as err:
            logger.error(
                "Couldn't update movie %s in table %s. Here's why: %s: %s",
                item,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return response["Attributes"]
    
    def dynamo_delete_item(self,key: dict) -> None:
        try:
            self.table.delete_item(
                Key=key
            )
        except ClientError as err:
            logger.error(
                "Couldn't delete movie %s. Here's why: %s: %s",
                key,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
    