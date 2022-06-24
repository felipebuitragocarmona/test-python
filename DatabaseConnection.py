import base64
import boto3
import pymysql
import json
from botocore.exceptions import ClientError


class DatabaseConnection():
    def __init__(self,config):
        """
        :param config: configuración básica que será necesaria para establecer la
        conexión
        """
        self.host = config['HOST']
        self.name = config['USERNAME']
        self.db_name = config['DB_NAME']
        self.secret_name = config['SECRET_NAME']
        my_session = boto3.session.Session()
        self.region_name = my_session.region_name
        self.conn=None
    def openConnection(self):
        """
        Establece la conexión con la base de datos, para esto utiliza
        "secrete manager" para llevar a cabo la negociación
        :return: conexión de la base datos
        """
        print("In Open connection")
        password = "None"
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=self.region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(SecretId=self.secret_name)
            print(get_secret_value_response)
        except ClientError as e:
            print(e)
        else:
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                j = json.loads(secret)
                password = j['password']
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                print("password binary:" + decoded_binary_secret)
                password = decoded_binary_secret.password
        try:
            if (self.conn is None):
                self.conn = pymysql.connect(self.host,
                                            user=self.name,
                                            passwd=password,
                                            db=self.db_name,
                                            connect_timeout=5)
            elif (not self.conn.open):
                # print(conn.open)
                self.conn = pymysql.connect(self.host,
                                            user=self.name,
                                            passwd=password,
                                            db=self.db_name,
                                            connect_timeout=5)
        except Exception as e:
            print(e)
            print("ERROR: Unexpected error: Could not connect to MySql instance.")
            raise e
        return self.conn

