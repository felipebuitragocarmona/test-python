import boto3
import json
from RefactoredCode.DatabaseConnection import DatabaseConnection
class IndividualNotifications():
    def __init__(self,message):
        """
        Constructor de la clase para las notificaciones,
        recibe como parámetro el mensaje genérico que será
        enviado de manera individual a los suscriptores
        :param message:
        """
        self.message=message
        config = self.loadFileConfig()
        self.fileName=config["data_base_profile"]["name_file"]
        newDBConnection = DatabaseConnection(config["data_base"])
        self.cnx = newDBConnection.openConnection()

        self.client = boto3.client("sns",
                                   TargetArn=arn,
                                   region_name=config["sns"]["region_name"])

    def sendMesagge(self,message, number):
        """
        Activación del servicio SNS de envio de mensajes de texto
        :param message: Mensaje a enviar
        :param number: número telefónico del receptor
        """
        self.client.publish(PhoneNumber=number, Message=message)
    def prepareMessageByProfile(self,user_profile, subscription_type):
        """
        Según el perfil de usuario, obtiene la información de todos los
        suscriptores a dicho perfil y por cada uno de los números
        telefónicos registrados prepara  un mensaje personalizado e invoca
        la función correspondiente para llevar a cabo el proceso
        :param user_profile: identificador del perfil en cuestión
        :param subscription_type: nombre detallado de la suscripción
        """
        dic = {}
        query = "select * from users where profile =%(the_profile)s"
        cursor = self.cnx.cursor()
        cursor.execute(query, {'the_profile': user_profile})
        for (first_name, last_name, phone_numbers, service_link) in cursor:
            for number in phone_numbers:
                query2 = "select number from phone_number where phone_id=%(the_number)s"
                cursor.execute(query2, {'the_number': number})
                dic['table'] = cursor.fetchall()
                message = self.formatMessage([first_name,last_name,subscription_type,service_link])
                self.sendMesagge(message, dic['table'][0]);
    def formatMessage(self, params_message):
        """
        A partir de un mensaje con las palabra reservada "$param$"
        las reemplaza  por la información enviada en la lista "params_message"
        :param params_message: listado de datos que serán posicionados
        en orden de aparición en el mensaje original
        :return: mensaje personalizado con los parámetros correspondientes
        """
        try:
            i = 0
            newMessage = ""
            for word in self.message.split(" "):
                if word == "$param$":
                    newMessage += str(params_message[i]) + " "
                    i = i + 1
                else:
                    newMessage += word + " "
        except IndexError:
            print("Error: The amount of data sent does not match the amount of data to replace")
        return self.message
    def loadFileConfig(self):
        """
        Carga el archivo de configuración con las constantes
        que se requieren para el funcionamiento del código
        :return: un diccionario el cual posee la estructura del json
        con la información de las constantes
        """
        with open('config.json') as f:
            data = json.load(f)
        return data
    def startProcessForProfiles(self,profiles_to):
        """
        Inicia el proceso de envio de los mensajes personalizados
        a cada uno de los usuarios suscriptores del servicio
        :param profiles_to: listado de nombres de perfiles a los que
        se requiere enviar la notificación
        """
        f = open(self.fileName, "r");
        Lines = f.readlines()
        for line in Lines:
            record = line.split(";");
            subscription_type=record[0].split(" ")[1]
            if record[0] in profiles_to:
                self.prepareMessageByProfile(record[1],subscription_type)

