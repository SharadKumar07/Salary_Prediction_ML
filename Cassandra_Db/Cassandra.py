import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from Logger.log import Logs
import os as os
from os import listdir
import pandas as pd
import shutil

dblog = Logs("train.log")

class dBOperation:
    """
      This class shall be used for handling all the cassandra db operations.
      """

    def __init__(self):
        self.path = 'Training_Database/'
        self.goodFilePath = "Training data/adult"
        self.logger =Logs
        self.secure_connect_bundle = 'secure-connect-adult-census-db.zip'
        self.client_id = 'XbTAgWktXwNbmIlqNEvYyfPX'
        self.client_secret = 'gbL0C4YW,xtz,ywQiZs+,c.u4tS644Qs4IqS-rmPftg9MJsJv.CQbQFCi4E7fh.M3HYCFWJ+KnbKC5mGP,GfcKD8Yo3B2r.E4eX6kJHS0Pa3KgNZhUzsE0CjqSZ7O2aU'

    def dataBaseConnection(self):

        """
                Method Name: dataBaseConnection
                Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
                Output: Connection to the DB
                On Failure: Raise ConnectionError
                """
        try:
            cloud_config = {'secure_connect_bundle': self.secure_connect_bundle}


            auth_provider = PlainTextAuthProvider(self.client_id, self.client_secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
            row = session.execute("select release_version from system.local").one()

            file = open("Training_Logs/CassandraConnectionLog.txt", 'a+')
            dblog.log("Cassandra database connection successful")
            file.close()
        except ConnectionError:
            file = open("Training_Logs/CassandraConnectionLog.txt", 'a+')
            dblog.log( "Error while connecting to database: %s" % ConnectionError)
            file.close()
            raise ConnectionError

        return session

    def createTableDb(self):
        """
                        Method Name: createTableDb
                        Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
                        Output: None
                        On Failure: Raise Exception
                        """
        try:

            session = self.dataBaseConnection()
            try:

                age = 'age'
                FNWEIGHT='FNWEIGHT'
                EDUCATION_NUMBER='EDUCATION-NUMBER '
                HOURS_PER_WEEK='HOURS-PER-WEEK'
                NET_CAPITAL_LOSS_GAIN='NET-CAPITAL-LOSS-GAIN'
                WORKCLASS='WORKCLASS'
                MARITAL_STATUS='MARITAL-STATUS'
                RACE='RACE'
                sex = 'sex'


                session.execute(
                    f"CREATE TABLE db.Good_Raw_Data({age} {int} PRIMARY KEY,{FNWEIGHT} {int},{EDUCATION_NUMBER} {int},{HOURS_PER_WEEK} {int},{NET_CAPITAL_LOSS_GAIN} {int},{WORKCLASS} {int},{MARITAL_STATUS} {int},{RACE} {int}, {sex} {int});")
                file = open("Training_Logs/CassandraTableLog.txt", 'a+')
                dblog.log("Tables created successfully!!")
                dblog.log("Database closed successfully")
                session.shutdown()
                file.close()
            except:
                file = open("Training_Logs/CassandraTableLog.txt", 'a+')
                dblog.log("Table already present in database")
                dblog.log( "Database closed successfully")
                session.shutdown()
                file.close()

        except Exception as e:
            file = open("Training_Logs/CassandraTableLog.txt", 'a+')
            dblog.log("Error while creating table: %s " % e)
            file.close()
            file = open("Training_Logs/CassandraTableLog.txt", 'a+')
            dblog.log("Database closed successfully")
            #session.shutdown()
            file.close()
            raise e

    def insertIntoTableGoodData(self):

        """
                               Method Name: insertIntoTableGoodData
                               Description: This method inserts the Good data files from the Good_Raw folder into the
                                            above created table.
                               Output: None
                               On Failure: Raise Exception
        """

        session = self.dataBaseConnection()
        goodFilePath = self.goodFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("Training_Logs/CassandraInsertionLog.txt", 'a+')

        age = 'age'
        FNWEIGHT = 'FNWEIGHT'
        EDUCATION_NUMBER = 'EDUCATION-NUMBER '
        HOURS_PER_WEEK = 'HOURS-PER-WEEK'
        NET_CAPITAL_LOSS_GAIN = 'NET-CAPITAL-LOSS-GAIN'
        WORKCLASS = 'WORKCLASS'
        MARITAL_STATUS = 'MARITAL-STATUS'
        RACE = 'RACE'
        sex = 'sex'

        for file in onlyfiles:
            try:
                data = pd.read_csv(goodFilePath + '/' + file)
                for i, row in data.iterrows():

                    query = f"insert into db.Good_Raw_Data ({age}, {FNWEIGHT}, {EDUCATION_NUMBER},{HOURS_PER_WEEK},{NET_CAPITAL_LOSS_GAIN},{WORKCLASS},{MARITAL_STATUS},{RACE},{sex}) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                    try:
                        session.execute(query, tuple(row))
                        dblog.log(" File loaded successfully!!")

                    except Exception as e:
                        raise e

            except Exception as e:
                dblog.log("Error while inserting data into table: %s " % e)
                log_file.close()
                session.shutdown()

        session.shutdown()
        log_file.close()

    def selectingDatafromtableintocsv(self):

        """
                               Method Name: selectingDatafromtableintocsv
                               Description: This method exports the data in GoodData table as a CSV file. in a given
                                            location.
                               Output: None
                               On Failure: Raise Exception
        """

        self.fileFromDb = 'Training_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
        try:
            session = self.dataBaseConnection()

            main_list = []
            for i in session.execute("select * from db.Good_Raw_Data;"):
                main_list.append(i)

            # Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # converting main_list to data frame
            df = pd.DataFrame(main_list)

            # saving the data frame df to output directory

            df.to_csv(f"{self.fileFromDb}" + '//' + f"{self.fileName}", index=False)

            dblog.log("File exported successfully!!!")
            log_file.close()

        except Exception as e:
            dblog.log( "File exporting failed. Error : %s" % e)
            log_file.close()

    def TurncateTable(self):
        """
                               Method Name: TurncateTable
                               Description: This method delete the data in GoodData table.
                               Output: None
                               On Failure: Raise Exception
        """

        try:
            session = self.dataBaseConnection()
            session.execute("TRUNCATE TABLE db.Good_Raw_Data;")

            file = open("Training_Logs/CassandraTableLog.txt", 'a+')
            dblog.log("Tables turncated successfully!!")
            file.close()
            session.shutdown()

        except Exception as e:
            file = open("Training_Logs/CassandraTableLog.txt", 'a+')
            dblog.log("Table Turncate failed. Error : %s" % e)
            file.close()
          #  session.shutdown()