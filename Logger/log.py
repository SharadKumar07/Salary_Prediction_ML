from datetime import datetime

class Logs:

    def __init__(self, file):
        self.filename = file
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d <> %H:%M:%S")
        file_obj = open(self.filename, "a+")
        file_obj.write("\n"+ current_time+ "<:>" +"New Logger instance created !\n\n")
        file_obj.close()

    def log(self, log_message):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S")
        logfile = open(self.filename, "a+")
        logfile.write(
            str(self.date) + "/" + str(self.current_time) + "\t\t" + log_message + "\n")
        logfile.close()

    def addLog(self, log_level, log_message):
        print("Logger file : Logs class")
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d <> %H:%M:%S")
        logfile = open(self.filename, "a+")
        logfile.write(current_time + " <:> " + log_level + " <:> " + log_message + "\n")
        logfile.close()
