from Config import config

class info:
    def __init__(self,db_name,table_name):
        self.db_name = config["database"]
        self.table_name = config["table"]

# main python program execution
if __name__ == '__main__':
    print("this will trigger the whole python"
          "script")