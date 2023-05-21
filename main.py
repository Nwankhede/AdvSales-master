from Config import config
from DataLoad import preprocessing as pre
from connection import conn
from ddlscripts import ddl as dd

# main python program execution
if __name__ == '__main__':
    print("this will trigger the whole python script")
    #
    obj = pre.data_preprocessing()
    obj.schema_clean()
    obj.stage_load_table()

    #trigger dim load

