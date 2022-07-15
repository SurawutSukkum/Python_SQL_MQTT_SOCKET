import paho.mqtt.publish as publish
import paho.mqtt.subscribe as subscribe

import socket
import time
from datetime import datetime
import json

import csv
import os.path
from pathlib import Path

import os
import logging
import pyodbc
import sys
import clr

print("User Current Version:-", sys.version)
try:
    print("start")
    HOST_socket = ''
    PORT_socket = 5432
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST_socket, PORT_socket))
    s.listen()

    hostname = ''
    port = 1883
    auth = {
        'username': '',
        'password': ''
    }
except OSError as err:
    print("OS error: {0}".format(err))
except ValueError:
    print("Could not convert data to an integer.")
except BaseException as err:
    print(f"Unexpected {err=}, {type(err)=}")
    raise

print("start")
# current date and time
now = datetime.now()
timestamp = datetime.fromtimestamp(datetime.timestamp(now))
timestamp = timestamp.strftime("%Y-%m-%d-%H%M%S")
logging.basicConfig(filename=timestamp +'.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')
logging.warning('This will get logged to a file')
logging.warning("start")

# current date and time
print("waiting for connection")
logging.warning("waiting for connection")

def Treacibility_insert_data(Dser_num,Dstation_name,DStation_Desc,Dent_time,Dext_time):
    assembly_path = r'D:\PackingInspection'
    sys.path.append(assembly_path)
    clr.AddReference("KETL_DLL")
    from KETL_DLL import Main_Function
    bc = Main_Function()

    ser_num = Dser_num
    station_name = Dstation_name
    Station_Desc = DStation_Desc
    ent_time = Dent_time
    ext_time = Dext_time
    #print(bc.Backcheck_Data(ser_num, station_name))
    rt_insert = bc.Insert_Process_Data(ser_num, station_name, Station_Desc, ent_time, ext_time, 1)
    if rt_insert == True:
        print("insert complete!!")
        logging.warning("insert complete!!" + ext_time )
    else:
        print("insert does not complete!!")
        logging.warning("insert does not complete!!" + ext_time )

def mqtt_send(object,data):
    publish.single(object, data, hostname=hostname, port=port, auth=auth)

def chk_file():
    if path.is_file() == False:
        with open(file_timestamp + '.csv', 'w') as csv_file:
            fieldnames = ['final_status', 'Unit', 'Unit_msg', 'Power_supply', 'Power_supply_msg', 'Tube',
                          'Tube_msg', 'Label', 'Label_msg', 'Filter', 'Filter_msg', 'Bag', 'Bag_msg', 'Bag1',
                          'Bag1_msg', 'Bag2',
                          'Bag2_msg',
                          'Timestamp']
            test_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            test_writer.writeheader()
            print("Create new file" + file_timestamp + '.csv')
            logging.warning("Create new file" + file_timestamp + '.csv')

def file_update(status,Unit,Unit_file,Power_supply,Power_supply_file,Tube,Tube_file,Label,Label_file,Filter,Filter_file,Bag,Bag_file,Bag1,Bag1_file,Bag2,Bag2_file,timestamp):
    with open(file_timestamp + '.csv', 'a') as csv_file:
        # test_writer = csv.writer(test_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        fieldnames = ['final_status', 'Unit', 'Unit_msg', 'Power_supply', 'Power_supply_msg', 'Tube',
                      'Tube_msg', 'Label', 'Label_msg', 'Filter', 'Filter_msg', 'Bag', 'Bag_msg', 'Bag1',
                      'Bag1_msg', 'Bag2',
                      'Bag2_msg',
                      'Timestamp']
        test_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        test_writer.writerow(
            {'final_status': status, 'Unit': Unit, 'Unit_msg': Unit_file, 'Power_supply': Power_supply,
             'Power_supply_msg': Power_supply_file, 'Tube': Tube, 'Tube_msg': Tube_file,
             'Label': Label, 'Label_msg': Label_file, 'Filter': Filter, 'Filter_msg': Filter_file, 'Bag': Bag,
             'Bag_msg': Bag_file, 'Bag1': Bag1, 'Bag1_msg': Bag1_file, 'Bag2': Bag2, 'Bag2_msg': Bag2_file,
             'Timestamp': timestamp})
        logging.warning("Update file completed "+status)

def file_update_sql(Serial_No,status,Unit,Unit_file,Power_supply,Power_supply_file,Tube,Tube_file,Label,Label_file,Filter,Filter_file,Bag,Bag_file,Bag1,Bag1_file,Bag2,Bag2_file):
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=;"
                          "Database=;"
                          "UID=;"
                          "PWD=;")
    cursor = cnxn.cursor()
    logging.warning("Connect to SQL")
    print("Connect to SQL")
    logging.warning("Update " + status)
    final_status = status
    # current date and time
    now_sql = datetime.now()
    timestamp_sql = datetime.fromtimestamp(datetime.timestamp(now_sql))
    timestamp_sql = timestamp_sql.strftime("%Y-%m-%d %H:%M:%S")
    print(timestamp_sql)
    logging.warning('timestamp: ' + timestamp_sql)
    count = cursor.execute("""INSERT INTO PACKING_HLA_PH1 (Serial_No, Final_status, Unit, Unit_msg, Power_supply, Power_supply_msg, Tube, Tube_msg, Label,
            Label_msg, Filter, Filter_msg, Bag, Bag_msg, Bag1, Bag1_msg, Bag2, Bag2_msg,Timestamp) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
                           , Serial_No, final_status, Unit, Unit_file, Power_supply, Power_supply_file,
                           Tube, Tube_file, Label, Label_file, Filter, Filter_file, Bag, Bag_file, Bag1,
                           Bag1_file, Bag2, Bag2_file, timestamp_sql).rowcount

    cnxn.commit()
    logging.warning('SQL save complete')
    logging.warning('Rows inserted: ' + str(count))
    print('Rows inserted: ' + str(count))

def chk_update_timestamp():
    update = datetime.now()
    update = datetime.fromtimestamp(datetime.timestamp(update))
    update = update.strftime("%Y-%m-%d %H:%M:%S")
    return update

def chk_timestamp():
    global file_timestamp
    global path
    global Serial_No
    global timestamp
    # current date and time
    now = datetime.now()
    timestamp = datetime.fromtimestamp(datetime.timestamp(now))
    timestamp = timestamp.strftime("%d%m%Y%H%M%S")
    file_timestamp = datetime.fromtimestamp(datetime.timestamp(now))
    file_timestamp = file_timestamp.strftime("%d%m%Y")
    path_to_file = file_timestamp + '.csv'
    path = Path(path_to_file)
    Serial_No = str(timestamp)
    #print(timestamp)

def mqtt_read():
    global pythonObj
    msg4 = subscribe.simple("countS", hostname=hostname, port=port, auth=auth)
    # print(" %s " % ( msg4.payload.decode("utf-8")))
    text = msg4.payload.decode("utf-8")
    # print(text)
    # json string

    # parse json file
    pythonObj = json.loads(text)
    # print(type(pythonObj))
    list_4 = pythonObj['DeepDetect']['obj_count_str']
    return list_4

class objectDefine:
    def __init__(self, name):
        self.name = name
        self.__amount = 0
        self.__image = ""

    def readStatus(self, obj):
        if obj.find(self.name) >= 0:
           if  self.__amount == 0:
               self.__amount = 1
               mqtt_send(self.name, "OK")
               obj_file = pythonObj['ImageCapture']['file_name']
               self.__image = obj_file
               print(self.__image)
               mqtt_send("image_file", self.__image)
               logging.warning("Found="+self.name)
           return 1
        else:
           return 0

    def readImage(self):
        logging.warning("Image name= "+self.__image)
        return self.__image

    def readStatusOnc(self):
        return self.__amount

    def reset(self):
        logging.warning("Reset= "+self.name)
        mqtt_send(self.name, " ")
        mqtt_send("rst_obj_"+self.name, " ")
        self.__amount = 0
        self.__image = ""

def chk_total():
    if obj_Unit.readStatusOnc() == 1:
        if obj_Bag1.readStatusOnc() == 1 & obj_PowerSupply.readStatusOnc() == 1 & obj_Tube.readStatusOnc() == 1 & obj_Filter.readStatusOnc() == 1 & obj_Label.readStatusOnc() == 1:
            mqtt_send("final_status", "OK")
            mqtt_send("time", timestamp)
            file_update("OK",obj_Unit.readStatusOnc(),obj_Unit.readImage(),obj_PowerSupply.readStatusOnc(),obj_PowerSupply.readImage(),obj_Tube.readStatusOnc(),obj_Tube.readImage(),obj_Label.readStatusOnc(),obj_Label.readImage(),obj_Filter.readStatusOnc(),obj_Filter.readImage(),obj_Bag.readStatusOnc(),obj_Bag.readImage(),obj_Bag1.readStatusOnc(),obj_Bag1.readImage(),obj_Bag2.readStatusOnc(),obj_Bag2.readImage(),timestamp)
            file_update_sql(Serial_No,"OK",obj_Unit.readStatusOnc(),obj_Unit.readImage(),obj_PowerSupply.readStatusOnc(),obj_PowerSupply.readImage(),obj_Tube.readStatusOnc(),obj_Tube.readImage(),obj_Label.readStatusOnc(),obj_Label.readImage(),obj_Filter.readStatusOnc(),obj_Filter.readImage(),obj_Bag.readStatusOnc(),obj_Bag.readImage(),obj_Bag1.readStatusOnc(),obj_Bag1.readImage(),obj_Bag2.readStatusOnc(),obj_Bag2.readImage())
            time.sleep(3)
            obj_Bag1.reset()
            obj_PowerSupply.reset()
            obj_Tube.reset()
            obj_Filter.reset()
            obj_Label.reset()
            obj_Unit.reset()
            time.sleep(3)
            print(timestamp)
            Treacibility_insert_data("MDH00000001", "MAN_HSG01", "MANUAL INSTALL COVER LABEL", chk_update_timestamp(),
                                     chk_update_timestamp())
        else:
            mqtt_send("final_status", "NG")
            mqtt_send("time", timestamp)
            file_update("NG",obj_Unit.readStatusOnc(),obj_Unit.readImage(),obj_PowerSupply.readStatusOnc(),obj_PowerSupply.readImage(),obj_Tube.readStatusOnc(),obj_Tube.readImage(),obj_Label.readStatusOnc(),obj_Label.readImage(),obj_Filter.readStatusOnc(),obj_Filter.readImage(),obj_Bag.readStatusOnc(),obj_Bag.readImage(),obj_Bag1.readStatusOnc(),obj_Bag1.readImage(),obj_Bag2.readStatusOnc(),obj_Bag2.readImage(),timestamp)
            file_update_sql(Serial_No,"NG",obj_Unit.readStatusOnc(),obj_Unit.readImage(),obj_PowerSupply.readStatusOnc(),obj_PowerSupply.readImage(),obj_Tube.readStatusOnc(),obj_Tube.readImage(),obj_Label.readStatusOnc(),obj_Label.readImage(),obj_Filter.readStatusOnc(),obj_Filter.readImage(),obj_Bag.readStatusOnc(),obj_Bag.readImage(),obj_Bag1.readStatusOnc(),obj_Bag1.readImage(),obj_Bag2.readStatusOnc(),obj_Bag2.readImage())

            time.sleep(3)
            obj_Bag1.reset()
            obj_PowerSupply.reset()
            obj_Tube.reset()
            obj_Filter.reset()
            obj_Label.reset()
            obj_Unit.reset()
            print(timestamp)
            Treacibility_insert_data("MDH00000001", "MAN_HSG01", "MANUAL INSTALL COVER LABEL", chk_update_timestamp(),
                                     chk_update_timestamp())

if __name__ == "__main__":
    #START programm

    obj_Bag = objectDefine("Bag")
    obj_Bag.reset()
    obj_Bag1 = objectDefine("Bag1")
    obj_Bag1.reset()
    obj_Bag2 = objectDefine("Bag2")
    obj_Bag2.reset()
    obj_PowerSupply = objectDefine("PowerSupply")
    obj_PowerSupply.reset()
    obj_Tube = objectDefine("Tube")
    obj_Tube.reset()
    obj_Filter = objectDefine("Filter")
    obj_Filter.reset()
    obj_Label = objectDefine("Label")
    obj_Label.reset()
    obj_Unit = objectDefine("Unit")
    obj_Unit.reset()

    print("node-red open")

    while True:

            # create timestamp
            chk_timestamp()
            # create file
            chk_file()

            # read MQTT
            data = mqtt_read()
            print("Bag1        = ",obj_Bag1.readStatus(data),obj_Bag1.readStatusOnc(), obj_Bag1.readImage())
            print("PowerSupply = ",obj_PowerSupply.readStatus(data),obj_PowerSupply.readStatusOnc(),obj_PowerSupply.readImage())
            print("Tube        = ",obj_Tube.readStatus(data), obj_Tube.readStatusOnc(),obj_Tube.readImage())
            print("Filter      = ",obj_Filter.readStatus(data), obj_Filter.readStatusOnc(),obj_Filter.readImage())
            print("Label       = ",obj_Label.readStatus(data), obj_Label.readStatusOnc() ,obj_Label.readImage())
            print("Unit        = ",obj_Unit.readStatus(data),obj_Unit.readStatusOnc(),obj_Unit.readImage())
            chk_total()


