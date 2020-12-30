
# this is for python 3.0 and above. use import thread for python2.0 versions

import os
import sys
import time
import threading
import json

# Creates a unique file name for datastore by appending epoch timestamp to the file name


def get_file_name():
    import uuid
    uniq_append_string = uuid.uuid4().hex
    return "LOCAL_STORAGE_{}".format(uniq_append_string)

    # Creates a unique file name with actual path for datastore and return


def get_instance(file_name=None):
    if file_name is None:
        file_name = get_file_name()
    import os

    try:
        os.mkdir('C:/tmp')
    except:
        full_file_name = f"{'C:/tmp'}/{file_name+'.txt'}"
    return full_file_name


# Actual DataStore class
class DataStore:

    # 'd' is the dictionary in which we store data
    def __init__(self, file_descriptor=None):
        self.__lock = threading.Lock()
        if(file_descriptor != None and os.path.isfile(file_descriptor)):
            self.file_descriptor = file_descriptor
        else:
            self.file_descriptor = get_instance()
        self.f = open(self.file_descriptor, 'w+')
        #
        self.d = dict(self.f.read())
    # for create operation
    # use syntax "create(key_name,value,timeout_value)" timeout is optional you can continue by passing two arguments without timeout

    def create(self, key, value, timeout=0):
        self.__lock.acquire()
        try:
            self.key = key
            self.value = value
            self.timeout = timeout
            if self.key in self.d:
                print("error: this key already exists")  # error message1
            else:
                if(self.key.isalpha()):
                    # constraints for file size less than 1GB and Jasonobject value less than 16KB
                    if (sys.getsizeof(self.d) < (1024*1024*1024) and sys.getsizeof(self.value) <= (16000)):
                        if (self.timeout == 0):
                            self.l = [self.value, self.timeout]
                        else:
                            self.l = [self.value, time.time()+self.timeout]

                        if (len(self.key) <= 32):  # constraints for input key_name capped at 32chars
                            self.d[self.key] = self.l
                    else:
                        # error message2
                        print("error: Memory limit exceeded!! ")
                else:
                    # error message3
                    print(
                        "error: Invalind key_name!! key_name must contain only alphabets and no special characters or numbers")
        finally:
            self.__lock.release()

    # for read operation
    # use syntax "read(key_name)"

    def read(self, key):
        self.__lock.acquire()
        try:
            self.json = {}
            self.key = key
            if (self.key not in self.d):
                # error message4
                print(
                    "error: given key does not exist in database. Please enter a valid key")
            else:
                self.b = self.d[key]
                if (self.b[1] != 0):
                    # comparing the present time with expiry time
                    if (time.time() < self.b[1]):
                        # to return the value in the format of JasonObject i.e.,"key_name:value"
                        if(type(self.b[0]) != dict()):
                            res = dict(self.b[0])

                        return res
                    else:
                        print("error: time-to-live of", self.key,
                              "has expired")  # error message5
                else:
                    if(type(self.b[0]) != dict()):
                        res = dict(self.b[0])

                    return res
        finally:
            self.__lock.release()

    # for delete operation
    # use syntax "delete(key_name)"

    def delete(self, key):
        self.key = key
        if (self.key not in self.d):
            # error message4
            raise "error: given key does not exist in database. Please enter a valid key"
        else:
            self.b = self.d[key]
            if (self.b[1] != 0):
                # comparing the current time with expiry time
                if (time.time() < self.b[1]):
                    del self.d[key]
                    print("key is successfully deleted")
                else:
                    print("error: time-to-live of", self.key,
                          "has expired")  # error message5
            else:
                del self.d[key]
                print("key is successfully deleted")
    # it Will save the data in a logical file on path : C:/tmp/*.txt

    def save(self):
        x = json.dumps(self.d, indent=4)
        self.f.write(x)
        self.f.close()
