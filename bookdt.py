# -*- coding: utf-8 -*-
"""
Created on Fri Jan  1 12:52:14 2021

@author: hemuarun
"""
import json
import time
import threading
import concurrent.futures    


class BookdtClass:
    def __init__(self):
      self.book = {}   #to store data in dictionary
 # Freshworks – Backend Assignment

#  fileName - bookdt.py
#  Requirement - To build a file-based key-value data store that supports the basic CRD (create, read, and delete) operations.
    def create(self, key, value, time_to_live=0): 
          t1 = threading.Thread(target=(self.create_method), args=(key, value, time_to_live),daemon=True)
          t1.start()
    
 
    def create_method(self, key, value, time_to_live = 0):
          if key in self.book:
              print('error message: the key you gave already exists.') 
          else:
              if len(self.book) < 1024*1024*1024 and len(value) <= 16384: 
                  if time_to_live == 0:
                      data = {'value': value, 'time_to_live': time_to_live}
                  else:
                      data = {'value': value, 'time_to_live': time.time() + time_to_live}
                  if len(key) <= 32:   
                      self.book[key] = data   
                      with open("book.json", "w") as outfile:
                         outfile.write(json.dumps(self.book))
                  else:
                      print('ERROR limit exceeded')
              else:
                  print('ERROR MESSAGE: key must contain only alphabet')
        
#by using read operation
#by using exception error #by using concurrent futures module which helps in launching parallel tasks.
# #to perform utility tasks.
    def read(self, key):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = executor.submit(self.read_method, key)
            return futures.result()
    
#to perform read operation
    def read_method(self, key):
        try:
            with open("book.json", "r") as openfile:
               self.book = json.load(openfile)
        except Exception:
            print('the file is not found.')
        if key not in self.book:
            print('ERROR MESSAGE: key does not exist.')
            return
        else:
            data = self.book[key]
            if data['time_to_live'] != 0:
                if time.time() < data['time_to_live']:
                   return json.dumps({key: self.book[key]['value']})
                else:
                    print('ERROR:duration of the key' + ' had been expired')
                    return
            else:
                data = self.book[key]
                return json.dumps({key: self.book[key]['value']})
                
#delete operation
    def delete(self, key):
        t3 = threading.Thread(target=(self.delete_method), args=(key,),daemon=True)
        t3.start()

#method for performing delete operation
    def delete_method(self, key):
        try:
            with open("book.json", "r") as openfile:
                self.book = json.load(openfile)
        except Exception:
              print('The file is not found.')
        if key not in self.book:
                print('ERROR MESSAGE: key does not exist')
        else:
           data = self.book[key]
           if data['time_to_live'] != 0:
               if time.time() < data['time_to_live']:
                   del self.book[key]
                   with open("book.json", "w") as outfile:
                       outfile.write(json.dumps(self.book))
                       print('HURRAY!! : The key ' + key + 'is deleted successfully.')
               else:
                   print('ERROR MESSAGE: Time of key ' + key + 'had been expired.')
           else:
                del self.book[key]
                with open("book.json", "w") as outfile:
                    outfile.write(json.dumps(self.book))
                    print('HURRAY!! : The key ' + key + 'is deleted successfully.')
