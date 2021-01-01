# -*- coding: utf-8 -*-
"""
Created on Fri Jan  1 12:53:27 2021

@author: hemuarun
"""
from bookdt import *
b = BookdtClass()
#create
b.create('ikigai', 'Hector Garcia', 135)

#read
print(b.read('ikigai'))

#delete
b.delete('ikigai')

#bookdt.create('ikigai','hector garcia', 135)
#displayerror