import os
import numpy as np
import pandas as pd
from datetime import datetime
import re
#import math

#Data Loading
cust_data= pd.read_csv("D:\\Users\\kasrikan\\Desktop\\Cust_dataset.csv",encoding = "ISO-8859-1")

#Data information 
cust_data.dtypes
cust_data.head(5)
cust_data.describe()
cust_data.info()
cust_data.shape

#Rule-1
cust_data = cust_data[pd.notnull(cust_data['TTLEA521'])]

#Rule-2
cust_data = cust_data[pd.notnull(cust_data['SUR1A521'])]

#Rule-3
cust_data = cust_data[pd.notnull(cust_data['ADD1A521'])]

#Rule-4
cust_data = cust_data[pd.notnull(cust_data['IDTPA521'])]

#Rule-5
cust_data = cust_data[pd.notnull(cust_data['IDNOA521'])]

#Rule-6
cust_data = cust_data[pd.notnull(cust_data['BRDTA521'])]

#Rule-7
cust_data = cust_data[pd.notnull(cust_data['CSEXA521'])]

#Rule-8
cust_data = cust_data[pd.notnull(cust_data['SICCA521'])]

#Rule-9
cust_data['TTLEA521'] = pd.to_numeric(cust_data['TTLEA521'],errors='coerce')

#Rule-10
for i in range(0,len(cust_data)):
    cust_data['SUR1A521'][i] = str(re.sub('[^0-9a-zA-Z]+',' ', str(cust_data['SUR1A521'][i])))
    print(i)

#Rule-11
for i in range(0,len(cust_data)):
    cust_data['ADD1A521'][i]=re.sub('[^0-9a-zA-Z]+',' ', str(cust_data['ADD1A521'][i]))
#Rule- 12
cust_data['IDTPA521'] = pd.to_numeric(cust_data['IDTPA521'])

#Rule- 13
for i in range(0,len(cust_data)):
    if len(str(cust_data['IDNOA521'][i])) != 13:
        cust_data['IDNOA521'][i]=None
        print(i)
cust_data = cust_data[pd.notnull(cust_data['IDNOA521'])] 

#Rule-  14
#cust_data['BRDTA521'] = pd.to_datetime(cust_data['BRDTA521'],format='%Y%m%d',errors='ignore')

#Rule-15
for i in range(0,len(cust_data)):
    if cust_data['CSEXA521'][i]!=1  or cust_data['CSEXA521'][i]!= 2:
        cust_data['CSEXA521'][i]= None
        print(i)
    
cust_data = cust_data[pd.notnull(cust_data['CSEXA521'])]

#Rule-16
cust_data['SICCA521'] = pd.to_numeric(cust_data['SICCA521'])


cust_data.to_csv("D:\\Users\\kasrikan\\Desktop\\Cleaned_Cust_data.csv")