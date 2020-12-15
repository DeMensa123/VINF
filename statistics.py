#!/usr/bin/env python3

import sys
from os import listdir
import os 
from os.path import isfile, join
import csv
import pandas as pd

# to run the script use this commmand:
# python statistics.py <input_dir> 

file_path = sys.argv[1] 
file_list = [f for f in listdir(file_path)]

dates_0 = 0
dates_birth_none = 0
dates_death_none = 0
dates_2 = 0

for file in file_list:
    csv_file = open(file_path + "/" + file, 'r', encoding = 'utf8') 
    data = csv.reader(csv_file, delimiter = ',')

    for line in data:    
        id = line[0]
        name = line[1]
        alias = line[2]
        birth = line[3]
        death = line[4]
        b_note = line[5]
        d_note = line[6]

        if b_note == '' and d_note == '':
            dates_2 += 1
        elif d_note == '' and b_note != '':
            dates_birth_none += 1
        elif b_note == '' and d_note != '':
            dates_death_none += 1
        else:
            dates_0 += 1
    
    csv_file.close()   
    
dates = {'Osoby': [
            'Celkovy pocet osob',
            'Pocet osob, ktore maju uvedene obidva datumy', 
            'Pocet osob, ktore nemaju uvedeny datum narodenia', 
            'Pocet osob, ktore nemaju uvedeny datum umrtia', 
            'Pocet osob, ktore nemaju uvedeny ani jeden datum'
        ],
        'Pocet' :[0, dates_2, dates_birth_none, dates_death_none, dates_0]
        
}

df = pd.DataFrame(dates, columns = ['Osoby', 'Pocet'])

df.loc[0,'Pocet'] = df.loc[1:,'Pocet'].sum(axis=0) # sum
df['%'] = round(df.loc[0:,'Pocet']/df.loc[0,'Pocet']*100,2) # percentage

with pd.option_context('display.max_rows', None, 'display.max_columns', df.shape[1]):
    print(df)

