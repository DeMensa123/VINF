#!/usr/bin/env python3

import re
import datetime
import sys
from os import listdir
import os 
from os.path import isfile, join
from whoosh.index import *

import whoosh_search as whoosh

def check_dates(result_1, result_2):
    note = "Lutujeme, ale datum narodenia ani umrtia hladanej osoby nie je znamy !" 
    checked_people_1 = []

    for x in result_1:
        checked_people_2 = []
        id_1, name_1, alias_1, dob_1, dod_1, b_note_1, d_note_1 = x['id'], x['name'], x['alias'], x['birth'], x['death'], x['b_note'], x['d_note']

        if id_1 not in checked_people_1:
            for y in result_2:
                id_2, name_2, alias_2, dob_2, dod_2, b_note_2, d_note_2 = y['id'], y['name'], y['alias'], y['birth'], y['death'], y['b_note'], y['d_note'] 
            
                if id_2 not in checked_people_2 and id_2 not in checked_people_1 and id_1 != id_2:
                                         
                    if b_note_1 != "" and d_note_1 != "":         
                        print(name_1 +" (alias: " + alias_1 + ") - " + note)
                        
                        if b_note_2 != "" and d_note_2 != "":
                            print(name_2 +" (alias: " + alias_2 + ") - " + note)
                        else:
                            print(name_2 +" (alias: " + alias_2 + ") * " + dob_2 + " - † " + dod_2 + " " + b_note_2 + "" + d_note_2)
                        
                    else:
                        print(name_1 +" (alias: " + alias_1 + ") * " + dob_1 + " - † " + dod_1 + " " + b_note_1 + "" + d_note_1)

                        if b_note_2 != "" and d_note_2 != "":
                            print(name_2 +" (alias: " + alias_2 + ") - " + note)
                        else:
                            print(name_2 +" (alias: " + alias_2 + ") * " + dob_2 + " - † " + dod_2 + " " + b_note_2 + "" + d_note_2)
                            
                            if dod_1 >= dob_2 and dob_1 <= dod_2:
                                print("Osoby sa mohli stretnut.")
                            else:
                                print("Osoby sa nemohli stretnut.")

                    checked_people_2.append(id_2)
                    print()

        checked_people_1.append(id_1)


def print_people(result):
    note = "Lutujeme, ale datum narodenia ani umrtia hladanej osoby nie je znamy !" 
    checked_people = []
    
    for x in result:
        id, name, alias, dob, dod, b_note, d_note = x['id'], x['name'], x['alias'], x['birth'], x['death'], x['b_note'], x['d_note']
        
        if id not in checked_people:
            if b_note != "" and d_note != "":
                print(name +" (alias: " + alias + ") - " + note)
            else:
                print(name +" (alias: " + alias + ") * " + dob + " - † " + dod + " " + b_note + "" + d_note)
            checked_people.append(id)


if __name__ == '__main__':
    name_1 = input("Zadajte meno prvej osoby: ")
    name_2 = input("Zadajte meno druhej osoby: ")

    if name_1 == "" or name_2 == "":
        print("Nezadali ste mena!")
        exit()

    # open existing index
    ix = open_dir("index")
    result_1, result_2 = whoosh.search(name_1, name_2, ix)

    if len(result_1) == 0 and len(result_2) == 0:
        print("Lutujeme, ale osoby '" + name_1 + "' a '" + name_2 + "' nie su v nasej databaze.")
        
    elif len(result_1) == 0:
        print("Lutujeme, ale osoba '" + name_1 + "' nie je v nasej databaze.")
        print_people(result_2)
        
    elif len(result_2) == 0:
        print("Lutujeme, ale osoba '" + name_2 + "' nie je v nasej databaze.")
        print_people(result_1)
    else:
        check_dates(result_1, result_2)




