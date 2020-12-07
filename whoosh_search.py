#!/usr/bin/env python3

from whoosh.index import *
from whoosh.fields import *
from whoosh.analysis import *
from whoosh.support.charset import accent_map
from whoosh.qparser import QueryParser
from whoosh.qparser import MultifieldParser
import os
from os import listdir
from os.path import isfile, join
import csv

# to run the script use this commmand:
# python whoosh_search.py <dir_to_index>
# dir_to_index - input directory which contains files you want to index 


# index each file in file directory 
def index_files(file_path, file_list):
    schema = Schema(id = TEXT(stored=True), name = TEXT(stored=True), alias = TEXT(stored=True), \
                    birth = TEXT(stored=True), death = TEXT(stored=True), b_note = TEXT(stored=True), d_note = TEXT(stored=True))

    if not os.path.exists("index"):
        os.mkdir("index")

    ix = create_in("index", schema)
    ix = open_dir("index")

    writer = ix.writer()

    for file in file_list:
        csv_file = open(file_path + "/" + file, 'r', encoding = 'utf8') 
        data = csv.reader(csv_file, delimiter = ',')

        for line in data:
            writer.add_document(
                id = line[0],
                name = line[1],
                alias = line[2],
                birth = line[3],
                death = line[4],
                b_note = line[5],
                d_note = line[6]
            )

        csv_file.close()   

    writer.commit()
    return ix

def search(name_1, name_2, ix):

    mparser = MultifieldParser(["name", "alias"], schema= ix.schema)

    query_1 = mparser.parse("*" + name_1 + "*")
    query_2 = mparser.parse("*" + name_2 + "*")

    s = ix.searcher()
    result_1 = s.search(query_1, limit = None)
    result_2 = s.search(query_2, limit = None)
    
    return result_1, result_2


if __name__ == '__main__':

    if len(sys.argv) > 1:
        file_path = sys.argv[1] 
        file_list = [f for f in listdir(file_path)]

        ix = index_files(file_path, file_list)
    else:
        print("Nezadali ste adresar, ktory chcete indexovat")
        exit()