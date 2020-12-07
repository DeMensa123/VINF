# VINF

## Zadanie projektu
Sparsovanie entít Person, vytvorenie jednoduchej služby 'mohli sa stretnúť?', ktorá po zadaní dvoch mien určí, či sa mohli dané osoby stretnúť (prekryv času ich života). Spark


## Spustenie programu
### pyspark.py
Prvý skript pyspark.py obsahuje filtrovanie vstupného súboru (freebase dumpu) pomocou Sparku a následné vytvorenie výstupného adresára obsahujúceho výstupné súbory vo formáte .csv.

Súbor pyspark.py je potrebné spustiť nasledovným príkazom:

```
spark-submit pyspark.py <mode> <input_file> <number_of_partitions> <ouput_directory> <master> <spark_executor_uri>
```


mode - ak program chcete spustiť lokálne zadajte 1, ak na klastri zadajte 2 \
input_file - vstupný súbor \
number_of_partitions - počet výstupných súborov \
output_directory - názov výstupného adresára, kam budú uložené vytvorené súbory \
master - master URL pre klaster \
spark_executor_uri - je potrebné zadať len na klastri


_Príklad spustenia lokálne_
```
spark-submit pyspark.py 1 freebase-head-100000 10 output_dir local[*] 
```

Ak sa vstupný súbor nachádza v inom adresári ako súbor pyspark.py je potrebné zadať celú cestu k súboru. To isté platí pre výstupný adresár.

_Príklad spustenia na klastri_
```
spark-submit pyspark.py 2 freebase-head-100000 10 output_dir <master> <path_to_spark>
```

### whoosh_search.py
Ďalší skript (whoosh_search.py) obsahuje funkcie na indexovanie a vyhľadávanie pomocou knižnice Whoosh. Daný skript je možné spustiť nasledujúcim príkazom:
```
python whoosh_search.py <dir_to_index>
```

dir_to_index - vstupný adresár obsahujúci .csv súbory vytvorené pomocou Sparku

### main.py
Posledný skript (main.py) obsahujé hlavnú funkcionalitu pre porovnanie dátumov dvoch osôb. Skript je možné spustiť nasledujúcim príkazom:
```
python main.py
```

Po spustení skriptu je potrebné zadať 2 mená osôb. Po ich zadaní sa vykoná funkcia vyhľadávania, ktoré prebieha v skripte whoosh_search.py vo funkcii search(name_1, name_2, index). Ak pre jedno zo zadaných mien neboli nájdené žiadne výsledky, vykoná sa len funkcia print_people, ktorá vypíše nájdené výsledky pre druhé meno. Ak neboli nájdené výsledky ani pre zadané jedno meno, vypíše sa len informácia o tom, že hľadané osoby nie sú v databáze dostupné. Ak boli nájdené výsledky, vykoná sa funkcia check_dates(result_1, result_2), v ktorej sú porovnané dátumy jednotlivých osôb.