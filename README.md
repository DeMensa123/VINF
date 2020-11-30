# VINF

### Zadanie projektu
Sparsovanie entít Person, vytvorenie jednoduchej služby 'mohli sa stretnúť?', ktorá po zadaní dvoch mien určí, či sa mohli dané osoby stretnúť (prekryv času ich života). Spark


### Spustenie programu
Na spustenie časti projektu, v ktorej sa dáta filtrujú pomocou Sparku stačí spustiť súbor pyspark.py nasledovným príkazom:

```
spark-submit pyspark.py <master> <input_file> <number_of_partitions> <ouput_directory>
```

master – master URL pre klaster \
input_file – vstupný súbor \
number_of_partitions – počet výstupných súborov \
output_directory – názov výstupného adresára, kam budú uložené vytvorené súbory


#### Príklad:
```
spark-submit pyspark.py local[*] freebase-head-100000 10 spark_output
```

Ak sa vstupný súbor nachádza v inom adresári ako súbor pyspark.py je potrebné zadať celú cestu k súboru. To isté platí pre výstupný adresár.
