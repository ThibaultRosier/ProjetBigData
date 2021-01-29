

# PROJET BIG DATA													

## LEXA  Corentin
## ROSIER Thibault

### Partie 1

#### Question 1

```scala

//Recupere le fichier
val myData = sc.textFile("C:\\Users\\thiba\\Desktop\\Cours\\bigdata\\Projet\\data-netflows.txt")

// Organise les données en dataframe ou chaques données separé par une ',' se retrouve dans une colonne differentes
val Dataframe = myData.map(_.split(',')).map(c=> {(c(0),c(1),c(2),c(3),c(4),c(5),c(6),c(7),c(8))}).toDF()

//Caste les colonnes et les renommes
var df = Dataframe.withColumn("_1", Dataframe("_1").cast(TimestampType)).withColumnRenamed("_1", "timestamp")
  .withColumn("_2", Dataframe("_2").cast(IntegerType)).withColumnRenamed("_2", "duree")
  .withColumn("_3", Dataframe("_3").cast(StringType)).withColumnRenamed("_3", "PC_src")
  .withColumn("_4", Dataframe("_4").cast(StringType)).withColumnRenamed("_4", "PORT_src")
  .withColumn("_5", Dataframe("_5").cast(StringType)).withColumnRenamed("_5", "PC_dest")
  .withColumn("_6", Dataframe("_6").cast(StringType)).withColumnRenamed("_6", "PORT_dest")
  .withColumn("_7", Dataframe("_7").cast(StringType)).withColumnRenamed("_7", "protocole")
  .withColumn("_8", Dataframe("_8").cast(IntegerType)).withColumnRenamed("_8", "NB_paquets")
  .withColumn("_9", Dataframe("_9").cast(IntegerType)).withColumnRenamed("_9", "NB_octets")

df.show(5)

```


|          timestamp|duree|PC_src|PORT_src|PC_dest|PORT_dest|protocole|NB_paquets|NB_octets|
|-------------------|-----|------|--------|-------|---------|---------|----------|---------|
|2010-10-10 18:30:48|    0| C1065|     389|  C3799|   N10451|        6|        10|     5323|
|2010-10-10 18:30:48|    0| C1423|   N1136|  C1707|       N1|        6|         5|      847|
|2010-10-10 18:30:48|    0| C1423|   N1142|  C1707|       N1|        6|         5|      847|
|2010-10-10 18:30:48|    0|C14909|   N8191|  C5720|     2049|        6|         1|       52|
|2010-10-10 18:30:48|    0|C14909|   N8192|  C5720|     2049|        6|         1|       52|

___

#### Question 2

Calcule le nombres d'occurrence des colonnes de type STRING.
On regroupe par la colonne à compté pour ne pas les compté plusieurs fois et on les affiches. 
`val countPC_src = df.groupBy("PC_src").count().show(5)`

|PC_src| count|
|------|------|
| C2320|135370|
| C1804| 10970|
| C1875|  2396|
| C1524|  5546|
|  C650|  5769|



`val countPort_src = df.groupBy("PORT_src").count().show(5)`

|PORT_src|count|
|--------|-----|
|   N7751| 1291|
|  N10831|  927|
|  N10504|  977|
|  N10979| 1019|
|  N10368| 1092|



`val countPC_dest = df.groupBy("PC_dest").count().show(5)`

|PC_dest| count|
|-------|------|
|  C2320|144895|
|  C1804| 11788|
|  C1875|  2812|
|  C1524|  5546|
|   C650|  5787|



`val countPort_dest = df.groupBy("PORT_dest").count().show(5)`

|PORT_dest|count|
|---------|-----|
|    N7751| 1199|
|   N10831|  863|
|   N10979|  863|
|   N10368|  913|
|   N10863| 1141|



`val countPrtcl = df.groupBy("protocole").count().show(5)`

|protocole|   count|
|---------|--------|
|       17| 5657380|
|        6|62284239|
|       41|     108|
|        1| 2035685|

---

#### Question 3

Calcule la moyenne, le minimum et le maximum des colonnes de type INTEGER.
On sélectionne la colonne, en faisant le calcul voulu dessus et on les affiches. 
`val moyDuree = df.select(mean(df("duree"))).show(5)`

|       avg(duree)|
|-----------------|
|9.473825282363972|



`val minDuree = df.select(min(df("duree"))).show(5)` 

|min(duree)|
|----------|
|         0|



`val maxDuree = df.select(max(df("duree"))).show(5)`

|max(duree)|
|----------|
|        75|

  
`val moyNbPaquet = df.select(mean(df("NB_paquets"))).show(5)`

|   avg(NB_paquets)|
|------------------|
|212.32167312789446|



`val minNbPaquet = df.select(min(df("NB_paquets"))).show(5)` 

|min(NB_paquets)|
|---------------|
|              1|



`val maxNbPaquet = df.select(max(df("NB_paquets"))).show(5)`
  
|max(NB_paquets)|
|---------------|
|        2169178|



`val moyNbOctet = df.select(mean(df("NB_octets"))).show(5)`

|    avg(NB_octets)|
|------------------|
|168525.09339474657|


`val minNbOctet = df.select(min(df("NB_octets"))).show(5)`

|min(NB_octets)|
|--------------|
|            46|



`val maxNbOctet = df.select(max(df("NB_octets"))).show(5)`

|max(NB_octets)|
|--------------|
|    2147133532|


---

#### Question 4

Définie une fenêtre temporelle de 5 min sur le timestamp
`val w = window($"timestamp", "5 Minute")`

On groupe par timestamp via la fenêtre et par la colonnes à compter, on trie par le timestamp et on les affiches.
`val countPC_src = df.groupBy(w,col("PC_src")).count().orderBy("window").show(5)`

|              window|PC_src|count|
|--------------------|------|-----|
|[2010-10-10 18:30...| C1322|    7|
|[2010-10-10 18:30...| C4098|    5|
|[2010-10-10 18:30...| C3471|    3|
|[2010-10-10 18:30...| C4005|    3|
|[2010-10-10 18:30...| C2150|    4|


  
`val countPort_src = df.groupBy(w,col("PORT_src")).count().orderBy("window").show(5)`  

|              window|PORT_src|count|
|--------------------|--------|-----|
|[2010-10-10 18:30...|   N5566|    2|
|[2010-10-10 18:30...|  N10869|    1|
|[2010-10-10 18:30...|    N305|    1|
|[2010-10-10 18:30...|   N9996|    1|
|[2010-10-10 18:30...|    N647|    1|



`val countPC_dest = df.groupBy(w,col("PC_dest")).count().orderBy("window").show(5)`  

|              window|PC_dest|count|
|--------------------|-------|-----|
|[2010-10-10 18:30...|   C608|    1|
|[2010-10-10 18:30...|   C505|    2|
|[2010-10-10 18:30...|  C1322|    7|
|[2010-10-10 18:30...|  C3387|    1|
|[2010-10-10 18:30...|  C1935|    2|



`val countPort_dest = df.groupBy(w,col("PORT_dest")).count().orderBy("window").show(5)`  

|              window|PORT_dest|count|
|--------------------|---------|-----|
|[2010-10-10 18:30...|   N10076|    1|
|[2010-10-10 18:30...|     N644|    1|
|[2010-10-10 18:30...|   N11121|    1|
|[2010-10-10 18:30...|   N11082|    1|
|[2010-10-10 18:30...|    N5085|    1|



`val countPrtcl = df.groupBy(w,col("protocole")).count().orderBy("window").show(5)` 
  
|              window|protocole|count|
|--------------------|---------|-----|
|[2010-10-10 18:30...|       17|  897|
|[2010-10-10 18:30...|        1|  460|
|[2010-10-10 18:30...|        6| 5676|
|[2010-10-10 18:35...|       17|  961|
|[2010-10-10 18:35...|        1|  397|

Ici presque pareil :
On groupe par timestamp via la fenêtre et on calcul la colonne voulu, on trie par le timestamp et on les affiches.
`val moyDuree = df.groupBy(w).mean("duree").orderBy("window").show(5)`  

|              window|        avg(duree)|
|--------------------|------------------|
|[2010-10-10 18:30...|  9.02715768519835|
|[2010-10-10 18:35...| 9.381232421375607|
|[2010-10-10 18:40...| 9.196126202947983|
|[2010-10-10 18:45...|10.396515311510031|
|[2010-10-10 18:50...|10.630743651753326|



`val minDuree = df.groupBy(w).min("duree").orderBy("window").show(5)`  

|              window|min(duree)|
|--------------------|----------|
|[2010-10-10 18:30...|         0|
|[2010-10-10 18:35...|         0|
|[2010-10-10 18:40...|         0|
|[2010-10-10 18:45...|         0|
|[2010-10-10 18:50...|         0|



`val maxDuree = df.groupBy(w).max("duree").orderBy("window").show(5)`  

|              window|max(duree)|
|--------------------|----------|
|[2010-10-10 18:30...|        72|
|[2010-10-10 18:35...|        72|
|[2010-10-10 18:40...|        72|
|[2010-10-10 18:45...|        72|
|[2010-10-10 18:50...|        72|



`val moyNbPaquet = df.groupBy(w).mean("NB_paquets").orderBy("window").show(5)`

|              window|   avg(NB_paquets)|
|--------------------|------------------|
|[2010-10-10 18:30...| 188.6163799232191|
|[2010-10-10 18:35...|217.50498593710049|
|[2010-10-10 18:40...|195.71177975392862|
|[2010-10-10 18:45...| 209.8334213305174|
|[2010-10-10 18:50...| 257.8627569528416|

`val minNbPaquet = df.groupBy(w).min("NB_paquets").orderBy("window").show(5)`  

|              window|min(NB_paquets)|
|--------------------|---------------|
|[2010-10-10 18:30...|              1|
|[2010-10-10 18:35...|              1|
|[2010-10-10 18:40...|              1|
|[2010-10-10 18:45...|              1|
|[2010-10-10 18:50...|              1|


`val maxNbPaquet = df.groupBy(w).max("NB_paquets").orderBy("window").show(5)`  
  
|              window|max(NB_paquets)|
|--------------------|---------------|
|[2010-10-10 18:30...|          48587|
|[2010-10-10 18:35...|          48586|
|[2010-10-10 18:40...|          48587|
|[2010-10-10 18:45...|          48586|
|[2010-10-10 18:50...|          48586|


`val moyNbOctet = df.groupBy(w).mean("NB_octets").orderBy("window").show(5)`  

|              window|    avg(NB_octets)|
|--------------------|------------------|
|[2010-10-10 18:30...|178076.27001279683|
|[2010-10-10 18:35...|212816.39695729993|
|[2010-10-10 18:40...|191889.32086734072|
|[2010-10-10 18:45...|205749.59978880675|
|[2010-10-10 18:50...|253427.48171100364|


`val minNbOctet = df.groupBy(w).min("NB_octets").orderBy("window").show(5)`  

|              window|min(NB_octets)|
|--------------------|--------------|
|[2010-10-10 18:30...|            46|
|[2010-10-10 18:35...|            46|
|[2010-10-10 18:40...|            46|
|[2010-10-10 18:45...|            46|
|[2010-10-10 18:50...|            46|


`val maxNbOctet = df.groupBy(w).max("NB_octets").orderBy("window").show(5)`

|              window|max(NB_octets)|
|--------------------|--------------|
|[2010-10-10 18:30...|      67524687|
|[2010-10-10 18:35...|      67524647|
|[2010-10-10 18:40...|      67526037|
|[2010-10-10 18:45...|      67524647|
|[2010-10-10 18:50...|      67524647|

---

#### Question 5

Calcule le nombre d'occurrence par PC source puis la somme, le minimum, le maximum et la moyenne des autres colonnes de type INTEGER.
`val countSRC = df.groupBy("PC_src").count().show(5)`

|PC_src| count|
|------|------|
| C2320|135370|
| C1804| 10970|
| C1875|  2396|
| C1524|  5546|
|  C650|  5769|


  
`val sumSRC = df.groupBy("PC_src").sum().show(5)`  

|PC_src|sum(duree)|sum(NB_paquets)|sum(NB_octets)|
|------|----------|---------------|--------------|
| C2320|    342081|         511568|     177696669|
| C1804|     72473|         698848|      89046233|
| C1875|     27065|          23004|       7261921|
| C1524|      1436|           7138|        640705|
|  C650|      1240|           8764|        848341|



`val minSRC = df.groupBy("PC_src").min().show(5)`  

|PC_src|min(duree)|min(NB_paquets)|min(NB_octets)|
|------|----------|---------------|--------------|
| C2320|         0|              1|            52|
| C1804|         0|              1|            46|
| C1875|         0|              1|            46|
| C1524|         0|              1|            46|
|  C650|         0|              1|            46|



`val maxSRC = df.groupBy("PC_src").max().show(5)`  

|PC_src|max(duree)|max(NB_paquets)|max(NB_octets)|
|------|----------|---------------|--------------|
| C2320|        59|             60|         74537|
| C1804|        62|           5911|       1010194|
| C1875|        61|           1141|        199100|
| C1524|        63|            661|        157736|
|  C650|        61|            897|        144233|



`val moySRC = df.groupBy("PC_src").mean().show(5)`

|PC_src|         avg(duree)|   avg(NB_paquets)|    avg(NB_octets)|
|------|-------------------|------------------|------------------|
| C2320|  2.527007461032725|3.7790352367585136|1312.6739233212677|
| C1804|  6.606472196900638| 63.70537830446673| 8117.250045578851|
| C1875| 11.295909849749583| 9.601001669449081|  3030.85183639399|
| C1524| 0.2589253516047602| 1.287053732419762|  115.525604038947|
|  C650|0.21494193101057377|1.5191540994973132| 147.0516553995493|


---

#### Question 6

Calcule le nombre d'occurrence de : port source , PC destination, port destination et protocole pour chaque PC source.
```scala
    val countPortSRC = df.groupBy("PC_src").agg(count("PORT_src")).show()
    val countDEST = df.groupBy("PC_src").agg(count("PC_dest")).show()
    val countPortDEST = df.groupBy("PC_src").agg(count("PORT_dest")).show()
    val countProtocole = df.groupBy("PC_src").agg(count("protocole")).show()
    
   ```

Nous avons le même tableau pour chaque dataframe  car chaque pc source a le même nombre de port source , PC destination, port destination et protocole.

|PC_src| count|
|------|------|
| C2320|135370|
| C1804| 10970|
| C1875|  2396|
| C1524|  5546|
|  C650|  5769|


---

#### Question 7

Récupère et affiche les 10 échanges les plus fréquents.
On groupe par PC source et destination, cela représente un échange, on trie par la colonne count, on de limite au 10 premier résultat et on les affiches.
`val top10 = df.groupBy("PC_src","PC_dest").count().sort(desc("count")).limit(10).show()`

|PC_src|PC_dest| count|
|------|-------|------|
| C6181|  C1549|483599|
| C6144|  C1549|483108|
| C1707| C12598|336789|
|C12598|  C1707|336785|
| C6123|  C1549|292761|
|C17693|  C5074|176237|
| C6785|  C1549|170962|
| C6055|  C1549|157137|
| C1685|  C5638|150217|
|  C467|  C2320|141628|



---

#### Question 8

On groupe par timestamp via la fenêtre et par PC source, on applique le calcul voulu, on trie par le timestamp et on les affiches.
`val countSRC2 = df.groupBy(w,col("PC_src")).count().orderBy("window").show(5)`  

|              window|PC_src|count|
|--------------------|------|-----|
|[2010-10-10 18:30...| C1322|    7|
|[2010-10-10 18:30...| C4098|    5|
|[2010-10-10 18:30...| C3471|    3|
|[2010-10-10 18:30...| C4005|    3|
|[2010-10-10 18:30...| C2150|    4|



`val sumSRC2 = df.groupBy(w,col("PC_src")).sum().orderBy("window").show(5)`  

|              window|PC_src|sum(duree)|sum(NB_paquets)|sum(NB_octets)|
|--------------------|------|----------|---------------|--------------|
|[2010-10-10 18:30...|  C608|        11|              9|          2945|
|[2010-10-10 18:30...| C3471|       103|             12|           834|
|[2010-10-10 18:30...| C1322|        55|             25|          6236|
|[2010-10-10 18:30...| C3387|         0|              1|            46|
|[2010-10-10 18:30...| C1935|         0|             10|          1714|



`val minSRC2 = df.groupBy(w,col("PC_src")).min().orderBy("window").show(5)`  

|              window|PC_src|min(duree)|min(NB_paquets)|min(NB_octets)|
|--------------------|------|----------|---------------|--------------|
|[2010-10-10 18:30...|  C608|        11|              9|          2945|
|[2010-10-10 18:30...| C3471|        34|              4|           278|
|[2010-10-10 18:30...| C1322|         0|              1|            52|
|[2010-10-10 18:30...| C3387|         0|              1|            46|
|[2010-10-10 18:30...| C1935|         0|              5|           857|



`val maxSRC2 = df.groupBy(w,col("PC_src")).max().orderBy("window").show(5)`  

|              window|PC_src|max(duree)|max(NB_paquets)|max(NB_octets)|
|--------------------|------|----------|---------------|--------------|
|[2010-10-10 18:30...| C1322|        55|             13|          4237|
|[2010-10-10 18:30...| C4098|        23|             17|          4349|
|[2010-10-10 18:30...| C3471|        35|              4|           278|
|[2010-10-10 18:30...| C4005|        11|             17|          4349|
|[2010-10-10 18:30...| C2150|         0|             15|         14960|


`val moySRC2 = df.groupBy(w,col("PC_src")).mean().orderBy("window").show(5)`  
  
|              window|PC_src|        avg(duree)|   avg(NB_paquets)|    avg(NB_octets)|
|--------------------|------|------------------|------------------|------------------|
|[2010-10-10 18:30...| C1322| 7.857142857142857|3.5714285714285716| 890.8571428571429|
|[2010-10-10 18:30...| C4098|              10.2|               6.8|            1140.0|
|[2010-10-10 18:30...| C3471|34.333333333333336|               4.0|             278.0|
|[2010-10-10 18:30...| C4005|3.6666666666666665| 6.666666666666667|1509.6666666666667|
|[2010-10-10 18:30...| C2150|               0.0|              11.0|           10216.5|


```scala
    val countPortSRC = df.groupBy(w,col("PC_src")).agg(count("PORT_src")).show(5)
    val countDEST = df.groupBy(w,col("PC_src")).agg(count("PC_dest")).show(5)
    val countPortDEST = df.groupBy(w,col("PC_src")).agg(count("PORT_dest")).show(5)
    val countProtocole = df.groupBy(w,col("PC_src")).agg(count("protocole")).show(5)
    
   ```  

Comme Pour la question 6 nous avons les meme redultat pour les 4 dataframes

|              window|PC_src|count|
|--------------------|------|-----|
|[2010-10-10 18:30...|  C608|    1|
|[2010-10-10 18:30...| C3471|    3|
|[2010-10-10 18:30...| C1322|    7|
|[2010-10-10 18:30...| C3387|    1|
|[2010-10-10 18:30...| C1935|    2|

___
___

### Partie 2

#### Question 1

```scala
//Calcule le nombre de connexion entre un PC source et un PC destination  
val nbConnexion = df.groupBy("PC_src", "PC_dest").count().as("NBConnexions")  
nbConnexion.withColumn("PC_src, PC_dest", concat(col("PC_src"), lit(", " ), col("PC_dest")))  
  .drop("PC_src")  
  .drop("PC_dest")  
  
nbConnexion.show(5)
   ```  

|PC_src|PC_dest|count|
|------|-------|-----|
|  C893|  C5736|  240|
| C2588|  C1156|15172|
|  C585|  C1082|  541|
|C26205|  C4174| 2249|
| C4087|  C3319| 6059|

___

#### Question 2

```scala
//Calcule le nombre de connexion entre un PC source et un PC destination pour chaque protocole  
val nbConProt = df.groupBy("PC_src", "PC_dest", "protocole").count().as("NBConProt")  
nbConProt.withColumn("PC_src, PC_dest", concat(col("PC_src"), lit(", " ), col("PC_dest")))  
  .drop("PC_src")  
  .drop("PC_dest")  
  
nbConProt.show(5)
   ```  

|PC_src|PC_dest|protocole|count|
|------|-------|---------|-----|
| C1357|   C528|       17|  160|
| C2619|  C1707|        6|  862|
| C3756|   C625|        6|  329|
|  C625|  C2557|        6|  514|
| C5139|   C706|        6|  870|


```scala
//Calcule le nombre de connexion entre un PC source et un PC destination pour chaque port source
val nbConProt = df.groupBy("PC_src", "PC_dest", "protocole").count().as("NBConProt")  
nbConProt.withColumn("PC_src, PC_dest", concat(col("PC_src"), lit(", " ), col("PC_dest")))  
  .drop("PC_src")  
  .drop("PC_dest")  
  
nbConProt.show(5)
   ```  

|PC_src|PC_dest|PORT_src|count|
|------|-------|--------|-----|
| C5030|  C4316|     443|  339|
|  C443|   C754|   N2314|    1|
|C23679| C13762|     N44|   19|
| C1697|  C1823|     139| 1259|
| C1685|  C3935|      N1|   19|


```scala
//Calcule le nombre de connexion entre un PC source et un PC destination pour chaque port destination
val nbConPortDest = df.groupBy("PC_src", "PC_dest", "PORT_dest").count().as("NBConPortDEST")  
nbConPortDest.withColumn("PC_src, PC_dest", concat(col("PC_src"), lit(", "), col("PC_dest")))  
  .drop("PC_src")  
  .drop("PC_dest")  
  
nbConPortDest.show(5)
   ```  

|PC_src|PC_dest|PORT_dest|count|
|------|-------|---------|-----|
| C1707|   C925|   N10487|    1|
|  C528|  C2016|   N10661|    1|
|  C457|  C3741|   N10977|    1|
|  C719|   C612|      389|   83|
|  C529|   C453|   N10732|    1|

___

#### Question 3

```scala
//Definie une fenetre temporelle de 5min
val w = window($"timestamp", "5 Minute")

val nbConnexion2 = df.groupBy(w,col("PC_src"), col("PC_dest")).count().as("NBConnexions").orderBy("window")
nbConnexion2.withColumn("PC_src, PC_dest", concat(col("PC_src"), lit(", " ), col("PC_dest")))
  .drop("PC_src")
  .drop("PC_dest")
  .show(5)
   ```

|              window|count|PC_src, PC_dest|
|--------------------|-----|---------------|
|[2010-10-10 18:30...|    1|     C706, C357|
|[2010-10-10 18:30...|    2|    C5883, C706|
|[2010-10-10 18:30...|    1|   C3350, C1685|
|[2010-10-10 18:30...|    2|    C467, C3299|
|[2010-10-10 18:30...|    7|    C586, C2584|  


```scala
val nbConProt2 = df.groupBy(w,col("PC_src"), col("PC_dest"), col("protocole")).count().as("NBConProt").orderBy("window")  
nbConProt2.withColumn("PC_src, PC_dest", concat(col("PC_src"), lit(", " ), col("PC_dest")))  
  .drop("PC_src")  
  .drop("PC_dest")  
  
nbConProt2.show(5)
   ```
   
|              window|PC_src|PC_dest|protocole|count|
|--------------------|------|-------|---------|-----|
|[2010-10-10 18:30...| C3405|   C467|        6|    6|
|[2010-10-10 18:30...|C22112| C22976|       17|    6|
|[2010-10-10 18:30...| C1707|  C3202|        6|    1|
|[2010-10-10 18:30...|  C706|  C5883|        6|    1|
|[2010-10-10 18:30...| C4123|  C5720|        6|    4|




```scala
var nbConPortSrc2 = df.groupBy(w,col("PC_src"), col("PC_dest"), col("PORT_src")).count().as("NBConPortSRC").orderBy("window")  
nbConPortSrc2.withColumn("PC_src, PC_dest", concat(col("PC_src"), lit(", " ), col("PC_dest")))  
  .drop("PC_src")  
  .drop("PC_dest")  
  
nbConPortSrc2.show(5)
   ```

|              window|PC_src|PC_dest|PORT_src|count|
|--------------------|------|-------|--------|-----|
|[2010-10-10 18:30...| C2270|  C5720|  N10536|    1|
|[2010-10-10 18:30...| C5720|  C2270|     443|    4|
|[2010-10-10 18:30...| C3611|   C612|  N10618|    1|
|[2010-10-10 18:30...|  C706|  C3605|      80|    2|
|[2010-10-10 18:30...|  C467|  C3299|     445|    1|
   


```scala
val nbConPortDest2 = df.groupBy(w,col("PC_src"), col("PC_dest"), col("PORT_dest")).count().as("NBConPortDEST").orderBy("window")  
nbConPortDest2.withColumn("PC_src, PC_dest", concat(col("PC_src"), lit(", "), col("PC_dest")))  
  .drop("PC_src")  
  .drop("PC_dest")  
  
nbConPortDest2.show(5)
   ```
   
|              window|PC_src|PC_dest|PORT_dest|count|
|--------------------|------|-------|---------|-----|
|[2010-10-10 18:30...|C11573|  C5736|   N10801|    1|
|[2010-10-10 18:30...| C4174|   C194|      137|    8|
|[2010-10-10 18:30...|  C195|  C5721|      445|    4|
|[2010-10-10 18:30...| C1707|  C3202|   N10613|    1|
|[2010-10-10 18:30...|  C586|  C3611|   N10780|    1|


___
___

### Partie 3


```scala
def selectByType(colType: DataType, df: DataFrame) = {  
  
  val cols = df.schema.toList.filter(x => x.dataType == colType).map(c => col(c.name))  
  df.select(cols: _*)  
}  
  
val res = selectByType(StringType, df)  
for (i <- res.columns)  
  for (j <- res.columns)  
    if (i!=j)  
      res.groupBy(i,j).count().show(5)
   ```


|PC_src|PORT_src|count|
|------|--------|-----|
| C3812|     445| 8019|
| C1344|   N5182|    2|
|  C159|  N10634|    1|
| C1845|  N10951|    1|
| C2986|  N10953|    4|



|PC_src|PC_dest|count|
|------|-------|-----|
|  C893|  C5736|  240|
| C2588|  C1156|15172|
|  C585|  C1082|  541|
|C26205|  C4174| 2249|
| C4087|  C3319| 6059|



|PC_src|PORT_dest|count|
|------|---------|-----|
|C25374|    N6818|    4|
|C24449|      137|15050|
| C5030|    N3898|   22|
| C1291|      445| 3893|
|  C232|      445| 4880|


|PC_src|protocole|count|
|------|---------|-----|
| C5139|        6| 2904|
|C19384|        6| 6157|
|  C242|        6|12246|
| C2719|        6| 1719|
| C4412|        6| 4575|



|PORT_src|PC_src|count|
|--------|------|-----|
|  N10852| C2270|   15|
|  N10708| C3611|    1|
|  N10077| C2584|    1|
|   N7227| C5638|    4|
|    N481| C4797|    4|



|PORT_src|PC_dest|count|
|--------|-------|-----|
|     N17|  C2270|   29|
|  N10810|   C128|    1|
|     445|  C2695| 5156|
|   N9508|   C706|   54|
|      N1|  C4135|   46|



|PORT_src|PORT_dest|count|
|--------|---------|-----|
|   N4806|       N1|    6|
|      N1|   N10607|    3|
|     445|     N574| 1665|
|     445|      N31| 1687|
|  N11096|      389|   79|


|PORT_src|protocole| count|
|--------|---------|------|
|     443|        6|588195|
|     N55|        6| 54199|
|  N10905|       17|    37|
|    N213|        6|  4985|
|  N10643|        6|   886|



|PC_dest|PC_src|count|
|-------|------|-----|
|   C893| C5736|  244|
|  C2588| C1156|15153|
|   C585| C1082|  541|
|  C2912| C1015| 8685|
| C26205| C4174| 2248|



|PC_dest|PORT_src|count|
|-------|--------|-----|
|  C4854|     N33|   30|
| C25374|   N6818|    4|
|  C5030|   N3898|   21|
|  C3935|  N10347|    4|
|   C788|     N33|  183|



|PC_dest|PORT_dest|count|
|-------|---------|-----|
|  C3812|      445| 8017|
|   C159|   N10634|    1|
|  C1845|   N10951|    1|
|  C5720|     N820|   19|
|  C5585|   N10914|    1|



|PC_dest|protocole|count|
|-------|---------|-----|
|  C5139|        6| 9751|
| C19384|        6| 6404|
|   C242|        6|12340|
|  C2719|        6| 1558|
| C13107|        6| 5221|



|PORT_dest|PC_src|count|
|---------|------|-----|
|      N17| C2270|   29|
|      445| C2695| 5158|
|    N9508|  C706|   44|
|      N46| C1670|  287|
|       N1| C4135|   46|



|PORT_dest|PORT_src|count|
|---------|--------|-----|
|   N10830|      88|   64|
|    N4806|      N1|    6|
|       N1|  N10607|    3|
|      445|    N574| 2921|
|      445|     N31| 2232|



|PORT_dest|PC_dest|count|
|---------|-------|-----|
|      N17|  C2270|   32|
|   N10852|  C2270|   15|
|   N10708|  C3611|    1|
|   N10077|  C2584|    2|
|    N7227|  C5638|    4|



|PORT_dest|protocole| count|
|---------|---------|------|
|      443|        6|572352|
|      N55|        6| 53056|
|   N10905|       17|    39|
|     N213|        6|  2788|
|   N10643|        6|   850|



|protocole|PC_src|count|
|---------|------|-----|
|        6|  C852| 5108|
|        6| C1240| 9294|
|        6| C2888| 1728|
|        6| C4287| 6474|
|        1| C1260|  155|



|protocole|PORT_src|count|
|---------|--------|-----|
|        6|   N4300| 2392|
|        6|    N332| 4905|
|        6|  N10981|  824|
|        6|   N1999| 3713|
|        6|   N7253| 1284|



|protocole|PC_dest|count|
|---------|-------|-----|
|        6|  C3319|14737|
|        6|  C1240| 9308|
|        6|  C2888| 1748|
|        6|  C4287| 6678|
|        1|  C1260|  161|



|protocole|PORT_dest|count|
|---------|---------|-----|
|        6|     N332| 3989|
|        6|    N4300| 1758|
|        6|   N11067|  854|
|        6|   N10981|  778|
|        6|    N1999| 2813|


___
___

### Partie 4


___
___

### Partie 5

#### Graphe 1
#### Question 1

```scala
	// Creer le dataframe des arcs pour la creation du graphe (avec les calculs demandé)  
	var edge = df.select("PC_src", "PC_dest", "NB_paquets")  
	edge = edge.groupBy("PC_src","PC_dest").agg(count("NB_paquets"),  
	  mean("NB_paquets"),  
	  stddev("NB_paquets"),  
	  sum("NB_paquets"),  
	  min("NB_paquets"),  
	  max("NB_paquets"))  
	edge.show(5)
```
La suite est le même tableaux mais trop grand.



|PC_src|PC_dest|count(NB_paquets)|   avg(NB_paquets)|
|------|-------|-----------------|------------------|
|  C893|  C5736|              240|1.2916666666666667|     
| C2588|  C1156|            15172| 3.358027946216715|      
|  C585|  C1082|              541|1.7541589648798521|     
|C26205|  C4174|             2249|1.5380168963983993|      
| C4087|  C3319|             6059|  2.99966991252682|


stddev_samp(NB_paquets)|sum(NB_paquets)|min(NB_paquets)|max(NB_paquets)|
-----------------------|---------------|---------------|---------------|
	 1.7327552410089755|            310|              1|             16|
	  2.639193296462126|          50948|              1|            101|
	 2.2518729415874557|            949|              1|             12|
      1.095323193532803|           3459|              1|             10|
	0.01816680999288364|          18175|              2|              3|

___

#### Question 2
```scala
// Renomme les colonnes en 'src' et 'dst' pour correpondre au demande de grapheframe  
edge = edge  
  .withColumnRenamed("PC_src","src")  
  .withColumnRenamed("PC_dest","dst")  
  
//Creer les dataframes des pc src et dest en enlevant les doublons et en 'id' correpondre au demande de grapheframe  
var vertex1 = df.select("PC_src").distinct().withColumnRenamed("PC_src","id")  
var vertex2 = df.select("PC_dest").distinct().withColumnRenamed("PC_dest","id")  
  
  
//Reunie vertex1 et vertex2 pour avoir l'integralité des sommets (en supprimant les doublons)  
val vertex = vertex1.union(vertex2).distinct()  
  
  
//Créer le graphe avec vertex pour les sommets et edge pour les arcs  
val graph = GraphFrame(vertex,edge);
```

___

#### Question 3

```scala
//Affiche les nombres d'arc entrant par sommets  
graph.inDegrees.show(5)  
  
//Affiche les nombres d'arc sortant par sommets  
graph.outDegrees.show(5)
```

|    id|inDegree|
|------|--------|
| C1602|      12|
|C15210|       6|
|C16820|      19|
|C11557|      20|
|C23728|      20|

|    id|outDegree|
|------|---------|
|C14290|        4|
|C15476|       18|
|C17344|        6|
|C18568|        3|
|C19676|        6|

___

#### Question 4

```scala
val sumTempSRC = graph.edges.groupBy("src").agg(sum(  
    col("count(NB_paquets)")+  
    col("avg(NB_paquets)")+  
    col("stddev_samp(NB_paquets)")+  
    col("sum(NB_paquets)")+  
    col("min(NB_paquets)")+  
    col("max(NB_paquets)")).as("Somme totale src"))  
  
val sumTempDEST = graph.edges.groupBy("dst").agg(sum(  
    col("count(NB_paquets)")+  
    col("avg(NB_paquets)")+  
    col("stddev_samp(NB_paquets)")+  
    col("sum(NB_paquets)")+  
    col("min(NB_paquets)")+  
    col("max(NB_paquets)")).as("Somme totale dst"))  
  
val sumS = sumTempSRC.join(sumTempDEST, sumTempSRC("src") === sumTempDEST("dst"), "outer")  
  .groupBy("src", "dst")  
  .agg(sum(col("Somme totale src") + col("Somme totale dst")).as("Somme Totale"))  
  
sumS.select("src","dst", "Somme Totale").filter($"src".isNotNull && $"dst".isNotNull)  
  .drop("dst").withColumnRenamed("src", "PC")  
  .show(5)
```

Les NaN du résultat suivant sont du au nombres trop grand pour la limite de spark

|    PC|      Somme Totale|
|------|------------------|
|C10081|102992.36091390779|
|C10508|               NaN|
|C10521|3542.1117284831216|
| C1100|               NaN|
|C11391|135553.81746212667|

___

#### Graphe 2
#### Question 1

```scala
//Creer les dataframes des pc src et dest en enlevant les doublons et en 'id' correpondre au demande de grapheframe  
var vertex1 = df.select("PC_src").distinct().withColumnRenamed("PC_src","id")  
var vertex2 = df.select("PC_dest").distinct().withColumnRenamed("PC_dest","id")  
  
  
//Reunie vertex1 et vertex2 pour avoir l'integralité des sommets (en supprimant les doublons)  
val vertex = vertex1.union(vertex2).distinct()

//Recupere df dans edge avant renommage  
var edge = df.select(col("*"))  
  
// Renomme les colonnes en 'src' et 'dst' pour correpondre au demande de grapheframe  
edge = edge  
  .withColumnRenamed("PC_src","src")  
  .withColumnRenamed("PC_dest","dst")  
  
//Créer le graphe avec vertex pour les sommets et edge pour les arcs  
val graph = GraphFrame(vertex,edge)

```

___

#### Question 2

```scala
//Affiche les nombres d'arc entrant par sommets  
graph.inDegrees.show(5)  
  
//Affiche les nombres d'arc sortant par sommets  
graph.outDegrees.show(5)
```


|    id|inDegree|
|------|--------|
| C2320|  144895|
| C1804|   11788|
| C1875|    2812|
| C1524|    5546|
|  C650|    5787|

|    id|outDegree|
|------|---------|
| C2320|   135370|
| C1804|    10970|
| C1875|     2396|
| C1524|     5546|
|  C650|     5769|



#### Question 3

```scala
val sumTempSRC = graph.edges.groupBy("src").agg(  
  mean("NB_paquets").as("mean_paquets"),  
  sum("NB_paquets").as("sum_paquets"),  
  min("NB_paquets").as("min_paquets"),  
  max("NB_paquets").as("max_paquets"))  
  .withColumn("PC", col("src"))  
  
  
val sumTempDST = graph.edges.groupBy("dst").agg(  
  mean("NB_paquets").as("mean_paquets"),  
  sum("NB_paquets").as("sum_paquets"),  
  min("NB_paquets").as("min_paquets"),  
  max("NB_paquets").as("max_paquets"))  
  .withColumn("PC", col("dst"))  
  
  
  
var sumTemp = sumTempSRC.union(sumTempDST).distinct()  
sumTemp = sumTemp.groupBy("PC").agg(min("min_paquets"),  
  mean("mean_paquets"),  
  max("max_paquets"),  
  sum("sum_paquets"))  
  
sumTemp.show(5)
```

|    PC|MIN|              MEAN|   MAX|     SUM|
|------|---|------------------|------|--------|
|C10508|  1|               8.5|    14|      40|
|C18550|  1|1.4365175332527207|    66|    2376|
|  C650|  1|1.5316523910308408|  1170|   17700|
| C6521|  1| 3.728959631803223|  1569|   67370|
| C2983|  1| 3957.148046702566|987270|85636819|

___

#### Question 4


___

#### Question 5

```scala
    var trigraph = graph.triangleCount.run
    trigraph = trigraph.select("id", "count")
    trigraph.show(5)
```
|    id|count|
|------|-----|
|C10061|    0|
|C10081|    0|
|C10508|    0|
|C10521|    0|
|C15476|    9|
