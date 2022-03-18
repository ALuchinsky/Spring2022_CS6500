    # 2022_SP_6500_FP_Gu_Luchinsky_Mitchell

Final project of CS6500 course

## Installation

It seems that reading JSON files can be easily done with SPARK. A nice docker image can be found at https://gitlab.com/sniu312/cs6500hdfs_updated.git. After cloning the repository you need to modify the `docker-compose.yml` to mount the local directory to datanode file system: simply add the lines like

    <..>
    datanode:
        <...>
        volumes:
        - hadoop_datanode:/hadoop/dfs/data
        - ../../2022_SP_6500_FP_Gu_Luchinsky_Mitchell:/2022_SP_6500_FP_Gu_Luchinsky_Mitchell/

After that you should run the docker and login into datanote and go to the project directory:

    docker-compose up -d
    docker exec -it datanode bash
    cd /2022_SP_6500_FP_Gu_Luchinsky_Mitchell/

Now you are in a datanode container. The final step is to

## Reading a Simple JSON table

The first example is to a file `data/tab.json`, extract all rows with x>0, and collect all words from field str.

This can be done using Scala language from `spark-shell`

    /opt/spark/bin/spark-shell
    :load scripts/read_tab.scala

The output should be

    Loading scripts/read_tab.scala...
    [This,is,a,string,This,the,other,string,one]

As you can see, all words except "two" are preset in the list ("two" is excluded since this row was filtered by x>0 condition)

The same result can be done using python. Now the command is

    /opt/spark/bin/spark-submit ./scripts/read_table.py | tee out.txt

Lots of debug information will be printed, but the main output (saved in out.txt file) is almost the same:

    /opt/spark/python/lib/pyspark.zip/pyspark/context.py:225: DeprecationWarning: Support for Python 2 and Python 3 prior to version 3.6 is deprecated as of Spark 3.0. See also the plan for dropping Python 2 support at https://spark.apache.org/news/plan-for-dropping-python-2-support.html.
    DeprecationWarning)
    ['This', 'is', 'a', 'string', 'This', 'the', 'other', 'string', 'one']

## Extracting Keywords from the List of Papers

Next example will be keywords extraction from data downloaded from HEP Inspires database. The file `data/AL_papers.json` was downloaded from this site (link https://inspirehep.net/api/literature?sort=mostrecent&size=500&page=1&q=find%20a%3ALuchinsky%2C%20A.V.) and corresponds to the request

    find a:Luchinsky, A.V.

This file can be analyzed by the script `scripts/read_papers.scala`:

    /opt/spark/bin/spark-shell
    scala> :load scripts/read_papers.scala

This scripts reads all the records, extracts titles and abstract strings for each paper, splits them into words and creates flat arrays of the results. To see the actual output it is required to run the following commands in `spark-shell`:

    scala> kws_tits.show()
    +---------------+-----+
    |              K|count|
    +---------------+-----+
    |$\chi_c$-mesons|    1|
    |         chi/b]|    1|
    |     conversion|    1|
    |          $W\to|    1|
    |    \rightarrow|    1|
    |        PHOTON]|    1|
    |            $Z$|    1|
    |     Light-Cone|    1|
    |         [$B_c$|    1|
    |       Composed|    1|
    |          gluon|    1|
    |           Pair|    1|
    |              K|    1|
    |      [Multiple|    1|
    |  $\rightarrow$|    1|
    |         mu-).]|    1|
    |          sigma|    1|
    |         jets"]|    1|
    |     tetraquark|    1|
    |          decay|    1|
    +---------------+-----+
    only showing top 20 rows

as you can see, the most rare words in this table seem to be connected to physics, so they can be used as keywords. Here is how to calculate the total number of unique title keywords

    scala> kws_tits.count()
    res3: Long = 366

The same analysis can be done with generated from abstracts keywords:

    scala> kws_abs.show()
    +--------------------+-----+
    |                   K|count|
    +--------------------+-----+
    |               Total|    1|
    |            Assuming|    1|
    |        ?bc+??cc++R,|    1|
    |     consideration.]|    1|
    |                  pi|    1|
    |         scattering)|    1|
    |                lies|    1|
    |      pseudorapidity|    1|
    |                  n?|    1|
    |                 e^+|    1|
    |$\Xi_{bc}\to\Xi_{...|    1|
    |             explain|    1|
    |               (DPS)|    1|
    |           \Upsilon,|    1|
    |           Bc->Bs*+n|    1|
    |          $\psi(2S)$|    1|
    |         $\chi_{c1}$|    1|
    |       interactions.|    1|
    |                  3P|    1|
    |                  By|    1|
    +--------------------+-----+
    only showing top 20 rows
    scala> kws_abs.count()
    res0: Long = 1463

now the keywords are not so physical, but we can try to correct it by excluding some popular english words.

Table with list of authors for each paper is also created, as well as full list of authors:

        scala> authors.show()
    +--------------------+
    |           full_name|
    +--------------------+
    |[Luchinsky, A.V.,...|
    |[Luchinsky, A.V.,...|
    |[Berezhnoy, A.V.,...|
    |[Gerasimov, A.S.,...|
    |[Berezhnoy, A.V.,...|
    |   [Luchinsky, A.V.]|
    |[Berezhnoy, A.V.,...|
    |   [Luchinsky, A.V.]|
    |[Likhoded, A.K., ...|
    |[Likhoded, A.K., ...|
    |   [Luchinsky, A.V.]|
    |[Likhoded, A.K., ...|
    |   [Luchinsky, A.V.]|
    |[Belyaev, I., Ber...|
    |   [Luchinsky, A.V.]|
    |[Likhoded, A.K., ...|
    |[Likhoded, A.K., ...|
    |[Likhoded, A.K., ...|
    |   [Luchinsky, A.V.]|
    |[Berezhnoy, A.V.,...|
    +--------------------+
    only showing top 20 rows
    scala> all_authors.show()
    +--------------------+
    |                 col|
    +--------------------+
    |      Vasiliev, A.N.|
    |       Kharlov, V.Y.|
    |Bystritsky, Viach...|
    |       Mesyats, G.A.|
    |            Gula, A.|
    |      Stolupin, V.M.|
    |  Slabospitsky, S.R.|
    |       Ukhanov, M.N.|
    |     Gerasimov, A.S.|
    |       Neganov, A.B.|
    |Bystritskii, Vita...|
    |   Goncharenko, Y.M.|
    |      Kachanov, V.A.|
    |       Borisov, N.S.|
    |     Poslavsky, S.V.|
    |      Markhrin, V.I.|
    |     Kolomiets, V.G.|
    |       Semenov, P.A.|
    |   Kormilitsin, V.A.|
    |     Meschanin, A.P.|
    +--------------------+
    only showing top 20 rows

Total number of authors is

    scala> all_authors.count()
    res15: Long = 84
