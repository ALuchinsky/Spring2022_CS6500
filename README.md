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

This scripts reads all the records, extracts to table `paps_short` only the information that could be useful (title, abstract, creation date, number of pages, assigned by journals keywords, references and list of authors). Some tables with keywords generated from titles and abstracts (actually, simply a list of distinct words from these sources with number of counts) and titles are also created..

For example, this is how you can look on the list of keywords generated from titles

    scala> kws_title.show
    +---------------+-----+
    |              K|count|
    +---------------+-----+
    |        process|    1|
    |          $W\to|    1|
    |$\chi_c$-mesons|    1|
    |       Composed|    1|
    |         [$B_c$|    1|
    |         chi/b]|    1|
    |            $Z$|    1|
    |     Light-Cone|    1|
    |    \rightarrow|    1|
    |        PHOTON]|    1|
    |          sigma|    1|
    |              K|    1|
    |         jets"]|    1|
    |           Pair|    1|
    |  $\rightarrow$|    1|
    |     tetraquark|    1|
    |      [Multiple|    1|
    |          decay|    1|
    |          gluon|    1|
    |         mu-).]|    1|
    +---------------+-----+
    only showing top 20 rows

As you can see, almost all of them are related to physics. In total there are 366 such words:

    scala> kws_title.count
    res3: Long = 366

Using the same approach we can list and count number of words from abstracts

    scala> kws_abstracts.show
    +--------------------+-----+
    |                   K|count|
    +--------------------+-----+
    |                  bc|    1|
    |        $\eta_{c,b}$|    1|
    |                   ;|    1|
    |      pseudorapidity|    1|
    |       Specifically,|    1|
    |                  n?|    1|
    |$\Xi_{bc}\to\Xi_{...|    1|
    |           Bc->Bs*+n|    1|
    |                  By|    1|
    |              plane,|    1|
    |               (DPS)|    1|
    |     consideration.]|    1|
    |           \Upsilon,|    1|
    |       interactions.|    1|
    |         scattering)|    1|
    |         $\chi_{c1}$|    1|
    |                  3P|    1|
    |             explain|    1|
    |                lies|    1|
    |          $\psi(2S)$|    1|
    +--------------------+-----+
    only showing top 20 rows

    scala> kws_abstracts.count
    res5: Long = 1463

The total number of words is larger and they seem to be less "physical". It could be possible, however, to reduce this list by removing some popular english words.

It could be also useful to inspect the keywords assigned by the professionals (authors, INSPIRES team, etc.):

    scala> kws_assigned.show
    +--------------------+-----+
    |               value|count|
    +--------------------+-----+
    |numerical calcula...|   31|
    |       CERN LHC Coll|   21|
    |quantum chromodyn...|   17|
    |               LHC-B|   14|
    |            14.40.Pq|   13|
    |          charmonium|   12|
    |   quarkonium: heavy|   10|
    |electron positron...|   10|
    |            12.38.Bx|   10|
    |            13.25.Gv|   10|
    |            13.66.Bc|   10|
    |J/psi(3100): pair...|    8|
    |          light cone|    7|
    |     branching ratio|    7|
    |      color: singlet|    7|
    |        color: octet|    7|
    |            12.38.-t|    7|
    |channel cross sec...|    6|
    |quarkonium: pair ...|    6|
    |transverse moment...|    6|
    +--------------------+-----+
    only showing top 20 rows

    scala> kws_assigned.count
    res7: Long = 481

The full list of authors is available in this table:

    scala> all_authors.show
    +------------------+-----+
    |         full_name|count|
    +------------------+-----+
    |   Luchinsky, A.V.|   73|
    |    Likhoded, A.K.|   49|
    |   Poslavsky, S.V.|   14|
    |     Braguta, V.V.|   13|
    |   Berezhnoy, A.V.|   12|
    |   Novoselov, A.A.|    8|
    |   Gershtein, S.S.|    3|
    |    Filipowicz, M.|    2|
    |    Bulgakov, T.L.|    2|
    |       Wozniak, J.|    2|
    |   Grebenyuk, V.M.|    2|
    |  Parzhitski, S.S.|    2|
    |     Donskov, S.V.|    2|
    |     Sorokin, S.A.|    2|
    |  Chaikovsky, S.A.|    2|
    |Sinebryukhov, V.A.|    2|
    |  Samoylenko, V.D.|    2|
    |Sinebryukhov, A.A.|    2|
    |     Mesyats, G.A.|    2|
    |    Ratakhin, N.A.|    2|
    +------------------+-----+
    only showing top 20 rows

    scala> all_authors.count
    res9: Long = 84
