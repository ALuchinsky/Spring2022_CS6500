    # 2022_SP_6500_FP_Gu_Luchinsky_Mitchell

Final project of CS6500 course

# Installation

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

Now you are in a datanode container.

# Python

It seems to be convenient to run pyspark from IPython shell. To do this you should install the correct version of the `pyspark` package. This package can be found locally, so you should run the following commands

    cd /opt/spark/python/
    pip3 install -e .
    cd /2022_SP_6500_FP_Gu_Luchinsky_Mitchell/

Now you can run pyspark from ipyton environment. The following initialization lines should be run in the beginning:

    import findspark
    import pyspark
    from pyspark.sql import SparkSession
    findspark.init()
    spark = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()

## Reading a simple json table

To run a `scripts/read_table.py` script use the following commands

    $ ipython 3
    In [1]: %run ./scripts/read_table.py

It will read file `data/tab.json`, filter rows by condition `x>0`, splits all the strings in `str` column into words and counts the words' occurrences.
The output will be (removing some debug info)

    +------+-----+
    |  word|count|
    +------+-----+
    |   one|    1|
    | other|    1|
    |    is|    1|
    |string|    2|
    |   the|    1|
    |  This|    2|
    |     a|    1|
    +------+-----+

Note that all created in the script variables are available in the shell, so, for example, you can look at the original dataframe:

    In [11]: df.show()
    +------+--------------------+---+
    |     F|                 str|  x|
    +------+--------------------+---+
    |[2, 0]|    This is a string|  1|
    |[4, 1]|This the other st...|  2|
    |[9, 2]|                 one|  3|
    |[2, 3]|                 two|  0|
    +------+--------------------+---+

## Reading a Papers' list

Next example will be keywords extraction from data downloaded from HEP Inspires database. The file `data/AL_papers.json` was downloaded from this site (link https://inspirehep.net/api/literature?sort=mostrecent&size=500&page=1&q=find%20a%3ALuchinsky%2C%20A.V.) and corresponds to the request

    find a:Luchinsky, A.V.

This file can be analyzed by the script `scripts/read_papers.py`:

    ipython3
    %run ./scripts/read_papers.py

In DataFrame `paps_short` some important information about the papers is stored:

    In [2]: paps_short.show()
    +--------------------+--------------------+--------------------+---------------+--------------------+--------+--------------------+
    |               title|            abstract|             created|number_of_pages|            keywords|num_refs|             authors|
    +--------------------+--------------------+--------------------+---------------+--------------------+--------+--------------------+
    |Charmonia product...|In this paper, pr...|2021-03-26T00:00:...|             15|[[PACS,, 14.40.Pq...|      35|[Luchinsky, A.V.,...|
    |Exclusive decays ...|Exclusive decays ...|2020-07-09T00:00:...|             10|[[, publisher, St...|      42|[Luchinsky, A.V.,...|
    |Doubly heavy bary...|The theoretical a...|2019-12-11T00:00:...|              8|[[INSPIRE,, talk]...|      40|[Berezhnoy, A.V.,...|
    |Weak decays of do...|We consider exclu...|2019-05-29T00:00:...|             10|[[, publisher, El...|      21|[Gerasimov, A.S.,...|
    |$B_c$ excitations...|Status of the Bc ...|2019-11-21T00:00:...|              8|[[INSPIRE,, talk]...|      49|[Berezhnoy, A.V.,...|
    |Excited $\rho$ me...|In this paper, ex...|2018-12-27T00:00:...|              7|[[, publisher, Ph...|      22|   [Luchinsky, A.V.]|
    |Doubly heavy bary...|The theoretical a...|2018-09-27T00:00:...|             14|[[, publisher, El...|      56|[Berezhnoy, A.V.,...|
    |Charmonia Product...|In the presented ...|2018-01-30T00:00:...|              9|[[PACS,, 14.40.Pq...|      30|   [Luchinsky, A.V.]|
    |Lifetimes of Doub...|The inclusive dec...|2019-02-25T00:00:...|             11|[[INSPIRE,, baryo...|      49|[Likhoded, A.K., ...|
    |Double Charmonia ...|This paper is dev...|2017-12-11T00:00:...|             14|[[PACS,, 13.38.Dg...|      36|[Likhoded, A.K., ...|
    |Muon Pair Product...|Muon pair product...|2017-09-11T00:00:...|              7|[[PACS,, 13.20.Gd...|      22|   [Luchinsky, A.V.]|
    |Production of hea...|Processes of sing...|2017-08-24T00:00:...|             13|[[INSPIRE,, quark...|      57|[Likhoded, A.K., ...|
    |Leading order NRQ...|The presented pap...|2017-06-14T00:00:...|              8|[[INSPIRE,, charm...|      29|   [Luchinsky, A.V.]|
    |Comments on 'Stud...|Recent LHCb measu...|2017-03-28T00:00:...|              8|[[PACS,, 13.87.Ce...|      32|[Belyaev, I., Ber...|
    |Double Charmonia ...|In this note next...|2016-12-12T00:00:...|              7|[[INSPIRE,, p p: ...|      20|   [Luchinsky, A.V.]|
    |Production of $J/...|In the present wo...|2016-06-23T00:00:...|              8|[[INSPIRE,, quant...|      31|[Likhoded, A.K., ...|
    |Production of hea...|The phenomenology...|2016-01-05T00:00:...|             10|[[INSPIRE,, quant...|      34|[Likhoded, A.K., ...|
    |Production of J/ ...|The inclusive pro...|2015-11-16T00:00:...|              8|[[INSPIRE,, parto...|      29|[Likhoded, A.K., ...|
    |$J/\Psi \Upsilon$...|Inclusive product...|2015-09-11T00:00:...|              8|[[INSPIRE,, J/psi...|      26|   [Luchinsky, A.V.]|
    |Hadroproduction o...|The production of...|2015-05-28T00:00:...|              9|[[INSPIRE,, CERN ...|      38|[Berezhnoy, A.V.,...|
    +--------------------+--------------------+--------------------+---------------+--------------------+--------+--------------------+
    only showing top 20 rows

# Scala

See file `README_scala.md`
