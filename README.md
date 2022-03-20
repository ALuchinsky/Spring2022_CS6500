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

# Scala

See file `README_scala.md`
