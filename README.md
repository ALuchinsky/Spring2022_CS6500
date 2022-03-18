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

After that you should run the docker and login into datanote:

    docker-compose up -d
    docker exec -it datanode bash

Now you are in a datanode container. The final step is to go to a project directory and run `spark-shell`

    cd /2022_SP_6500_FP_Gu_Luchinsky_Mitchell/
    /opt/spark/bin/spark-shell

From this shell yo can run different scripts

## Reading a Simple JSON table

The first script reads a file `data/tab.json`, extracts all rows with x>0, and collects all words from field str:

    :load scripts/read_tab.scala
    Loading scripts/read_tab.scala...
    [This,is,a,string,This,the,other,string,one]

As you can see, all words except "two" are preset in the list ("two" is excluded since this row was filtered by x>0 condition)
