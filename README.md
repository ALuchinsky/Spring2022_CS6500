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

    scala> kws_tits.collect()
    res0: Array[String] = Array(Charmonia, production, in, $W, \to, (c\bar, c), D_s^{(*)}$, decays, Charmonia, production, in, W, ?, (cc?)Ds(?), decays, Exclusive, decays, of, the, doubly, heavy, baryon, $\Xi_{bc}$, Exclusive, Decays, of, the, Doubly, Heavy, Baryon, $\Xi_{bc}$, Doubly, heavy, baryons, from, the, theoretical, point, of, view, Weak, decays, of, doubly, heavy, baryons:, Decays, to, a, system, of, $\pi$, mesons, $B_c$, excitations, at, LHC:, first, observations, and, further, research, prospects, Excited, $\rho$, mesons, in, $B_{c}\to\psi^{(')}KK_{S}$, decays, Excited, $\rho$, mesons, in, $B_{c}\to\psi^{(')}KK_{S}$, decays, Doubly, heavy, baryons, at, the, LHC, Doubly, heavy, baryons, at, LHC, Charmonia, Production, in, $W\to, (c\bar{c}), D_{s}^{(*)}$,...

    scala> res0.length
    res1: Int = 1155

    scala> kws_abs.collect()
    res2: Array[String] = Array([In, this, paper,, production, of, charmonium, state, ?, in, exclusive, W, ??Ds(?), decays, is, analyzed, in, the, framework, of, both, leading, order, Non-relativistic, Quantum, Chromodynamics, (NRQCD), and, light-cone, (LC), expansion, models., Analytical, and, numerical, predictions, for, the, branching, fractions, of, these, decays, in, both, the, approaches, are, given., The, typical, value, of, the, branching, fractions, is, ?10?11, and, it, turns, out, that, the, LC, results, are, significantly, larger, than, NRQCD, ones, (approximately, two, or, four, times, increase, depending, on, the, quantum, numbers, of, the, final, particles),, so, the, effect, of, internal, quark, motion, should, be, taken, into, account., Some, roug...

    scala> res2.length
    res4: Int = 6906
