Here is how you can run some scripts in the scala environment. Remember, that all scripts should be run from datanode container, main project directory. See main readme file for details

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

This scripts reads all the records, extracts to table `paps_short` only the information that could be useful (title, abstract, creation date, number of pages, assigned by journals keywords, number of references and list of authors). The schema of this short table is

    scala> paps_short.printSchema
    root
    |-- title: string (nullable = true)
    |-- abstract: string (nullable = true)
    |-- created: string (nullable = true)
    |-- number_of_pages: long (nullable = true)
    |-- keywords: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- schema: string (nullable = true)
    |    |    |-- source: string (nullable = true)
    |    |    |-- value: string (nullable = true)
    |-- num_refs: integer (nullable = false)
    |-- authors: array (nullable = true)
    |    |-- element: string (containsNull = true)

There are also some tables with keywords generated from titles and abstracts (actually, simply a list of distinct words from these sources with number of counts) and titles.

For example, this is how you can look on the list of keywords generated from titles

    scala> kws_title.show
    +-----------+-----+
    |          K|count|
    +-----------+-----+
    |       Pair|    1|
    |      sigma|    1|
    |  [Multiple|    1|
    |         e+|    1|
    |  chi(b0,2)|    1|
    |          K|    1|
    | tetraquark|    1|
    |      decay|    1|
    | Light-Cone|    1|
    |      meson|    1|
    |       [New|    1|
    | (SPASCHARM|    1|
    |     [$B_c$|    1|
    |      $W\to|    1|
    |          =|    1|
    |      gluon|    1|
    |\rightarrow|    1|
    |     chi/b]|    1|
    |   Composed|    1|
    |      $B_c$|    1|
    +-----------+-----+
    only showing top 20 rows

As you can see, almost all of them are related to physics. Note that some most-common english words (list downloaded from https://github.com/pkLazer/password_rank/blob/master/4000-most-common-english-words-csv.csv) were removed. In total there are 308 generated words:

    scala> kws_title.count
    res2: Long = 254

Using the same approach we can list and count number of words from abstracts

    scala> kws_abstracts.show
    +-------------+-----+
    |            K|count|
    +-------------+-----+
    |      scaling|    1|
    |    observed,|    1|
    |   $B_s^{(*)}|    1|
    |       bosons|    1|
    |distributions|    1|
    |        peaks|    1|
    |        decay|    1|
    |  $\chi_{c1}$|    1|
    |  scattering)|    1|
    |interactions.|    1|
    |           n?|    1|
    |         lies|    1|
    |   $\psi(2S)$|    1|
    |    kinematic|    1|
    |        (DPS)|    1|
    |    Bc->Bs*+n|    1|
    |    \Upsilon,|    1|
    |           3P|    1|
    |      given.]|    1|
    |    involving|    1|
    +-------------+-----+
    only showing top 20 rows

    scala> kws_abstracts.count
    res0: Long = 1060

The total number of words is larger and they seem to be less "physical", although most common english words are also removed.

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
