# See https://www.bmc.com/blogs/python-spark-machine-learning-classification/
import pyspark.sql.functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.sql.types import StructType, StructField, NumericType
import pandas as pd
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

findspark.init()
spark = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()


# ------- Loading Data -------
cols = ('age',
        'sex',
        'chest pain',
        'resting blood pressure',
        'serum cholesterol',
        'fasting blood sugar',
        'resting electrocardiographic results',
        'maximum heart rate achieved',
        'exercise induced angina',
        'ST depression induced by exercise relative to rest',
        'the slope of the peak exercise ST segment',
        'number of major vessels ',
        'thal',
        'last')
data = pd.read_csv('./data/heart.csv', delimiter=' ', names=cols)
data = data.iloc[:, 0:13]


def isSick(x):
    if x in (3, 7):
        return 0
    else:
        return 1


data['isSick'] = data['thal'].apply(isSick)
df = spark.createDataFrame(data)

# ----- Selecting Features -------------
features = ('age',
            'sex',
            'chest pain',
            'resting blood pressure',
            'fasting blood sugar',
            'resting electrocardiographic results',
            'maximum heart rate achieved',
            'exercise induced angina',
            'ST depression induced by exercise relative to rest',
            'the slope of the peak exercise ST segment',
            'number of major vessels ')
assembler = VectorAssembler(inputCols=features, outputCol="features")
raw_data = assembler.transform(df)
raw_data.select("features").show(truncate=False)

# --------- Scaling Features ------------
standardscaler = StandardScaler().setInputCol(
    "features").setOutputCol("Scaled_features")
raw_data = standardscaler.fit(raw_data).transform(raw_data)
raw_data.select("features", "Scaled_features").show(5)

# ------- test/train sets----------------
training, test = raw_data.randomSplit([0.5, 0.5], seed=12345)

# ------- Fitting LR model ---------
lr = LogisticRegression(
    labelCol="isSick", featuresCol="Scaled_features", maxIter=10)
model = lr.fit(training)
predict_train = model.transform(training)
predict_test = model.transform(test)
predict_test.select("isSick", "prediction").show(10)

# ------- Coefficients --------------
print("Multinomial coefficients: " + str(model.coefficientMatrix))
print("Multinomial intercepts: " + str(model.interceptVector))

# ------- Comparison Table -----------
check = predict_test.withColumn('correct', F.when(
    F.col('isSick') == F.col('prediction'), 1).otherwise(0))
check_table = check.groupby("correct").count()
check_table.show()
metrics = check_table.select("count").collect()
print("Accuracy: ", int(100*metrics[0][0]/(metrics[0][0]+metrics[1][0])), "%")
