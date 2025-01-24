'''
Find the companies who have at least 2 users who speak both English and German in
pyspark

Data:
data = [(A, 1, English),
(A, 1, German),
(A, 2, English),
(A, 2, German),
(A, 3, German),
(B, 1, English),
(B, 2, German),
(C, 1, English),
(C, 2, German)]

schema =(company_id, user_id, language)

'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, size

spark = SparkSession.builder.appName("LanguagesCount").getOrCreate()

data = [("A", 1, "English"),
("A", 1, "German"),
("A", 2, "English"),
("A", 2, "German"),
("A", 3, "German"),
("B", 1, "English"),
("B", 2, "German"),
("C", 1, "English"),
("C", 2, "German")]

schema = ["company_id", "user_id", "language"] 

df = spark.createDataFrame(data, schema)
df.show()


language_set = df.groupBy("company_id","user_id").agg(collect_set("language").alias("languages_collection"))
#language_set.show()
  
  
bilingual = language_set.filter(size(col("languages_collection")) == 2)
#bilingual.show()

company_bilingual_count = bilingual.groupBy("company_id").count()
  
#company_bilingual_count.show()
  
bilingual_companies=company_bilingual_count.filter(col("count") >=2 )

bilingual_companies.select("company_id").show()  