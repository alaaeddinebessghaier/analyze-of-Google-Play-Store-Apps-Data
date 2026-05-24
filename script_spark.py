from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col , regexp_replace, mean, when, to_date , sum ,isnan, avg,max, count
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
import plotly.graph_objects as go
import numpy as np





spark = SparkSession.builder \
    .appName("app_store") \
    .getOrCreate()
## columns type 
def load_data(app_path,review_path):
    schema = StructType([
        StructField("App",StringType(),True),
        StructField("Category",StringType(),True),
        StructField("Rating",FloatType(),True),
        StructField("Reviews",IntegerType(),True),
        StructField("Size",StringType(),True),
        StructField("Installs",StringType(),True),
        StructField("Type",StringType(),True),
        StructField("Price",StringType(),True),
        StructField("Content Rating",StringType(),True),
        StructField("Genres",StringType(),True),
        StructField("Last Updated",StringType(),True),
        StructField("Current Ver",StringType(),True),
        StructField("Android Ver",StringType(),True)
    ])
    schema_review = StructType([
    StructField("App",StringType(),True),
    StructField("Translated_Review",StringType(),True),
    StructField("Sentiment",StringType(),True),
    StructField("Sentiment_Polarity",StringType(),True),
    StructField("Sentiment_Subjectivity",StringType(),True)
    ])
    df = spark.read.csv(app_pathpath, header=True, schema=schema)
    df_review = spark.read.csv(review_path,header=True, schema=schema_review)

    return df,df_review

def clean_data(df,df_review):
    ### EDA
    df = df.fillna(0)
    ###### price column
    df = df.withColumn("Price",regexp_replace("Price", "\\$",""))
    df = df.withColumn("price",df["Price"].cast("float"))
    #df.groupBy("Price").count().orderBy("count", ascending=False).show(50, False)

    ###### Installs column
    df = df.withColumn("Installs",regexp_replace("Installs", "\\+",""))
    df = df.withColumn("Installs",regexp_replace("Installs",",",""))
    df = df.withColumn("Installs",df["Installs"].cast("long"))
    #df.groupBy("Installs").count().orderBy("count", ascending=False).show(50, False)

    ###### size column 
    df = df.withColumn("Size",regexp_replace("Size","M",""))
    df = df.withColumn("Size",df["Size"].cast("float"))
    avg_size= df.select(mean("Size")).collect()[0][0]
    df = df.fillna({"Size":avg_size})
    #df.groupBy("Size").count().orderBy("count",ascending=False).show(50,False)

    ##### type column
    df = df.withColumn("Type",when(col("Type").isin("Free","Paid"),col("Type")).otherwise(None))
    df= df.dropna(subset=["Type"])
    #df.groupBy("Type").count().show()

    ####Last updated
    df = df.withColumn("Last Updated",to_date(col("Last Updated"),"MMMM d, yyyy"))

    """
    df.select([sum(when(col(c).isNull(),1).otherwise(0)).alias(c)   for c in df.columns]).show()
    df.show(5)
    df.describe().show()
    df.printSchema()
    """


#####################


    df_review = df_review.withColumn("Sentiment_polarity",col("Sentiment_polarity").cast("float"))
    df_review = df_review.filter(~isnan("Sentiment_polarity"))
    avg_sentiment=df_review.select(mean("Sentiment_polarity")).collect()[0][0]
    df_review = df_review.fillna({"Sentiment_polarity":avg_sentiment})
    #df_review.select([sum(when(col(c).isNull(),1).otherwise(0)).alias(c) for c in df_review.columns]).show()
    """
    df_review.show(5)
    df_review.printSchema()
    df_review.describe().show()
    """
    joined_df = df.join(df_review,on="App",how="inner")

    return joined_df
def analyze(joined_df):
    #########Which category has the highest share of (active) apps in the market?
    total_apps = joined_df.select("App").count()
    Category_share = joined_df.groupBy("Category").count()
    Category_share = Category_share.withColumn("share",(col("count") / total_apps) * 100)
    #Category_share.orderBy("share",ascending=False).show()


    ################ avg rate of apps 
    avg_rate_each_category = joined_df.groupBy("Category").agg(avg("Rating").alias("avg_category"))
    #avg_rate_each_category.orderBy('avg_category',ascending= False).show()



    ####### Sizing Strategy - Light Vs Bulky?
    ####### How do app sizes impact the app rating?

    df_plot = joined_df.select("Size","Rating").dropna()
    df_size = df_plot.withColumn("sizeGroup",when(col("Size")<10,"small")
                                        .when((col("Size")>10) &(col("Size")<50),"Medium" ).otherwise("large"))
    result= df_size.groupBy("sizeGroup").agg(avg("Rating").alias("avg_rating"))
    #result.orderBy("avg_rating",ascending=False).show()
    """
    pdf = df_plot.toPandas()
    plt.scatter(pdf['Size'],pdf['Rating'])
    plt.xlabel("App Size (MB)")
    plt.ylabel("Rating")
    plt.title("App Size vs Rating")
    plt.savefig("size_vs_rating.png")
    plt.close()
    """


    ##############Pricing Strategy - Free Vs Paid?¶
    ##############How do app prices impact app rating?


    Strategy = joined_df.select("Type","Rating").dropna()
    rst = Strategy.groupBy("Type").agg(avg("Rating").alias("avg_rating"))
    #rst.orderBy("avg_rating",ascending=False).show()



    ###########Current pricing trend - How to price your app?

    pt = joined_df.select("Category", "price").dropna().toPandas()
    #tt = pt.groupby("Category").agg(avg("price").alias("avg_price"))
    #tt.orderBy("avg_price",ascending=False).show()
    #joined_df.agg(max("price")).show()


    """
    plt.figure(figsize=(12,6))
    sns.stripplot(x="Category", y="price", data=pt, jitter=True)
    plt.xticks(rotation=90)
    plt.title("Price distribution across categories")
    plt.savefig("price_strip_plot.png")
    plt.close()
    """





    ##########distribution between free and paid 

    joined_df.groupBy("Type").agg(count("Installs")).show()
    #joined_df.groupBy("Category","Type") \
    #        .agg(sum("Installs").alias("total_installs"))\
    #        .show()




    ######## correlations between features 

    num_cols = [
        field.name for field in joined_df.schema.fields
        if isinstance(field.dataType, (IntegerType,FloatType))
    ]
    df_num = joined_df.select(num_cols).dropna()
    corr_df = df_num.toPandas()
    corr = corr_df.corr()
    """
    plt.figure(figsize=(10,8))
    sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Correlation Matrix of Numeric Features")

    plt.savefig("correlation_heatmap.png")
    plt.close()
    """

    df_counts = joined_df.groupBy("Category", "Sentiment").count()

    pivot_df = df_counts.groupBy("Category") \
        .pivot("Sentiment") \
        .sum("count") \
        .fillna(0)

    pdf = pivot_df.toPandas()
    sentiments = ["Positive", "Neutral", "Negative"]
    cols = [c for c in sentiments if c in pdf.columns]

    # 3. Normalize (100% stacked)
    pdf[cols] = pdf[cols].div(
        pdf[cols].sum(axis=1).replace(0, 1),
        axis=0
    )

    # 4. X-axis setup
    categories = pdf["Category"]
    x = np.arange(len(categories))

    # 5. Extract values safely
    positive = pdf["Positive"] if "Positive" in pdf.columns else np.zeros(len(pdf))
    neutral = pdf["Neutral"] if "Neutral" in pdf.columns else np.zeros(len(pdf))
    negative = pdf["Negative"] if "Negative" in pdf.columns else np.zeros(len(pdf))

    # 6. Plot figure
    plt.figure(figsize=(12,6))

    plt.bar(x, positive, color="#2ca02c", label="Positive")
    plt.bar(x, neutral, bottom=positive, color="#1f77b4", label="Neutral")
    plt.bar(x, negative, bottom=positive + neutral, color="#d62728", label="Negative")
    """
    # 7. Labels & formatting
    plt.title("Sentiment Distribution by Category (100% Stacked)")
    plt.xlabel("Category")
    plt.ylabel("Fraction of Reviews")

    plt.xticks(x, categories, rotation=45, ha="right")

    plt.legend()

    plt.tight_layout()

    # 8. Save image
    plt.savefig("sentiment_by_category.png", dpi=300)

    plt.close()
    """

    


def save_to_postgres(joined_df):
    joined_df.write.mode("overwrite").parquet("file:///home/hadoop/play_store/data/clean_playstore")






    ##### load to db

    joined_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/playstore_db") \
        .option("dbtable", "apps_reviews_clean") \
        .option("user", "postgres") \
        .option("password", "heisenberg9N12") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()








if __name__ == "__main__":

    df, df_review = load_data(
        "file:///home/hadoop/play_store/googleplaystore.csv",
        "file:///home/hadoop/play_store/googleplaystore_user_reviews.csv"
    )

    joined_df = clean_data(df, df_review)

    analyze(joined_df)

    save_to_postgres(joined_df)