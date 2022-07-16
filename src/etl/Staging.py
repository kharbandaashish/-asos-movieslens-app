from pyspark.sql.functions import *


def check_if_table_exists(logger, spark, db_name, table_nm):
    logger.debug("Inside check_if_table_exists function in Staging.py")
    logger.info("Checking if ratings table already exists")
    table_flag = False
    spark.sql("use {};".format(db_name))
    existing_tables_df = spark.sql("show tables;")
    existing_tables = existing_tables_df.select("tableName").collect()
    for table_name in existing_tables:
        if table_name.tableName == table_nm:
            table_flag = True
    return table_flag


def stage_ratings_files(logger, spark, db_name, df, ratings_table_name, ratings_update_table_name):
    logger.debug("Inside stage_ratings_files function in Staging.py")
    logger.info("Staging ratings files")

    # Adding yearmo column for paritioning data while writing
    ratings_df = df.withColumn("yearmo", from_unixtime("timestamp", 'yyyyMM'))

    # Casting columns
    ratings_df = ratings_df.select(col("userId").cast("bigint"), col("movieId").cast("bigint"),
                                   col("rating").cast("float"), col("timestamp").cast("bigint"),
                                   col("yearmo").cast("int"))

    ratings_table_flag = check_if_table_exists(logger, spark, db_name, "ratings")
    logger.debug("Ratings_table_flag - {}".format(ratings_table_flag))
    if not ratings_table_flag:
        logger.info("Ratings table does not exist. Writing data to table")
        ratings_df.write.partitionBy("yearmo").format("delta").saveAsTable(
            "{}.{}".format(db_name, ratings_table_name))
        logger.info("Ratings table created")
    elif ratings_table_flag:
        logger.info("Ratings table exists. Creating  updates table and merging data to existing table")
        ratings_df.write.partitionBy("yearmo").mode("overwrite").format("delta").saveAsTable(
            "{}.{}".format(db_name, ratings_update_table_name))
        spark.sql("""
            MERGE INTO {db_name}.{table_name}
            USING {db_name}.{updates_table_name}
            ON 
            {db_name}.ratings.userId = {db_name}.{updates_table_name}.userId 
            AND
            {db_name}.ratings.movieId = {db_name}.{updates_table_name}.movieId
            WHEN MATCHED THEN
              UPDATE SET
                userId = {updates_table_name}.userId,
                movieId = {updates_table_name}.movieId,
                rating = {updates_table_name}.rating,
                timestamp = {updates_table_name}.timestamp
            WHEN NOT MATCHED
              THEN INSERT (
                userId,
                movieId,
                rating,
                timestamp
              )
              VALUES (
                {updates_table_name}.userId,
                {updates_table_name}.movieId,
                {updates_table_name}.rating,
                {updates_table_name}.timestamp
            )""".format(db_name=db_name, table_name=ratings_table_name,
                        updates_table_name=ratings_update_table_name))
        logger.info("Staged ratings files, table created/updated - {}.{}".format(db_name, ratings_table_name))


def stage_movies_files(logger, spark, db_name, df, table_name):
    logger.debug("Inside stage_movies_files function in Staging.py")
    logger.info("Staging movies files")

    # Casting columns
    movies_df = df.select(col("movieId").cast("bigint"), col("title"), col("genres"))

    # Dropping duplicates from movies files. Found during analysis Refer Analysis notebook
    movies_df = movies_df.dropDuplicates(["title"])

    movies_df.write.mode("overwrite").format("delta").saveAsTable("{}.{}".format(db_name, table_name))
    logger.info("Staged movies files, table created - {}.{}".format(db_name, table_name))


def stage_tags_files(logger, spark, db_name, df, table_name):
    logger.debug("Inside stage_tags_tile function in Staging.py")
    logger.info("Staging tags files")

    # Casting columns
    tags_df = df.select(col("userId").cast("bigint"), col("movieId").cast("bigint"), col("tag"),
                        col("timestamp").cast("bigint"))

    tags_df.write.mode("overwrite").format("delta").saveAsTable("{}.{}".format(db_name, table_name))
    logger.info("Staged tags files, table created - {}.{}".format(db_name, table_name))


def main(logger, spark, conf):
    logger.info("Staging Started")
    logger.debug("Inside main function in Staging.py")
    database_name = conf["database_name"]
    datasets_dir = conf["datasets_dir"]
    dbfs_dir = 'dbfs:/{}'.format(datasets_dir)
    ratings_table_name = conf["ratings_table_name"]
    ratings_update_table_name = conf["ratings_update_table_name"]
    movies_table_name = conf["movies_table_name"]
    tags_table_name = conf["tags_table_name"]

    logger.info("Creating staging database - {}".format(database_name))
    spark.sql("CREATE DATABASE IF NOT EXISTS {};".format(database_name))

    # Reading ratings files
    ratings_df = spark.read.csv("{}/ratings*.csv".format(dbfs_dir), header=True)
    # Staging ratings files
    stage_ratings_files(logger, spark, database_name, ratings_df, ratings_table_name, ratings_update_table_name)

    # Reading movies files
    movies_df = spark.read.csv("{}/movies*.csv".format(dbfs_dir), header=True)
    # Staging movies files
    stage_movies_files(logger, spark, database_name, movies_df, movies_table_name)

    # Reading tags files
    tags_df = spark.read.csv("{}/tags*.csv".format(dbfs_dir), header=True)
    # Staging tags files
    stage_tags_files(logger, spark, database_name, tags_df, tags_table_name)

    logger.info("Staging Completed")
