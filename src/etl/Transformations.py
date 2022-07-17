from pyspark.sql.functions import *
from pyspark.sql.window import Window


def split_movies_genres(logger, spark, db_name, movies_table_name, output_table_name):
    """
    Reads movies table and explodes the records if the genres columns has multiple genres separated by '|' and store it
    in the provided table name in overwrite mode.
    """
    logger.debug("Inside split_movies_genres function in Transformations.py")
    logger.info("Splitting the movie genres to get a single genre per row")
    # reading movies table in a dataframe
    movies_df = spark.table("{}.{}".format(db_name, movies_table_name))
    # exploding df on genres column to split multiples genres to multiple records
    exploded_movies_df = movies_df.withColumn("genre", explode(split(col("genres"), '\\|'))).drop("genres").sort(
        "movieId")
    # storing the output of exploded movies df as a delta table
    exploded_movies_df.write.mode("overwrite").format("delta").saveAsTable("{}.{}".format(db_name, output_table_name))
    logger.info("Table created : {}.{}".format(db_name, output_table_name))
    return exploded_movies_df


def top_10_movies(logger, spark, db_name, movies_table_name, ratings_table_name, output_file):
    """
    Reads movies & ratings table and create a CSV file with top 10 movies based on the average ratings with atleast
    5 ratings.
    """
    logger.debug("Inside top_10_movies function in Transformations.py")
    logger.info("Identify top 10 films by average rating")
    # reading movies table in a dataframe
    movies_df = spark.table("{}.{}".format(db_name, movies_table_name))
    # reading ratings table in a dataframe
    ratings_df = spark.table("{}.{}".format(db_name, ratings_table_name))
    # joining movies df with ratings df
    movie_ratings_df = movies_df.join(ratings_df, ["movieId"], "left")
    # grouping on movieId, title to get average rating and total number of ratings
    movie_ratings_df1 = movie_ratings_df.groupBy("movieId", "title").agg(count(col("userID")).alias("totalRatings"),
                                                                         avg("rating").alias("avgRating"))
    # filtering for movies with atleast 5 reviews
    movie_ratings_df2 = movie_ratings_df1.where(col("totalRatings") >= 5)
    # creating window to rank movies based on ratings
    w = Window.orderBy(col("avgRating").desc())
    # assigning a rank to filtered movies
    movie_ratings_df3 = movie_ratings_df2.withColumn("rank", dense_rank().over(w))
    # filtering for top 10 movies
    movie_ratings_df4 = movie_ratings_df3.where(col("rank") <= 10)
    # writing data to a single csv file
    movie_ratings_df4.coalesce(1).write.mode("overwrite").csv("{}".format(output_file), header=True)
    logger.info("CSV file created in directory: {}".format(output_file))
    return movie_ratings_df4


def main(logger, spark, conf):
    """
    Calls various transformration functions and display output if the show_output_flag is 1
    """
    logger.info("Transformations Started")
    logger.debug("Inside main function in Transformations.py")
    database_name = conf["database_name"]
    movies_table_name = conf["movies_table_name"]
    ratings_table_name = conf["ratings_table_name"]
    output_file_dir = conf["output_file_dir"]
    exploded_movies_table_name = conf["exploded_movies_table_name"]
    show_output_flag = conf["show_output_flag"]

    df1 = split_movies_genres(logger, spark, database_name, movies_table_name, exploded_movies_table_name)

    df2 = top_10_movies(logger, spark, database_name, movies_table_name, ratings_table_name, output_file_dir)
    logger.info("show_output_flag - {}".format(show_output_flag))
    if show_output_flag == "1":
        logger.info("Output for genres split - ")
        df1.show(20, False)
        logger.info("Output for top 10 movies based on ratings - ")
        df2.show(20, False)
    logger.info("Transformations Completed")
