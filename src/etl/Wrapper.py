import os
import Utils
import Staging
import Transformations

curr_dir = os.path.dirname(os.path.realpath(__file__))
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
log_dir = os.path.join(root_dir, "logs")
conf_dir = os.path.join(root_dir, "conf")
data_dir = os.path.join(root_dir, "data")

app_name = os.path.basename(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
log_file = os.path.join(log_dir, app_name)
conf_file = os.path.join(conf_dir, "config.ini")

if __name__ == '__main__':
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)
    logger = Utils.get_logger(True, log_file)
    logger.info("Application - '{}' started".format(app_name))
    logger.info("Reading configuration file")
    conf = Utils.read_config(logger, conf_file)
    dataset_url = conf['dataset_url']
    zip_name = conf['zip_name']
    datasets_dir = conf["datasets_dir"]
    output_file_dir = conf["output_file_dir"]
    download_data = conf["download_data"]
    staging_flag = conf["staging_flag"]
    transformations_flag = conf["transformations_flag"]
    logger.info("Creating spark session with app-name - '{}'".format(app_name))
    spark = Utils.get_spark_session(logger, app_name)
    logger.info("Setting up databricks utils(dbutils)")
    dbutils = Utils.get_dbutils(logger, spark)
    download_dir = os.path.join(data_dir, zip_name)
    if download_data == "1":
        logger.info("Downloading dataset from URL - {}".format(dataset_url))
        download_flag = Utils.download_dataset(logger, dataset_url, download_dir)
    logger.info("Unzipping dataset")
    unzip_flag = Utils.unzip_dataset(logger, download_dir, data_dir)
    logger.info("Uploading files to DBFS")
    unzipped_dir = os.path.join(data_dir, datasets_dir)
    dbfs_dir = 'dbfs:/{}'.format(datasets_dir)
    dbfs_upload_flag = Utils.upload_files_to_dbfs(logger, unzipped_dir, 'dbfs:/{}'.format(datasets_dir))
    if dbfs_upload_flag:
        logger.info("staging_flag - {}".format(staging_flag))
        if staging_flag == "1":
            Staging.main(logger, spark, conf)
        logger.info("transformations_flag - {}".format(transformations_flag))
        if transformations_flag == "1":
            Transformations.main(logger, spark, conf)
            target_dir = os.path.join(data_dir, output_file_dir)
            if not os.path.isdir(target_dir):
                os.makedirs(target_dir)
            logger.info("Downloading top 10 movies")
            Utils.download_files_from_dbfs(logger, 'dbfs:/{}'.format(output_file_dir), target_dir)
            logger.info("Cleaning output directory and renaming part file")
            Utils.rename_and_clean_output(logger, target_dir, output_file_dir)
    else:
        logger.error("Error in uploading files to dbfs. Terminating")
    logger.info("Cleaning dbfs")
    Utils.cleanup(logger)
    logger.info("Application - '{}' completed".format(app_name))