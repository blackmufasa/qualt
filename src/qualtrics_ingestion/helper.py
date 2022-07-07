"""
Helper functions for the ETL load.
"""
import pendulum as pdt
import qualtrics_ingestion.config as cfg
import pandas as pd
import boto3
import logging

def get_list_of_surveys(freq, adb_conn):
    """
    Get a list of surveys
    :param freq:
    :param adb_conn:
    :return:
    """
    asql = cfg.list_of_surveys.format(freq=freq)
    logging.info('the sql is {}'.format(asql))
    adb_conn.execute(asql)
    result: tuple = adb_conn.fetchall()
    alist = []
    #logging.info('the list is {}'.format(list(result)))
    for elem in list(result):
        alist.append(elem[0])
    return alist


def get_date_time_string():
    """
    Date formatting sub routine
    :return:
    """
    today = pdt.now()
    return today.to_datetime_string()


def reset_survey_stage_tables(tlist, tschema, tdb):
    """
    Deletes/Truncates the staging tables
    :param tlist:
    :param tschema:
    :param tdb:
    :return:
    """
    logging.info('Now deleteing all the stage tables')
    conn = tdb.create_connection()
    conn.autocommit = True
    with conn.cursor() as curs:
        for atable in tlist:
            asql = cfg.erase_table_sql.format(schema=tschema, tname=atable)
            curs.execute(asql)
    conn.close()


def upload_survey_data_into_stage(asql, tdb) -> bool:
    """
    Runs the cpoy command to upoad survey data into db table 
    :param asql:
    :param tdb:
    :return:
    """
    try:
        conn = tdb.create_connection()
        conn.autocommit = True
        with conn.cursor() as curs:
            curs.execute(asql)
        conn.close()
        return True
    except Exception as e:
        logging.exception(
            'Could not upload into table from s3 due to %s', e, exc_info=True)
        return False


def cleanup_date_cols(adf) -> pd.DataFrame:
    """
    Takes a dataframe and cleanup the nan, naT from the date columns
    :param adf:
    :return:
    """
    for cols in list(adf):
        adf[cols] = adf[cols].replace('None', '')
        if cols in cfg.numeric_cols_body:
            adf[cols] = adf[cols].fillna(0)
        else:
            adf[cols] = adf[cols].fillna('')
    for cols in cfg.body_date_cols:
        if cols in adf:
            adf[cols] = adf[cols].replace('NaT', 'None')
            adf[cols] = adf[cols].replace('0', '')
            adf[cols] = adf[cols].replace('None', '')
            adf[cols] = adf[cols].fillna('')

    return adf


def upload_a_file_into_s3(afile, upload_path, abucket):
    """
    upload a file into the proper s3 location
    :param afile:
    :param upload_path:
    :param abucket:
    :return:
    """
    try:
        session = boto3.Session(
            aws_access_key_id=cfg.access_key,
            aws_secret_access_key=cfg.access_secret,
        )
        s3 = session.resource('s3')
        # Filename - File to upload
        # Bucket - Bucket to upload to (the top level directory under AWS S3)
        # Key - S3 object name (can contain subdirectories). If not specified then file_name is used
        s3.meta.client.upload_file(
            Filename=afile, Bucket=abucket, Key=upload_path)
    except Exception as e:
        logging.info('Could not upload into s3 due to {}'.format(e))


def format_upload_sql(asql, abucket, filename, key, secret):
    """
    format a sql with the apprpriate values and returns the formatted sql
    :param asql:
    :param abucket:
    :param filename:
    :param key:
    :param secret:
    :return:
    """
    return asql.format(bucket=abucket, file_name=filename, access_key=key, access_secret=secret)
