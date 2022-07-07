from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry import trace
__author__ = "Ani"


import requests
from requests.structures import CaseInsensitiveDict
import pandas as pd
import qualtrics_cfg as cfg
import qualtrics_helper as hlp
from dataclasses import dataclass
from http import HTTPStatus
import logging
import argparse
from sqlalchemy import create_engine
import json
import time
import os
import zipfile
import re
import sys
import json
#from retrying import retry

from typing import Optional

parser = argparse.ArgumentParser('Enter the frequency of the Surveys')
parser.add_argument('-freq', choices=['d', 'w'])

LoggingInstrumentor().instrument(set_logging_format=True)

#[amaheswa] logging
#logging.basicConfig(level=logging.DEBUG)
#print(logging.root.handlers[:])
#for handler in logging.root.handlers[:]:
#    logging.root.removeHandler(handler)

#Comment out if log file not required
#timestr = time.strftime("%Y%m%d_%H%M%S")
#logging.basicConfig(handlers=[logging.FileHandler(f"app_{timestr}.log", mode='w'),logging.StreamHandler()],format = '%(asctime)s %(levelname)s [%(name)s] [%(filename)s:%(lineno)d] [trace_id=%(otelTraceID)s span_id=%(otelSpanID)s resource.service.name=%(otelServiceName)s] - %(message)s', force=True)

@dataclass
class Qualtrics_Obj:
    """
    The main class which is used to Store Qualtrics details for each Survey, the link, the actual Survey ID, API Token
    and the list of questions
    """
    data_center: str
    api_link: str
    survey_id: str
    api_token: str
    params: Optional[tuple] = None
    qlist: Optional[list] = None

    def format_header(self) -> CaseInsensitiveDict:
        headers: CaseInsensitiveDict = CaseInsensitiveDict()
        headers["Content-Type"] = "application/json"
        headers["X-API-TOKEN"] = self.api_token
        return headers
        
    #@retry(stop_max_attempt_number=3, wait_random_min=1000, wait_random_max=2000)
    def get_api_link(self):
        """
        Based on the survey it gets the survey data using an API call
        :return: a json body
        """
        headers = self.format_header()
        url = self.api_link.format(
            data_center=self.data_center, survey_id=self.survey_id)
        #logging.info(f'the 1 url is {url}, {headers}, {self.params}')
        if self.params is not None:
            resp = requests.get(url, headers=headers, params=self.params)
        else:
            resp = requests.get(url, headers=headers)
        if resp.status_code == HTTPStatus.OK:
            #logging.info('200 status code. Now parsing the data')
            return resp.json()
        else:
            logging.info(
                'The URL {resp.status_code} resulted in a non-zero return code')
            return {}

    def post_api_link(self, data):
        """
        Uses a payload to make a POST API call to initiate a file download process
        :param data: format, surveyId and Fields
        :return: The response ID of a file that would be downloaded
        """
        headers = self.format_header()
        url = self.api_link

        resp = requests.post(url, headers=headers, data=data)
        if resp.status_code == HTTPStatus.OK:
            #[amaheswa] - Testing
            #logging.info(f" POST LINK RESPONSE ID: {resp.json().get('result', {}).get('id')}")
            #logging.info(f'200 status code. Now parsing the data {resp.json()}')
            return resp.json().get('result', {}).get('id')
        else:
            logging.info(f'The URL {resp.status_code} resulted in a non-zero return code')
            return ''

    def get_final_file(self) -> str:
        """
        Uses the response ID from the last step to download the final file
        :return: absolute path of the file that was downloaded
        """
        headers = self.format_header()
        url = self.api_link
        
        # weaton: removed sleep here
        # change to recursively probe status of endpoint
        
        def query_status(url: str, headers: CaseInsensitiveDict) -> bool:
            progress = requests.get(url, headers=headers).json()
            logging.info("Percent complete: %s", progress.get("result").get("percentComplete"))
            if progress.get("result").get("status") == "complete":
                return True
            else:
                time.sleep(2)
                return query_status(url, headers=headers)
            
        # the query export status URL expects the "/file" segment to be chopped off
        # as per https://api.qualtrics.com/instructions/b3A6NjA5OTU-legacy-get-response-export-progress
        chopped_url = "/".join(url.split("/")[0:-1])


        if query_status(url=chopped_url, headers=headers):
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
                
            afile_name = os.path.join(
                cfg.survey_ingestion, '{survey_id}_response.zip'.format(survey_id=self.survey_id))
            
            # weaton: changed here, the original script did not ensure these folders existed before writing.
            if not os.path.exists(cfg.survey_ingestion):
                os.makedirs(cfg.survey_ingestion)
                os.mkdir(cfg.upload_loc)
                os.mkdir(cfg.unzipped_loc)
        
            with open(afile_name, 'wb') as f:
                f.write(resp.content)
            logging.info('File created successfully')
            return afile_name

        return ""

    def parse_response_body(self):
        """
        Parse the response fetched from API("surveys" and "survey-definition") to format and prepare data for question header table 
        and list of Question Ids
        :param: Qualtrics Object
        :return: absolute path of the file that was downloaded
        """
        try:
            resp = self.get_api_link()
            #second api call
            asurvey = self.survey_id
            survey_def_obj=Qualtrics_Obj(cfg.data_center, cfg.url4, asurvey, cfg.api_token, cfg.param1)
            resp_survey_def= survey_def_obj.get_api_link()
            
            abiglist = []
            obiglist = []
            qid_list = list(resp.get('result', {}).get('questions',{}).keys())
            q_num_list=list(resp.get('result', {}).get('exportColumnMap',{}).keys())
            #amaheswa
            #logging.info(f'qid_list : {qid_list} \n ..........\n q_num_list {q_num_list}')
            if resp and resp_survey_def:
                adict = {'header_type': 'question_id',
                         'survey_id': self.survey_id}
                bdict = {'header_type': 'question_name',
                         'survey_id': self.survey_id}
                cdict = {'header_type': 'question_name_full',
                         'survey_id': self.survey_id}
                ctr=1
                for q_num in q_num_list:
                    col_name = f'C{ctr}'
                    q_id=resp.get('result', {}).get('exportColumnMap',{}).get(q_num,{}).get('question',{})
                    q_name_full_temp=hlp.remove_html_tags(resp.get('result', {}).get('questions',{}).get(q_id,'').get('questionText',''))
                    q_name_temp=hlp.remove_html_tags(resp_survey_def.get('result', {}).get('Questions', {}).get(q_id,{}).get('QuestionDescription',{}))
                    
                    subQuestion_check=resp.get('result', {}).get('exportColumnMap',{}).get(q_num,{}).get('subQuestion')
                    choice_check=resp.get('result', {}).get('exportColumnMap',{}).get(q_num,{}).get('choice')
                    
                    if subQuestion_check is not None:
                        sub_choice_question_part=resp.get('result', {}).get('questions',{}).get(subQuestion_check.split('.')[0],{}).get(subQuestion_check.split('.')[1],{}).get(subQuestion_check.split('.')[2],{}).get('choiceText','')
                        
                        q_name = q_name_temp + '-' + hlp.remove_html_tags(sub_choice_question_part)
                        q_name_full = q_name_full_temp + '-' + hlp.remove_html_tags(sub_choice_question_part)
                        
                    elif choice_check is not None:
                        sub_choice_question_part=resp.get(
                            'result', {}).get('questions',{}).get(choice_check.split('.')[0],{}).get(choice_check.split('.')[1],{}).get(choice_check.split('.')[2],{}).get('choiceText','')
                        
                        q_name = q_name_temp + '-' + hlp.remove_html_tags(sub_choice_question_part)
                        q_name_full = q_name_full_temp + '-' + hlp.remove_html_tags(sub_choice_question_part)
                        
                    else:
                        q_name = q_name_temp
                        q_name_full = q_name_full_temp
    
                    adict[col_name] = q_num
                    bdict[col_name] = q_name
                    cdict[col_name] = q_name_full
                    ddict={'question_id': q_num, 'question_full': q_name_full}
                    abiglist.append(ddict)
                    ctr += 1

                obiglist.append(adict)
                obiglist.append(bdict)
                obiglist.append(cdict)
                #amaheswa
                #logging.info(f'abiglist : {abiglist} \n ..........\n : {obiglist}')
                #

            return [abiglist, obiglist], qid_list
        except Exception as e:
            logging.info('Could not extract datalist due to {}'.format(e))
            return [], []


@dataclass
class Redshift_Obj:
    host: str
    database: str
    user: str
    password: str

    def create_connection(self):
        import redshift_connector
        try:
            return redshift_connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                # sslmode="require",
                # port value of 5439 is specified by default
            )
        except Exception as e:
            logging.exception('Could not connect to database due to:-', exc_info=e)
            return None

    def create_engine(self):
        try:
            connect_string = "redshift+psycopg2://{user}:{password}@{host}:5439/{db}".format(user=self.user,
                                                                                             password=self.password, host=self.host, db=self.database)
            #logging.info('the connect engine is {}'.format(connect_string))
            engine = create_engine(connect_string,
                                   connect_args={'sslmode': 'require'}
                                   )
            engine = engine.execution_options(autocommit=True)
            return engine
        except Exception as e:
            logging.exception('Could not connect to database due to', exc_info=e)
            return None


#def get_questions_for_the_given_survey(asrv_obj):

#    #logging.info('I am in qet qstn def')
#    qlist, srv_ques_order = asrv_obj.parse_response_body()
#    return qlist, srv_ques_order


def download_all_survey_details(asurvey):
    '''
    Create object of surveys and call the function to parse the json response
    :return: Question Details data and Question Ids from survey
    '''
    asurvey_obj = Qualtrics_Obj(
        cfg.data_center, cfg.url1, asurvey, cfg.api_token, cfg.param1)
    #return get_questions_for_the_given_survey(asurvey_obj)
    return asurvey_obj.parse_response_body()


def upload_df_into_staging(adf: pd.DataFrame, adb: Redshift_Obj, tname: str):
    '''
    Upload dataframe to stage table
    :param: adf: pd.DataFrame
    :param: adb: Redshift_Obj
    :param: tname: Stage Table Name
    :return:
    '''
    try:
        engine = adb.create_engine()
        if engine != None:
            with engine.begin() as conn:
                adf.to_sql(tname, con=conn, schema=cfg.stage_schema, if_exists='append', index=False, method='multi',
                           chunksize=16384)
            return True
        else:
            logging.error('DB connection could not be initiated')
            return False
    except Exception as e:
        logging.exception(
            'Could not create a table from dataframe due to {}'.format(e), exc_info=True)
        sys.exit(1)


def get_last_response_for_given_survey(survey_id, db_conn):
    with db_conn.create_connection().cursor() as cursor:
        asql = cfg.fetch_last_response.format(survey_id=survey_id,)
        logging.info(f'the sql is {asql}')
        cursor.execute(asql)
        result: tuple = cursor.fetchall()
        alist = []
        logging.info(f'the list is {list(result)}')
        if list(result) and result[0] is not None:
            return result[0][0]
        return None


def parse_survey_data_body(actual_file, asurvey):
    """
    Read the Survey data file and deduce the survey data from the file
    :param actual_file: location of the actual file
    :param asurvey: Survey  Id for the associated file
    :return: DataFrame for the survey Data
    """
    adf = pd.read_csv(actual_file, index_col=False, skiprows=[1, 2], na_filter=False)
    
    ##[amaheswa] --logging testing
    #logging.info('the dataframe list is {}'.format(adf))
    #bdf=pd.read_csv(actual_file, index_col=False)
    #logging.info('the dataframe list is {}'.format(bdf))
    ##
    
    adf.insert(0, 'data_row_number', range(1, 1 + len(adf)))
    adf.insert(1, 'survey_id', asurvey)
    q_list = [elem for elem in list(adf) if re.search(
        "^Q\d+.*$", elem) is not None]
    ctr = 1
    repl_dict = {}
    for elem in q_list:
        col_lbl = f'c{str(ctr)}'
        repl_dict[elem] = col_lbl
        ctr += 1
    
    adf.rename(columns=repl_dict, inplace=True)
    adf.columns = [elem.lower() for elem in list(adf)]
    
    ##[amaheswa] --Testing
    #logging.info(f'the dataframe list before rename xlat is {list(adf)}')

    adf.rename(columns=cfg.body_xlat_cols, inplace=True)
    alist = [elem for elem in list(adf) if elem in cfg.body_col_order]
    ##[amaheswa] --uncommenting
    #logging.info(f'the dataframe list after rename is {list(adf)}')
    #logging.info(f'the list is {alist}')
    ##
    adf = adf[alist]
    adf = hlp.cleanup_date_cols(adf)
    ##[amaheswa] --Testing
    #logging.info(f'the adf is {adf}')
    ##
    return adf


def upload_file_via_s3_into_db(local_file, actual_file, adb):
    """
    First formats a sql to upload, upload the file using that sql and then runs the copy command
    :param local_file:
    :param actual_file:
    :return:
    """
    s3_upload_key = os.path.join(cfg.path_to_upload, actual_file)
    hlp.upload_a_file_into_s3(local_file, s3_upload_key, cfg.s3_bucket)
    get_upload_sql = hlp.format_upload_sql(cfg.upload_sql, cfg.s3_bucket, actual_file, cfg.access_key,
                                           cfg.access_secret)
    ###[amaheswa]
    logging.info(f'get upload sql  {get_upload_sql}')
    ###
    return hlp.upload_survey_data_into_stage(get_upload_sql, adb)


def start_process_for_uploading_data_file(body_df, file_path, adb):
    """
    Deduce the data file for the survey data
    :param body_df: The core dataframe which has the Survey Data
    :param file_path: Location of the source data file
    :param base_path: Location of the target data file
    :return: None
    """
    try:
        actual_file = os.path.basename(file_path).split(
            '.')[0].replace(' ', '_') + '_data.json'
        adict = body_df.to_dict('records')
        file_2_write = os.path.join(cfg.upload_loc, actual_file)
        with open(file_2_write, 'w') as f:
            for elem in adict:
                f.write(json.dumps(elem) + '\n')
        return upload_file_via_s3_into_db(file_2_write, actual_file, adb)
    except Exception as e:
        logging.info(f'Could not write content into the file due to {e}')
        return False


def ingest_survey_data_files(actual_file, asurvey, ared_db):
    """
    Core process for triggering the data download for the Body data for the given Survey
    :param actual_file: Location of the source data file
    :param asurvey: Survey ID
    :param ared_db: DB Object
    :return: 
    """
    body_df = parse_survey_data_body(actual_file, asurvey)
    #[amaheswa]
    logging.info(f'the data file body is {body_df} & file sent actual : {actual_file}')
    #
    if len(body_df) > 0:
        return start_process_for_uploading_data_file(body_df, actual_file, ared_db)
    else:
        logging.info(f'No Response Data found in Survey:- {asurvey}')
        return True
    


def extract_data_from_file(asurvey, srv_export_id, redDB):
    """
    Triggers the download process based on the export ID of the associated Survey
    :param asurvey: Survey ID
    :param srv_export_id: The export ID of the triggered process
    :param redDB: A DB Object
    :return:
    """
    asurvey_obj = Qualtrics_Obj(cfg.data_center, cfg.url3.format(
        result_id=srv_export_id), asurvey, cfg.api_token)
    downloaded_file = asurvey_obj.get_final_file()
    if downloaded_file > '':
        actual_file = os.path.basename(downloaded_file)
        logging.info(f'the path to the zipped file is {downloaded_file}, {actual_file}')
        with zipfile.ZipFile(downloaded_file, 'r') as zip_ref:
            zip_ref.extractall(cfg.unzipped_loc)
            listOfiles = zip_ref.namelist()
        logging.info(f'the zipped file is unzipped now proceed to upload {listOfiles}')
        if listOfiles[0]:
            return ingest_survey_data_files(os.path.join(
                cfg.unzipped_loc, listOfiles[0]), asurvey, redDB)
    #ingest_survey_data_files(os.path.join(cfg.unzipped_loc, afile), asurvey, redDB)


def prepare_to_download_survey_response(asurvey, survey_qs_order, dred):
    """
    Download the survey response content
    :param asurvey:
    :param survey_qs_order:
    :param dred:
    :return:
    """
    complete_list = cfg.survey_data_hist+survey_qs_order
    aresp_id = get_last_response_for_given_survey(asurvey, dred)
    #[amaheswa] Making Changes to retrieve full data incase no last responseID
    #if aresp_id is not None:
    cfg.data["surveyId"] = asurvey
    if aresp_id is not None:
        logging.info(f'Last Response Id found : {aresp_id}')
        cfg.data["lastResponseId"] = aresp_id
    else:
        logging.info(f'Last Response Id not found. Proceeding with full load of survey {asurvey}')
        cfg.data.pop("lastResponseId", None)
    cfg.data["includedQuestionIds"] = complete_list
    #
    #logging.info(f'The complete list is of payload data:- {cfg.data}')
    #
    asurvey_obj = Qualtrics_Obj(
        cfg.data_center, cfg.url2, asurvey, cfg.api_token)
    srv_export_id = asurvey_obj.post_api_link(json.dumps(cfg.data))
    if srv_export_id == '':
        logging.info(f'Export Id could not be fetched {srv_export_id}')
        return False
    logging.info(f'Now downloading data for {srv_export_id}')
    return extract_data_from_file(asurvey, srv_export_id, dred)


def add_question_data_to_header_tables(alist, dred: Optional[Redshift_Obj], upload_data: bool = True):
    """
    add the questions for a given survey to the two question tables: stg_qltrcs_srvy_qstn_dfntn_full and stg_qltrcs_srvy_hdr_new
    :param alist:
    :param dred: Redshift Object
    :return:
    """
    adf1 = pd.DataFrame(alist[0]) if type(alist[0]) == list else []
    stg_load_dttm = hlp.get_date_time_string()
    adf1['stg_load_dttm'] = stg_load_dttm
    adf1 = adf1.reindex(columns=cfg.survey_questions)
    logging.info('Now creating the 1st table')
    adf2 = pd.DataFrame(alist[1]) if type(alist[1]) == list else []
    #amaheswa commenting
    #adf2['stg_load_dttm'] = stg_load_dttm
    
    for elem in zip([adf1, adf2], [cfg.table_1, cfg.table_2]):
        adf = elem[0]
        tname = elem[1]
        if upload_data and dred != None:
        
            #[amaheswa] adding to log check --> stg_qltrcs_srvy_qstn_dfntn_full & stg_qltrcs_srvy_hdr_new
            logging.info(f'Loading data via add_question_data_to_header_files function for table:- {tname}')
            #logging.info(f'adf: {adf}')
            upload_df_into_staging(adf, dred, tname)
        else:
            return False
    return True


def onboard_all_survey_data(alist, adb):
    for asql in alist:
        
        #[amaheswa] Uncommenting to check log
        logging.info(f'Now running {asql}')
        #
        
        if not hlp.upload_survey_data_into_stage(asql, adb):
            logging.info(f'not able to upload the survey data successfully {asql}')
            break


def trigger_survey_pulls():
    """
    Trigger Survey pulling
    :return:
    """
    ctr = cfg.sleep_ctr
    frequency = parser.parse_args()
    input_freq = frequency.freq
    error_survey_list=[]
    success_survey_list=[]
    srv_ques_order = []
    ##
    #logging.info(f' for frequency:- {input_freq}')
    #exit()
    #
    #print(cfg.host, cfg.database, cfg.user, cfg.password)
    #logging.info(f'{cfg.host}, {cfg.database}, {cfg.user}, {cfg.password}')
    #exit()
    #
    ared = Redshift_Obj(host=cfg.host, database=cfg.database, user=cfg.user, password=cfg.password)
    with ared.create_connection().cursor() as cursor:
        list_of_surveys = hlp.get_list_of_surveys(cfg.freq_dict.get(input_freq) , cursor)
    
    #list_of_surveys = ["SV_a4f5aZoSPd9ejtP", "SV_74kgMZLcFi3d9jv", "SV_1HN9SygKblK3DeK"]
    #list_of_surveys = ["SV_74kgMZLcFi3d9jv"]
    logging.info(f'Number of surveys found:- {len(list_of_surveys)} for frequency:- {input_freq}')
    if len(list_of_surveys) <= 0:
        sys.exit()
    logging.info(f'The surveys for which load will be triggered:- {list_of_surveys}')

    dred = Redshift_Obj(host=cfg.host, database=cfg.database, user=cfg.user, password=cfg.password)
    
    hlp.reset_survey_stage_tables(cfg.tables_2b_reset, cfg.stage_schema, dred)
    for surveys in list_of_surveys:
        logging.info(f'Now working with survey:- {surveys}')
        qlist, srv_ques_order = download_all_survey_details(surveys)

        if qlist:
            # Now upload the survey header data
            if add_question_data_to_header_tables(qlist, dred):
                logging.info(
                    'Questions are added. Now proceeding to download, cleanse and upload survey data')
                # Now upload the survey data
                if prepare_to_download_survey_response(surveys, srv_ques_order, dred):
                    logging.info(f'Just finished uploading all survey data for {surveys}')
                    success_survey_list.append(surveys)
                else:
                    error_survey_list.append(surveys) 
                    continue
            else:
                error_survey_list.append(surveys) 
                continue
        logging.info(f'Going to sleep for :- {ctr} seconds')
        time.sleep(ctr)
        #ctr += cfg.sleep_ctr Need to check quota of API Call
    if len(success_survey_list) > 0:
        onboard_all_survey_data(cfg.sql_cmd_list, dred)
    logging.info(f'Number of surveys successfully loaded: {len(success_survey_list)}')
    logging.info(f'List of surveys successfully loaded: {success_survey_list}')
    if type(error_survey_list) == list and len(error_survey_list) > 0:
        #logging.info('All Success')
        logging.info(f'Number of surveys that failed to load: {len(error_survey_list)}')
        logging.info(f'List of surveys that failed to load: {error_survey_list}')
    



if __name__ == "__main__":
    # making a root span here to capture all of the metrics under it in APM
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("root"):
        trigger_survey_pulls()
