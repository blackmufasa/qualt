from pathlib import Path
import os

survey_ingestion = os.path.join(Path.home(), 'files', 'qualtrics')
unzipped_loc = os.path.join(survey_ingestion, 'unzipped')
upload_loc = os.path.join(survey_ingestion, 'upload')

data_center = 'co1'
url1 = 'https://{data_center}.qualtrics.com/API/v3/survey-definitions/{survey_id}'

url2 = "https://co1.qualtrics.com/API/v3/responseexports"

url3 = "https://iad1.qualtrics.com/API/v3/responseexports/{result_id}/file"


param1 = (
    ('questions', ''),
)

data = {
    "format": "csv",
}


# S3 Credentials
s3_bucket = 'rhbibucket'
path_to_upload = 'cee_qualtrics_survey'


#api_dict = {url1: param1, url2: param2}
freq_dict = {'d': 'DAILY', 'w': 'WEEKLY'}
list_of_surveys = "select surveyid from rsdw_stg.stg_qltrcs_survey_list_hist where active_flag = true and " \
                  "run_frequency  = '{freq}'"
host = os.environ.get(
    "REDSHIFT_HOST", 'rhdwrsstage.cpe1poooghvl.us-west-2.redshift.amazonaws.com')
user = os.environ.get("REDSHIFT_USER")
password = os.environ.get("REDSHIFT_PASSWORD")
database = 'rhdwrs'
survey_questions = ['stg_load_dttm', 'question_id', 'question_full']
table_1 = 'stg_qltrcs_srvy_qstn_dfntn_full'
table_2 = 'stg_qltrcs_srvy_hdr_hist'
schema = 'rsdw_stg'
api_token = os.getenv("QUALTRICS_API_TOKEN")


fetch_last_response = """select response_Id from rsdw_stg.stg_qltrcs_srvy_data_hist where survey_id = 
                        '{survey_id}' order by stg_load_dttm desc, data_row_number desc limit 1;"""

data = {"format": "csv", "surveyId": "{survey}",
        "lastResponseId": "{last_response}"}

survey_data_hist = ["DataRowNumber", "SurveyID", "ResponseID", "ResponseSet", "IPAddress", "StartDate", "EndDate",
                    "RecipientLastName", "RecipientFirstName", "RecipientEmail", "ExternalDataReference", "Finished",
                    "Status", "Complete", "Case_AccountNumber", "Case_CaseNumber", "Case_CreatedDate",
                    "Case_LastClosedAt", "Case_LastModifiedDate", "Case_OwnerId", "Case_Product", "Case_RecordTypeDesc",
                    "Case_RecordTypeName", "Case_Severity", "Case_Status", "Case_Subject", "Case_Type", "Case_Version",
                    "cdh_party_number_account", "cdh_party_number_contact", "cee_account_country",
                    "cee_account_eod_enhanced_delivery", "cee_account_global_region", "cee_account_ispartner",
                    "cee_account_name", "cee_account_partner_level", "cee_account_partner_type",
                    "cee_account_super_region", "Cert_CatalogLink", "Cert_Identifier", "closed_booked_opps",
                    "contact_city", "contact_country_eloqua", "contact_country_final", "contact_country_sfdc",
                    "contact_created_date", "contact_email_opt_out", "contact_first_name", "contact_full_name",
                    "contact_is_active", "contact_is_bounced_eloqua", "contact_is_bounced_final",
                    "contact_is_bounced_gs", "contact_is_bounced_sfdc", "contact_is_developer", "contact_is_internal",
                    "contact_is_org_admin", "contact_is_subscribed", "contact_is_tam_contact", "contact_is_viable",
                    "contact_job_persona", "contact_job_role", "contact_job_title_eloqua", "contact_job_title_final",
                    "contact_job_title_sfdc", "contact_last_nps_contact_role", "contact_last_nps_email_sent_date",
                    "contact_last_nps_response_date", "contact_last_nps_response_email_sent_date",
                    "contact_last_nps_score", "contact_lightblue_id", "contact_null_fields", "contact_phone",
                    "contact_preferred_language_eloqua", "contact_preferred_language_sfdc", "contact_salutation",
                    "contact_sfdc_instance", "contact_sso_username", "contact_state_prov", "contact_subregion",
                    "contact_super_region_eloqua", "contact_super_region_final", "contact_super_region_sfdc",
                    "contains_human_name", "csm_email", "csm_fullname", "csm_geo", "csm_manager", "csm_manager_email",
                    "csm_phone", "csm_sso_username", "csm_sub_geo", "duns_number", "duplicate_count",
                    "ebs_account_number", "ebs_account_number_account", "ebs_account_number_contact",
                    "eloqua_contact_id", "first_booked_opp", "first_booked_opp_month", "first_sub_start_date",
                    "first_sub_start_date_month", "interaction_status", "interaction_status_modified_timestamp",
                    "is_telco", "name", "NPS_Round", "psm_email", "psm_fullname", "psm_geo", "psm_manager",
                    "psm_manager_email", "psm_phone", "psm_sso_username", "psm_sub_geo", "sales_account_global_region",
                    "sales_account_name", "sales_account_partner_flag", "sales_account_partner_grouping",
                    "sales_account_partner_type", "sales_account_sic_code", "sales_account_vertical_industry",
                    "sales_account_vertical_primary", "sales_account_vertical_secondary", "sales_classification",
                    "sales_global_customer_name", "sales_owner", "sales_owner_email", "sales_owner_geo",
                    "sales_owner_manager", "sales_owner_subregion", "sales_owner_territory", "sfdc_account_id",
                    "sfdc_account_id_cee", "sfdc_account_id_sales", "sfdc_contact_id", "survey_case_id",
                    "survey_feedback_id", "tam_cloud_email", "tam_cloud_fullname", "tam_cloud_geo", "tam_cloud_manager",
                    "tam_cloud_manager_email", "tam_cloud_phone", "tam_cloud_sso_username", "tam_cloud_sub_geo",
                    "tam_middleware_email", "tam_middleware_fullname", "tam_middleware_geo", "tam_middleware_manager",
                    "tam_middleware_manager_email", "tam_middleware_phone", "tam_middleware_sso_username",
                    "tam_middleware_sub_geo", "tam_other_email", "tam_other_fullname", "tam_other_geo",
                    "tam_other_manager", "tam_other_manager_email", "tam_other_phone", "tam_other_sso_username",
                    "tam_other_sub_geo", "tam_platform_email", "tam_platform_fullname", "tam_platform_geo",
                    "tam_platform_manager", "tam_platform_manager_email", "tam_platform_phone",
                    "tam_platform_sso_username", "tam_platform_sub_geo", "tam_storage_email", "tam_storage_fullname",
                    "tam_storage_geo", "tam_storage_manager", "tam_storage_manager_email", "tam_storage_phone",
                    "tam_storage_sso_username", "tam_storage_sub_geo", "Current Page URL", "Customer Account Number",
                    "RH User ID", "Q_CHL", "Q_Language", "PanelID", "UserAgent", "Q_URL", "Referer", "Q_TotalDuration",
                    "Completed", "Email", "EventName", "Resource Attributes", "business_partner_type", "account_id",
                    "partner_source", "finder_partner_type_name", "technology_partner_type"]


db_host = {
        'prod': {
                'host': 'rhdwrsprod.cpe1poooghvl.us-west-2.redshift.amazonaws.com',
                'database': 'rhdwrs'
                },
        'stage': {
                'host': 'rhdwrsstage.cpe1poooghvl.us-west-2.redshift.amazonaws.com',
                'database': 'rhdwrs'
                },
        'dev': {
                'host': 'rhdwrsqa.corp.qa.redhat.com',
                'database': 'dwrsdev'
                }
}

stage_header_table = 'stg_qltrcs_srvy_hdr_new'
stage_schema = 'rsdw_stg'
#stage_data_table = 'stg_qltrcs_srvy_data_new_alt'
stage_add_seq = [stage_header_table]
tables_2b_reset = [table_1, table_2,
                   stage_header_table, 'stg_qltrcs_srvy_data_new']

erase_table_sql = 'Truncate table {schema}.{tname}'

body_xlat_cols = repl_dict = {'responseid': 'response_id', 'responseset': 'response_set', 'startdate': 'start_date',
                              'enddate': 'end_date', 'recipientlastname': 'recipient_last_name', 'surveyid': 'survey_id',
                              'recipientfirstname': 'recipient_first_name', 'recipientemail': 'recipient_email',
                              'externaldatareference': 'external_data_reference', 'case_accountnumber': 'case_account_number',
                              'case_casenumber': 'case_case_number', 'case_createddate': 'case_created_date',
                              'case_lastclosedat': 'case_last_closed_at', 'case_lastmodifieddate': 'case_last_modified_date',
                              'case_ownerid': 'case_owner_id', 'case_recordtypedesc': 'case_record_type_desc',
                              'case_recordtypename': 'case_record_type_name', 'cert_cataloglink': 'cert_catalog_link'}

header_col_order = ['header_type', 'survey_id', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'c10', 'c11',
                    'c12', 'c13', 'c14', 'c15', 'c16', 'c17', 'c18', 'c19', 'c20', 'c21', 'c22', 'c23', 'c24', 'c25',
                    'c26', 'c27', 'c28', 'c29', 'c30', 'c31', 'c32', 'c33', 'c34', 'c35', 'c36', 'c37', 'c38', 'c39',
                    'c40', 'c41', 'c42', 'c43', 'c44', 'c45', 'c46', 'c47', 'c48', 'c49', 'c50', 'c51', 'c52', 'c53',
                    'c54', 'c55', 'c56', 'c57', 'c58', 'c59', 'c60', 'c61', 'c62', 'c63', 'c64', 'c65', 'c66', 'c67',
                    'c68', 'c69', 'c70', 'c71', 'c72', 'c73', 'c74', 'c75', 'c76', 'c77', 'c78', 'c79', 'c80', 'c81',
                    'c82', 'c83', 'c84', 'c85', 'c86', 'c87', 'c88', 'c89', 'c90', 'c91', 'c92', 'c93', 'c94', 'c95',
                    'c96', 'c97', 'c98', 'c99', 'c100', 'c101', 'c102', 'c103', 'c104', 'c105', 'c106', 'c107', 'c108',
                    'c109', 'c110', 'c111', 'c112', 'c113', 'c114', 'c115', 'c116', 'c117', 'c118', 'c119', 'c120',
                    'c121', 'c122', 'c123', 'c124', 'c125', 'c126', 'c127', 'c128', 'c129', 'c130', 'c131', 'c132',
                    'c133', 'c134', 'c135', 'c136', 'c137', 'c138', 'c139', 'c140', 'c141', 'c142', 'c143', 'c144',
                    'c145', 'c146', 'c147', 'c148', 'c149', 'c150', 'c151', 'c152', 'c153', 'c154', 'c155', 'c156',
                    'c157', 'c158', 'c159', 'c160', 'c161', 'c162', 'c163', 'c164', 'c165', 'c166', 'c167', 'c168',
                    'c169', 'c170', 'c171', 'c172', 'c173', 'c174', 'c175', 'c176', 'c177', 'c178', 'c179', 'c180',
                    'c181', 'c182', 'c183', 'c184', 'c185', 'c186', 'c187', 'c188', 'c189', 'c190', 'c191', 'c192',
                    'c193', 'c194', 'c195', 'c196', 'c197', 'c198', 'c199', 'c200', 'c201', 'c202', 'c203', 'c204',
                    'c205', 'c206', 'c207', 'c208', 'c209', 'c210', 'c211', 'c212', 'c213', 'c214', 'c215', 'c216',
                    'c217', 'c218', 'c219', 'c220', 'c221', 'c222', 'c223', 'c224', 'c225', 'c226', 'c227', 'c228',
                    'c229', 'c230', 'c231', 'c232', 'c233', 'c234', 'c235', 'c236', 'c237', 'c238', 'c239', 'c240',
                    'c241', 'c242', 'c243', 'c244', 'c245', 'c246', 'c247', 'c248', 'c249', 'c250', 'c251', 'c252',
                    'c253', 'c254', 'c255', 'c256', 'c257', 'c258', 'c259', 'c260', 'c261', 'c262', 'c263', 'c264',
                    'c265', 'c266', 'c267', 'c268', 'c269', 'c270', 'c271', 'c272', 'c273', 'c274', 'c275', 'c276',
                    'c277', 'c278', 'c279', 'c280', 'c281', 'c282', 'c283', 'c284', 'c285', 'c286', 'c287', 'c288',
                    'c289', 'c290', 'c291', 'c292', 'c293', 'c294', 'c295', 'c296', 'c297', 'c298', 'c299', 'c300']

body_col_order = ['data_row_number', 'survey_id', 'response_id', 'response_set', 'ipaddress', 'start_date', 'end_date',
                  'recipient_last_name', 'recipient_first_name', 'recipient_email', 'external_data_reference', 'finished',
                  'status', 'complete', 'case_account_number', 'case_case_number', 'case_created_date', 'case_last_closed_at',
                  'case_last_modified_date', 'case_owner_id', 'case_product', 'case_record_type_desc',
                  'case_record_type_name', 'case_severity', 'case_status', 'case_subject', 'case_type',
                  'case_version', 'cdh_party_number_account', 'cdh_party_number_contact', 'cee_account_country',
                  'cee_account_eod_enhanced_delivery', 'cee_account_global_region', 'cee_account_ispartner',
                  'cee_account_name', 'cee_account_partner_level', 'cee_account_partner_type', 'cee_account_super_region',
                  'cert_catalog_link', 'cert_identifier', 'contact_city', 'contact_country_eloqua', 'contact_country_final',
                  'contact_country_sfdc', 'contact_created_date', 'contact_email_opt_out', 'contact_first_name',
                  'contact_full_name', 'contact_is_active', 'contact_is_bounced_eloqua', 'contact_is_bounced_final',
                  'contact_is_bounced_gs', 'contact_is_bounced_sfdc', 'contact_is_developer', 'contact_is_internal',
                  'contact_is_org_admin', 'contact_is_subscribed', 'contact_is_tam_contact', 'contact_is_viable',
                  'contact_job_persona', 'contact_job_role', 'contact_job_title_eloqua', 'contact_job_title_final',
                  'contact_job_title_sfdc', 'contact_last_nps_contact_role', 'contact_lightblue_id', 'contact_null_fields',
                  'contact_phone', 'contact_preferred_language_eloqua', 'contact_preferred_language_sfdc',
                  'contact_salutation', 'contact_sfdc_instance', 'contact_sso_username', 'contact_state_prov',
                  'contact_subregion', 'contact_super_region_eloqua', 'contact_super_region_final',
                  'contact_super_region_sfdc', 'contains_human_name', 'csm_email', 'csm_fullname', 'csm_geo',
                  'csm_manager', 'csm_manager_email', 'csm_phone', 'csm_sso_username', 'csm_sub_geo', 'duplicate_count',
                  'ebs_account_number', 'ebs_account_number_account', 'eloqua_contact_id', 'first_booked_opp_month',
                  'first_sub_start_date', 'first_sub_start_date_month', 'interaction_status',
                  'interaction_status_modified_timestamp', 'is_telco', 'name', 'nps_round', 'psm_email', 'psm_fullname',
                  'psm_geo', 'psm_manager', 'psm_manager_email', 'psm_phone', 'psm_sso_username', 'psm_sub_geo',
                  'sales_account_global_region', 'sales_account_name', 'sales_account_partner_flag',
                  'sales_account_partner_grouping', 'sales_account_partner_type', 'sales_account_sic_code',
                  'sales_account_vertical_industry', 'sales_account_vertical_primary', 'sales_account_vertical_secondary',
                  'sales_classification', 'sales_global_customer_name', 'sales_owner', 'sales_owner_email',
                  'sales_owner_geo', 'sales_owner_manager', 'sales_owner_subregion', 'sales_owner_territory',
                  'sfdc_account_id', 'sfdc_account_id_cee', 'sfdc_account_id_sales', 'sfdc_contact_id',
                  'survey_case_id', 'survey_feedback_id', 'tam_cloud_email', 'tam_cloud_fullname', 'tam_cloud_geo',
                  'tam_cloud_manager', 'tam_cloud_manager_email', 'tam_cloud_phone', 'tam_cloud_sso_username',
                  'tam_cloud_sub_geo', 'tam_middleware_email', 'tam_middleware_fullname', 'tam_middleware_geo',
                  'tam_middleware_manager', 'tam_middleware_manager_email', 'tam_middleware_phone',
                  'tam_middleware_sso_username', 'tam_middleware_sub_geo', 'tam_other_email', 'tam_other_fullname',
                  'tam_other_geo', 'tam_other_manager', 'tam_other_manager_email', 'tam_other_phone',
                  'tam_other_sso_username', 'tam_other_sub_geo', 'tam_platform_email', 'tam_platform_fullname',
                  'tam_platform_geo', 'tam_platform_manager', 'tam_platform_manager_email', 'tam_platform_phone',
                  'tam_platform_sso_username', 'tam_platform_sub_geo', 'tam_storage_email', 'tam_storage_fullname',
                  'tam_storage_geo', 'tam_storage_manager', 'tam_storage_manager_email', 'tam_storage_phone',
                  'tam_storage_sso_username', 'tam_storage_sub_geo', 'current_page_url', 'customer_account_number',
                  'rh_user_id', 'q_chl', 'q_language', 'panel_id', 'user_agent', 'q_url', 'referer', 'q_total_duration',
                  'completed', 'email', 'event_name', 'resource_attributes', 'business_partner_type', 'account_id',
                  'partner_source', 'finder_partner_type_name', 'technology_partner_type', 'calculated_persona',
                  'calculated_persona_title', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'c10', 'c11', 'c12',
                  'c13', 'c14', 'c15', 'c16', 'c17', 'c18', 'c19', 'c20', 'c21', 'c22', 'c23', 'c24', 'c25', 'c26', 'c27',
                  'c28', 'c29', 'c30', 'c31', 'c32', 'c33', 'c34', 'c35', 'c36', 'c37', 'c38', 'c39', 'c40', 'c41', 'c42',
                  'c43', 'c44', 'c45', 'c46', 'c47', 'c48', 'c49', 'c50', 'c51', 'c52', 'c53', 'c54', 'c55', 'c56', 'c57',
                  'c58', 'c59', 'c60', 'c61', 'c62', 'c63', 'c64', 'c65', 'c66', 'c67', 'c68', 'c69', 'c70', 'c71', 'c72',
                  'c73', 'c74', 'c75', 'c76', 'c77', 'c78', 'c79', 'c80', 'c81', 'c82', 'c83', 'c84', 'c85', 'c86', 'c87',
                  'c88', 'c89', 'c90', 'c91', 'c92', 'c93', 'c94', 'c95', 'c96', 'c97', 'c98', 'c99', 'c100', 'c101',
                  'c102', 'c103', 'c104', 'c105', 'c106', 'c107', 'c108', 'c109', 'c110', 'c111', 'c112', 'c113', 'c114',
                  'c115', 'c116', 'c117', 'c118', 'c119', 'c120', 'c121', 'c122', 'c123', 'c124', 'c125', 'c126', 'c127',
                  'c128', 'c129', 'c130', 'c131', 'c132', 'c133', 'c134', 'c135', 'c136', 'c137', 'c138', 'c139', 'c140',
                  'c141', 'c142', 'c143', 'c144', 'c145', 'c146', 'c147', 'c148', 'c149', 'c150', 'c151', 'c152', 'c153',
                  'c154', 'c155', 'c156', 'c157', 'c158', 'c159', 'c160', 'c161', 'c162', 'c163', 'c164', 'c165', 'c166',
                  'c167', 'c168', 'c169', 'c170', 'c171', 'c172', 'c173', 'c174', 'c175', 'c176', 'c177', 'c178', 'c179',
                  'c180', 'c181', 'c182', 'c183', 'c184', 'c185', 'c186', 'c187', 'c188', 'c189', 'c190', 'c191', 'c192',
                  'c193', 'c194', 'c195', 'c196', 'c197', 'c198', 'c199', 'c200', 'c201', 'c202', 'c203', 'c204', 'c205',
                  'c206', 'c207', 'c208', 'c209', 'c210', 'c211', 'c212', 'c213', 'c214', 'c215', 'c216', 'c217', 'c218',
                  'c219', 'c220', 'c221', 'c222', 'c223', 'c224', 'c225', 'c226', 'c227', 'c228', 'c229', 'c230', 'c231',
                  'c232', 'c233', 'c234', 'c235', 'c236', 'c237', 'c238', 'c239', 'c240', 'c241', 'c242', 'c243', 'c244',
                  'c245', 'c246', 'c247', 'c248', 'c249', 'c250', 'c251', 'c252', 'c253', 'c254', 'c255', 'c256', 'c257',
                  'c258', 'c259', 'c260', 'c261', 'c262', 'c263', 'c264', 'c265', 'c266', 'c267', 'c268', 'c269', 'c270',
                  'c271', 'c272', 'c273', 'c274', 'c275', 'c276', 'c277', 'c278', 'c279', 'c280', 'c281', 'c282', 'c283',
                  'c284', 'c285', 'c286', 'c287', 'c288', 'c289', 'c290', 'c291', 'c292', 'c293', 'c294', 'c295', 'c296',
                  'c297', 'c298', 'c299', 'c300']

# All the date columns are scrubbed  so there are no conversion errors
body_date_cols = ['contact_last_nps_email_sent_date', 'contact_last_nps_response_email_sent_date',
                  'contact_last_nps_response_date', 'contact_created_date', 'first_booked_opp', 'first_sub_start_date',
                  'interaction_status_modified_timestamp', 'contact_created_date', 'case_created_date',
                  'case_last_closed_at', 'case_last_modified_date', 'start_date', 'end_date']

# All the numeric columns are srubbed so that blank fields are properly initialized
numeric_cols_body = ['data_row_number', 'finished', 'status', 'cdh_party_number_account', 'cdh_party_number_contact',
                     'closed_booked_opps', 'contact_last_nps_score', 'contact_null_fields', 'duns_number', 'duplicate_count',
                     'ebs_account_number', 'ebs_account_number_account', 'ebs_account_number_contact',
                     'first_booked_opp_month', 'first_sub_start_date_month']

# SQL used to transfer data from S3 into Redshift tables.
upload_sql = """
            COPY rsdw_stg.stg_qltrcs_srvy_data_new  
            FROM 's3://{bucket}/cee_qualtrics_survey/{file_name}'
            TRUNCATECOLUMNS
            ACCEPTINVCHARS
            BLANKSASNULL
            COMPUPDATE OFF
            STATUPDATE off
            credentials 'aws_access_key_id={access_key};aws_secret_access_key={access_secret}'
            json 'auto'
            maxerror 10
            timeformat 'auto';
            """
sql1 = """DROP TABLE IF EXISTS tmp_variables;"""
sql2 = """CREATE TABLE tmp_variables AS SELECT CURRENT_TIMESTAMP :: DATETIME AS stg_end_dttm;"""
sql3 = """INSERT INTO rsdw_stg.stg_qltrcs_srvy_data_hist
        SELECT (SELECT stg_end_dttm FROM tmp_variables) as stg_load_dttm, * FROM rsdw_stg.stg_qltrcs_srvy_data_new
        WHERE NOT (status = 8 AND q_total_duration < 2 AND finished = 0);"""
sql4 = """TRUNCATE TABLE rsdw_stg.stg_qltrcs_srvy_data_new;"""
sql5 = """truncate table rsdw_stg.stg_qltrcs_srvy_hdr_old;"""

sql6 = """insert into rsdw_stg.stg_qltrcs_srvy_hdr_old select header_type, survey_id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33, c34, c35, c36, c37, c38, c39, c40, c41, c42, c43, c44, c45,c46, c47, c48, c49, c50, c51, c52, c53, c54, c55, c56, c57, c58, c59, c60, c61, c62, c63, c64, c65, c66, c67, c68, c69, c70, c71, c72, c73, c74, c75, c76, c77, c78, c79, c80, c81, c82, c83, c84, c85, c86, c87, c88, c89, c90, c91, c92, c93, c94,c95, c96, c97, c98, c99, c100, c101, c102, c103, c104, c105, c106, c107, c108, c109, c110, c111, c112, c113, c114, c115, c116, c117, c118, c119, c120, c121, c122, c123, c124, c125, c126, c127, c128, c129, c130, c131, c132, c133, c134, c135, c136, c137, c138, c139, c140, c141, c142, c143,c144, c145, c146, c147, c148, c149, c150, c151, c152, c153, c154, c155, c156, c157, c158, c159, c160, c161, c162, c163, c164, c165, c166, c167, c168, c169, c170, c171, c172, c173, c174, c175, c176, c177, c178, c179, c180, c181, c182, c183, c184, c185, c186, c187, c188, c189, c190, c191, c192,c193, c194, c195, c196, c197, c198, c199, c200, c201, c202, c203, c204, c205, c206, c207, c208, c209, c210, c211, c212, c213, c214, c215, c216, c217, c218, c219, c220, c221, c222, c223, c224, c225, c226, c227, c228, c229, c230, c231, c232, c233, c234, c235, c236, c237, c238, c239, c240, c241,c242, c243, c244, c245, c246, c247, c248, c249, c250, c251, c252, c253, c254, c255, c256, c257, c258, c259, c260, c261, c262, c263, c264, c265, c266, c267, c268, c269, c270, c271, c272, c273, c274, c275, c276, c277, c278, c279, c280, c281, c282, c283, c284, c285, c286, c287, c288, c289, c290,c291, c292, c293, c294, c295, c296, c297, c298, c299, c300 from rsdw_stg.stg_qltrcs_srvy_hdr_hist where survey_id = ( select distinct survey_id from rsdw_stg.stg_qltrcs_srvy_hdr_new) and stg_curr_flg = true ;"""
sql7 = """INSERT INTO rsdw_stg.stg_qltrcs_srvy_hdr_hist
SELECT
( SELECT stg_end_dttm FROM tmp_variables) as stg_load_dttm,
1 as stg_curr_flg,
( SELECT stg_end_dttm FROM tmp_variables) as stg_start_dttm,
'99991231' as stg_end_dttm,
*
FROM
( SELECT * FROM rsdw_stg.stg_qltrcs_srvy_hdr_new
MINUS
SELECT * FROM rsdw_stg.stg_qltrcs_srvy_hdr_old);"""
sql8 = """TRUNCATE TABLE rsdw_stg.stg_qltrcs_srvy_hdr_old; """
sql9 = """INSERT INTO rsdw_stg.stg_qltrcs_srvy_hdr_old SELECT * FROM 
rsdw_stg.stg_qltrcs_srvy_hdr_new;"""
sql10 = """UPDATE rsdw_stg.stg_qltrcs_srvy_hdr_hist
SET stg_start_dttm = stg_start_dttm_new
, stg_end_dttm = stg_end_dttm_new
, stg_curr_flg = stg_curr_flg_new
FROM
(
SELECT
header_type
,survey_id
,stg_load_dttm
, case when lag(stg_load_dttm) over (partition by header_type, survey_id order by
stg_load_dttm asc ) is null then to_date ( '19000101' , 'yyyymmdd' )
else stg_load_dttm
end as stg_start_dttm_new
, nvl (lead(stg_load_dttm) over (partition by header_type, survey_id order by
stg_load_dttm asc ), to_date ( '99991231' , 'yyyymmdd' )) as stg_end_dttm_new, case when lead(stg_load_dttm) over (partition by header_type, survey_id order by
stg_load_dttm asc ) is null then 1 else 0 end as stg_curr_flg_new
FROM rsdw_stg.stg_qltrcs_srvy_hdr_hist
WHERE
survey_id in ( select survey_id from rsdw_stg.stg_qltrcs_srvy_hdr_hist where
stg_load_dttm > sysdate - 1)
) a
WHERE
rsdw_stg.stg_qltrcs_srvy_hdr_hist.header_type = a.header_type
and rsdw_stg.stg_qltrcs_srvy_hdr_hist.survey_id = a.survey_id
and rsdw_stg.stg_qltrcs_srvy_hdr_hist.stg_load_dttm = a.stg_load_dttm
and (rsdw_stg.stg_qltrcs_srvy_hdr_hist.stg_start_dttm <> a.stg_start_dttm_new or
rsdw_stg.stg_qltrcs_srvy_hdr_hist.stg_end_dttm <> a.stg_end_dttm_new or
rsdw_stg.stg_qltrcs_srvy_hdr_hist.stg_curr_flg <> a.stg_curr_flg_new)
;"""
sql11 = """TRUNCATE TABLE rsdw_stg.stg_qltrcs_srvy_hdr_new;"""
sql_cmd_list = [sql1, sql2, sql3, sql4, sql5,
                sql6, sql7, sql8, sql9, sql10, sql11]
sleep_ctr = 60
