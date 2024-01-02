import awswrangler as wr
import boto3
import os
import zipfile
import pandas as pd
from io import BytesIO

def lambda_handler(event, context):

    s3 = boto3.client('s3')
    file_name = event['Records'][0]['s3']['object']['key']
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    bucket_source = 'jx-parquet-test/output'
    prefixes = ['DIAGNOSIS', 'INPATIENT_20', 'INPATIENT_MOVEMENT', 'OUTPATIENT', 'SERVICES']

    for prefix in prefixes:
        key_source = f'{prefix}.parquet'

        # Check if the parquet file exists, if not create an empty one
        if not wr.s3.does_object_exist(f's3://{bucket_source}/{key_source}'):
            empty_df = pd.DataFrame()
            wr.s3.to_parquet(empty_df, f's3://{bucket_source}/{key_source}')

        # Read the parquet file from S3 bucket_source/key_source into dataframe df_source
        df_source = wr.s3.read_parquet(f's3://{bucket_source}/{key_source}')

        # Unzip the zip file from S3: bucket_name/file_name, and keep in buffer within lambda function
        zip_obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        zip_file = zipfile.ZipFile(BytesIO(zip_obj['Body'].read()))

        # Within the unzipped files, read the txt file start with prefix, into data frame df_delta
        df_list = []
        for file in zip_file.namelist():
            if file.startswith(prefix) and file.endswith('.txt'):
                df_delta = pd.read_csv(zip_file.open(file), sep='\x1D\x1E\x1F', header=0, encoding='latin1', engine = 'python')
                df_delta['source_file_name'] = file
                df_list.append(df_delta)

        # Append df_delta to df_source
        if df_list:
            df_source = pd.concat([df_source] + df_list)

        # Save the new df_source to overwrite the old parquet file within S3 bucket_source/key_source
        buffer = BytesIO()
        try:
            df_source['PATIENT_AGE'] = pd.to_numeric(df_source['PATIENT_AGE'], errors='coerce')
        except:
            None
        df_source.to_parquet(buffer)
        df_source.drop_duplicates(subset=get_columns_to_remove(prefix), keep='last', inplace=True)
        wr.s3.to_parquet(df_source, f's3://{bucket_source}/{key_source}')
    

def get_columns_to_remove(argument):
    columns_to_remove =[
        ['PATIENT_NRIC', 'CASE_NO', 'CHECK_DIGIT', 'VISIT_NO', 'DIAG_SEQ_NO', 'CSN_NO'],
        ['PATIENT_NRIC', 'CASE_NO', 'CHECK_DIGIT', 'CSN_NO', 'ADM_DATE_TIME'],
        ['PATIENT_NRIC', 'CASE_NO', 'CHECK_DIGIT', 'MOVEMENT_SEQ_NO', 'CSN_NO', 'ADM_DATE_TIME', 'MOV_START_DATE_TIME', 'MOV_END_DATE_TIME'],
        ['PATIENT_NRIC', 'CASE_NO', 'CHECK_DIGIT', 'VISIT_NO', 'CSN_NO', 'VISIT_DATE_TIME'],
        ['PATIENT_NRIC', 'CASE_NO', 'CHECK_DIGIT', 'VISIT_NO', 'SERVICE_CD', 'SERVICE_SEQ_NO', 'CSN_NO', 'SERVICE_DATE']
    ]

    prefixes = ["DIAGNOSIS", "INPATIENT_20", "INPATIENT_MOVEMENT", "OUTPATIENT", "SERVICES"]

    if argument in prefixes:
        return columns_to_remove[prefixes.index(argument)]
    else:
        return None