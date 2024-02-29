import os
import pandas as pd
import json
import cx_Oracle
import sqlalchemy
from sqlalchemy import create_engine, types,text
import re
import shutil
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime

processed_files = []
run_id =''
file_count = 0
error_count = 0
load_count = 0
other_errors = []
# Folder containing files
folder_path = 'D:/Test_files'

# Path to JSON file containing database credentials
credentials_path = 'D:/myProjects/Codebase/oracle_db.json'
# Archive Folder for successfully processed files
processed_folder = 'D:/Archives'


# Function to replace non-alphanumeric characters with underscores
def clean_name(name):
    return re.sub(r'\W+', '_', name)

# Function to infer schema and data types, including handling dates
def infer_schema(df):
    print(df.info())
     # Infer data types
    dtype_mapping = {
        'str': types.Unicode(250),
        'int64': types.INTEGER(),
        'float64': types.FLOAT(20,10),
        'datetime64[ns]': types.Date(),  # Map datetime64 type to Oracle DATE
        'object': types.Unicode(250)  # Map object type to Oracle VARCHAR
    }
    print(dtype_mapping)
    column_types = {col : dtype_mapping.get(df[col].dtype, types.Unicode(250) ) for col in df.columns}
    print(column_types)
    return column_types

# Function to create table in Oracle database
def create_table(engine, table_name, column_types):
    with engine.connect() as connection:
        # Check if table exists
        query = text(f"SELECT COUNT(*) FROM user_tables WHERE table_name = '{table_name}'")
        result = connection.execute(query)
        table_exists = bool(result.scalar())

        # If table doesn't exist, create it
        if not table_exists:
            column_definitions = ', '.join([f"{col} {dtype}" for col, dtype in column_types.items()])
            create_table_query = text(f"CREATE TABLE {table_name} ({column_definitions})")
            connection.execute(create_table_query)

# Function to load data into Oracle database and handle file movement
def load_data(engine, df, table_name):
    
    column_types = infer_schema(df)

    #create_table(engine, table_name, column_types) # may need to create table explicitly

    # Convert object-type columns to string
    object_columns = df.select_dtypes(include=['object']).columns
    df[object_columns] = df[object_columns].astype(str)

    # load data into table
    df.to_sql(table_name, engine, if_exists='append', index=False,dtype=column_types)
    print(f"Data successfully loaded into table '{table_name}'")

    
# Function to send email notification with excel attachment
def send_email_notification():
    email_config = {
        'sender_email': 'venkat.m@uchicago.edu',
        'receiver_email': 'vmarakala@uchicago.edu',
        'smtp_server': 'smtp.uchicago.edu',
        'smtp_port': 25
    }

    msg = MIMEMultipart()
    msg['From'] = email_config['sender_email']
    msg['To'] = email_config['receiver_email']
    msg['Subject'] = 'Notification: Data Loading Status'

    body = "Custom Files Data loading status:\n\n"

    # Append system errors information
    body += ", ".join(other_errors)
    # Append information about processed files
    body += f"Total Files Processed: {file_count}\n"
    body += f"Total Files Loaded: {load_count}\n"
    body += f"Total Files errored: {error_count}\n"
    
    # Write error info to XLS file
    process_info_df = pd.DataFrame(processed_files)
    process_info_df.to_excel("process_info.xlsx", index=False)

    # Attach XLS file to the email
    with open('process_info.xlsx', 'rb') as file:
        xls_attachment = file.read()

    attachment = MIMEText(xls_attachment, 'base64', 'utf-8')
    attachment['Content-Disposition'] = 'attachment; filename=process_info.xlsx'
    msg.attach(attachment)

    # Add body text to the email
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
            server.starttls()
            #server.login(email_config['smtp_username'], email_config['smtp_password'])
            server.send_message(msg)
            print("Email notification sent successfully")
    except Exception as e:
        print(f"Error sending email notification: {str(e)}")


# Function to connect to Oracle database
def connect_to_oracle(credentials_path):
    with open(credentials_path, 'r') as f:
        credentials = json.load(f)

    try:
        cx_Oracle.init_oracle_client(lib_dir=r"D:/condapython/instantclient_19_22")
        print(cx_Oracle.clientversion())
    except Exception as client_error:
        print(f"Error connecting to Database client: {str(client_error)}")
    
    engine = create_engine(
        f"oracle+cx_oracle://{credentials['username']}:{credentials['password']}@"
        f"{credentials['host']}:{credentials['port']}/{credentials['database']}",
        connect_args={"encoding": "UTF-8", "nencoding": "UTF-8"}
    )
    return engine


# Main function
def main():
    global file_count
    global load_count
    global error_count
    try:
        if not os.path.isdir(folder_path):
            raise ValueError("Invalid folder path")

        # Connect to Oracle database
        engine = connect_to_oracle(credentials_path)

        # Capture program run start time, convert it to a VARCHAR type to use it as a Batch Identifier
        run_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            file_count += 1
            try:
                if os.path.isfile(file_path):
                    table_name = clean_name(os.path.splitext(file_name)[0]).upper()  # Use file name without extension as table name
                    print(table_name)
                    if file_path.endswith('.csv'):
                        df = pd.read_csv(file_path, parse_dates=True,header='infer',delimiter=',')
                    elif file_path.endswith('.xls') or file_path.endswith('.xlsx'):
                        df = pd.read_excel(file_path, parse_dates=True)
                    else:
                        raise ValueError("Unsupported file format")
                    df.columns = [clean_name(col).upper() for col in df.columns]
                    # Convert column data types with all null values to string
                    columns_with_all_null = df.columns[df.isnull().all()]
                    df[columns_with_all_null] = df[columns_with_all_null].astype(str)
                    # Replace NaN with empty string to infer correct data types
                    #df.fillna('', inplace=True)
                    #print(df.info())
                    # Convert mixed date types to yyyy-mm-dd format
                    for col in df.select_dtypes(include='datetime64'):
                        df[col] = pd.to_datetime(df[col].dt.strftime('%Y-%m-%d'))
                
                    # Add Audit columns
                    df['FILE_NAME'] = file_name  # Add file name as a column
                    df['BATCH_ID'] = run_id  # Add run_id column
                    df['CREATION_DATE'] = pd.to_datetime(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    print(df.head(5))
                    
                    load_data(engine, df, table_name)
                    
                    # Move file to processed folder
                    shutil.move(file_path, os.path.join(processed_folder, os.path.basename(file_path)))
                    print(f"File '{os.path.basename(file_path)}' moved to processed folder")

                    # Append status
                    processed_files.append({'file_name': file_path, 'table_name': table_name, 'status': 'Success', 'error_message': None})
                    load_count += 1
            except Exception as file_processing_error:
                processed_files.append({'file_name': file_path, 'table_name': table_name, 'status': 'Error', 'error_message': str(file_processing_error)})
                error_count += 1
    except Exception as system_error:
        other_errors.append('error_message: ' + str(system_error))
    finally :
        # Send email notification
        send_email_notification()

if __name__ == "__main__":
    main()
