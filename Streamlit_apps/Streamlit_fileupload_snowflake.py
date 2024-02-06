import streamlit as st
import pandas as pd
import json

def validate_data(df):
    valid = True
    allowed_state_codes = ['NY','NJ','RI','MA','PA','VT']
    null_cols = df[df.text_column.isna()] 
    invalid_dates = df[pd.to_datetime(df['Date_column'],format='mixed',errors='coerce').isna()]
    df1 = df.dropna(subset=['number_column'])
    invalid_numbers  = df1[pd.to_numeric(df1['number_column'],errors='coerce').isna()]
    invalid_values = df[~df.state.isin(allowed_state_codes)]   
    if not null_cols.empty:
        st.write('rows with text_column as empty')
        null_cols
        valid = False
    if not invalid_dates.empty:
        st.write('rows with Invalid date types')
        #fmt = ["%m/%d/%y","%y-%m-%d"]
        invalid_dates
        valid = False
    if not invalid_numbers.empty:
        st.write('rows with invalid numbers')
        invalid_numbers
        valid = False    
    if not invalid_values.empty:
        st.write('rows with invalid State code')
        invalid_values        
        valid = False
    return valid

def get_data(data_file):    
    file_details = {"filename": data_file.name, "filetype": data_file.type, "filesize": data_file.size}
    #st.write(file_details)
    df1 = pd.DataFrame()
    if file_details['filetype'] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        df1 = pd.read_excel(data_file, sheet_name=0, header=0)
    elif file_details['filetype'] == "text/csv":
            # provide delimiter options    
            st.session_state['delim_list'] = [",","|"]
            choice = st.radio("Delimiter",options=st.session_state['delim_list'],index=None,horizontal=True)
            if choice :
                df1 = pd.read_csv(data_file,header='infer',delimiter=choice)
    else: st.write('Invalid File type')
    return df1

def main():
    data_file = st.file_uploader("Upload File", type=["csv","xlsx"])
    #df = pd.DataFrame()
    if data_file is not None:
        df = get_data(data_file)
        if not df.empty:
            st.dataframe(df)
            #df.dtypes
            valid = validate_data(df)
            if valid:
                st.write('load data')
            else: st.warning('Correct errors and re upload')

if __name__ == '__main__':
    main()
