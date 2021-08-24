import configparser
import pandas
import logging as log
import requests

log.basicConfig(level=log.INFO)

parser = configparser.ConfigParser()
parser.read("file_info.config")


excel_file_input = parser.get('FILE_CONFIG',"input_file")
excel_file_output = parser.get('FILE_CONFIG',"output_file")
api = parser.get('API',"url")
output = None


def fetch_covid_data(date,iso):
    final_dict = dict()
    num_confirmed = 0
    num_deaths = 0
    num_recovered = 0

    params = {'date':date,"iso":iso}
    req = requests.get('https://covid-api.com/api/reports',params=params).json()
    data = req.get('data',None)

    if data:
        total_len = len(req.get('data'))
        for i in range(total_len):
            num_confirmed = data[i].get('confirmed') + num_confirmed
            num_deaths = data[i].get('deaths') + num_deaths
            num_recovered = data[i].get('recovered') + num_recovered
        final_dict['date'] = date
        final_dict['iso'] = iso
        final_dict['num_confirmed'] = num_confirmed
        final_dict['num_deaths'] = num_deaths
        final_dict['num_recovered'] = num_recovered

    return final_dict

try:
    df = pandas.read_csv(excel_file_input)
    output = pandas.DataFrame()

    for index, row in df.iterrows():
        final_dict = fetch_covid_data(row['date'], row['iso'])
        if final_dict:
            output = output.append(final_dict, ignore_index=True)
            log.info('Data Extracted and stored for %s.', row['iso'])
        else:
            log.warning('Data could not be extracted for %s.', row['iso'])


except Exception as FileNotFoundError:
    log.error("Input file is not placed in the given path.")

if output is not None:
    try:
        output.to_csv(excel_file_output, index=False)
        log.info('DataFrame is written successfully to Excel Sheet.')
    except Exception as FileNotFoundError:
        log.error("Output file is not placed in the given path.")




