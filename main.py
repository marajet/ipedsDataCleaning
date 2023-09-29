import pandas as pd
import os
from warnings import simplefilter

EARLIEST_YEAR = 1984
LATEST_YEAR = 2021


def standard_file(t_data, data_list):
    data = pd.DataFrame()
    t_data.columns = map(str.upper, t_data.columns)
    t_data.columns = map(str.strip, t_data.columns)
    for i in range(len(data_list)):
        d = data_list[i]
        if data_list[i] in t_data:
            data = pd.concat([data, t_data[data_list[i]]], axis=1)
        else:
            data[data_list[i]] = ''
    return data


def read_varlist(varlist_name):
    file = open(varlist_name, "r")
    varlist = file.read().splitlines()
    file.close()
    return varlist


def read_inst_data(year, inst_list):
    filename = ''
    if year >= 2002:  # 2002-2021
        filename = f'hd{year}'
    elif year >= 2000:  # 2000-2001
        filename = f'fa{year}hd'
    elif year == 1999:
        filename = 'ic99_hd'
    elif year == 1998:
        filename = 'ic98hdac'
    elif year == 1997:
        filename = 'ic9798_hdr'
    elif year >= 1995:  # 1995-1996
        year1 = year - 1900
        year2 = year1 + 1
        filename = f'ic{year1}{year2}_a'
    elif year >= 1992 or (1986 <= year <= 1989):  # 1992-1994, 1986-1989
        filename = f'ic{year}_a'
    elif year == 1991:
        filename = 'ic1991_ab'
    elif year == 1990:
        filename = 'ic90hd'
    elif year >= 1984:  # 1984-1985
        filename = f'ic{year}'

    t_inst_data = pd.read_csv(f'data/inst_data/{filename}.csv', encoding='latin1')
    return standard_file(t_inst_data, inst_list)


def read_inst_data_2(year, inst2_list):
    if year == 2021 or (2000 <= year <= 2007):  # 2000-2007, 2021
        filename = f'ic{year}'
    elif year >= 2008:  # 2008-2020
        filename = f'ic{year}_rv'
    else:
        inst_data_2 = pd.DataFrame()
        for i in range(len(inst2_list)):
            inst_data_2[inst2_list[i]] = ''
        return inst_data_2

    t_inst_data_2 = pd.read_csv(f'data/inst_data_2/{filename}.csv')
    return standard_file(t_inst_data_2, inst2_list)


def read_stud_data(year, stud_list):
    filename = ''
    if year == 2021 or (2000 <= year <= 2003):  # 2000-2003, 2021
        filename = f'c{year}_a'
    elif year >= 2004:  # 2004-2020
        filename = f'c{year}_a_rv'
    elif year >= 1995:  # 1995-1999
        year2 = year - 1900
        year1 = year2 - 1
        filename = f'c{year1}{year2}_a'
    elif year >= 1991 or (1984 <= year <= 1989):  # 1984-1989, 1991-1994
        filename = f'c{year}_cip'
    elif year == 1990:
        filename = 'c8990cip'

    t_stud_data = pd.read_csv(f'data/stud_data/{filename}.csv')

    if year <= 2007:  # 2007 and before, rename old data names
        t_stud_data.columns = map(str.upper, t_stud_data.columns)
        t_stud_data.columns = map(str.strip, t_stud_data.columns)
        old_stud_list = read_varlist("data/old_stud_varlist.txt")

        i = 0
        while i < len(old_stud_list):
            if old_stud_list[i] in t_stud_data:
                t_stud_data.rename(columns={old_stud_list[i]: old_stud_list[i+1]}, inplace=True)
            i = i + 2

    return standard_file(t_stud_data, stud_list)


def merge(inst_data, inst_data_2, stud_data):
    data = inst_data.join(inst_data_2.set_index('UNITID'), on='UNITID', how='outer')
    return data.join(stud_data.set_index('UNITID'), on='UNITID', how='right')


def write(year, data):
    if os.path.exists(f'data/output/{year}output.csv'):
        os.remove(f'data/output/{year}output.csv')
    data.to_csv(f'data/output/{year}output.csv', index=False, encoding='latin1')


def main():
    simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
    inst_list = read_varlist("data/inst_varlist.txt")
    inst2_list = read_varlist("data/inst_2_varlist.txt")
    stud_list = read_varlist("data/stud_varlist.txt")

    year = LATEST_YEAR
    while year >= EARLIEST_YEAR:
        inst_data = read_inst_data(year, inst_list)
        inst_data_2 = read_inst_data_2(year, inst2_list)
        stud_data = read_stud_data(year, stud_list)
        data = merge(inst_data, inst_data_2, stud_data)
        write(year, data)
        print(f'{year} done.')
        year -= 1


if __name__ == '__main__':
    main()

