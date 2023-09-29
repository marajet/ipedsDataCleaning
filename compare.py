import pandas as pd


def main():
    data1 = pd.read_excel('data/inst_dict/hd2004.xlsx', sheet_name='varlist')
    data2 = pd.read_excel('data/inst_dict/hd2005.xlsx', sheet_name='varlist')

    data1 = data1[['varnumber', 'varname']]
    data2 = data2[['varnumber', 'varname']]

    print(data1.equals(data2))
    data3 = data1.compare(data2)
    print(data3.to_string())


if __name__ == "__main__":
    main()