import os
import glob
import sqlite3 as sql
import openpyxl
import pandas as pd
import numpy as np

from Hktvmall_coding import *
#function for classify the item
def one_category_classify(file, month, function_name, outputname):
    os.chdir("C:/Users/hhng/Desktop/New folder/split item")
    outdir = 'C:/Users/hhng/Desktop/New folder/classification'
    month = str(month)
    filename = file + "20210" + month + ".csv"
    df = pd.read_csv(filename)
    df['item'] = df['productname'].apply(function_name)
    df["Rank"] = df.groupby("item")["salesnumber"].rank(method='dense', ascending=False)
    outfilename = outputname + "20210" + month + ".csv"
    path = os.path.join(outdir, outfilename)
    df.to_csv(path, encoding='utf-8-sig')
    return print(f"{outfilename} output done.")

def two_category_classify(file1, file2, month, function_name, outputname):
    os.chdir("C:/Users/hhng/Desktop/New folder/split item")
    outdir = 'C:/Users/hhng/Desktop/New folder/classification'
    month = str(month)
    filename1 = file1 + "20210" + month + ".csv"
    filename2 = file2 + "20210" + month + ".csv"
    df = pd.read_csv(filename1)
    df2 = pd.read_csv(filename2)
    df = df.append(df2)
    df['item'] = df['productname'].apply(function_name)
    df["Rank"] = df.groupby("item")["salesnumber"].rank(method='dense', ascending=False)
    outfilename = outputname + "20210" + month + ".csv"
    path = os.path.join(outdir, outfilename)
    df.to_csv(path, encoding='utf-8-sig')
    return print(f"{outfilename} output done.")

def tri_classify(file1, file2, file3, month, function_name, outputname):
    os.chdir("C:/Users/hhng/Desktop/New folder/split item")
    outdir = 'C:/Users/hhng/Desktop/New folder/classification'
    month = str(month)
    filename1 = file1 + "20210" + month + ".csv"
    filename2 = file2 + "20210" + month + ".csv"
    filename3 = file3 + "20210" + month + ".csv"
    df = pd.read_csv(filename1)
    df1 = pd.read_csv(filename2)
    df2 = pd.read_csv(filename3)
    df = df.append(df1)
    df = df.append(df2)
    df['item'] = df['productname'].apply(function_name)
    df["Rank"] = df.groupby("item")["salesnumber"].rank(method='dense', ascending=False)
    outfilename = outputname + "20210" + month + ".csv"
    path = os.path.join(outdir, outfilename)
    df.to_csv(path, encoding='utf-8-sig')
    return print(f"{outfilename} output done.")

def classification_item(category1, category2, function, filename):
    if category2 != '':
        two_category_classify(category1, category2, 9, function, filename)
        two_category_classify(category1, category2, 8, function, filename)
        two_category_classify(category1, category2, 7, function, filename)
    else:
        one_category_classify(category1, 9, function, filename)
        one_category_classify(category1, 8, function, filename)
        one_category_classify(category1, 7, function, filename)

#function for building database
def create_database_by_one_category(filename,CPICode1):
    os.chdir("C:/Users/hhng/Desktop/New folder/classification")
    outdir = 'C:/Users/hhng/Desktop/New folder/db'
    outfilename = filename + ".db"
    df = pd.read_excel("full list.xlsx", index_col = 0, engine='openpyxl')
    df7 = pd.read_csv(filename + "202107.csv", index_col = 0)
    df8 = pd.read_csv(filename + "202108.csv", index_col = 0)
    df9 = pd.read_csv(filename + "202109.csv", index_col = 0)
    df = df.loc[(df['CPICode'] == CPICode1)]
    path = os.path.join(outdir, outfilename)
    conn = sql.connect(path)
    cur = conn.cursor()
    df.to_sql('list', conn, if_exists='replace', index=True)
    df7.to_sql('item2107', conn, if_exists='replace', index=True)
    df8.to_sql('item2108', conn, if_exists='replace', index=True)
    df9.to_sql('item2109', conn, if_exists='replace', index=True)
    conn.commit()
    conn.close()
    return print(f"{outfilename} output done.")

def create_database_by_two_category(filename,CPICode1,CPICode2):
    os.chdir("C:/Users/hhng/Desktop/New folder/classification")
    outdir = 'C:/Users/hhng/Desktop/New folder/db'
    outfilename = filename + ".db"
    df = pd.read_excel("full list.xlsx", index_col = 0, engine='openpyxl')
    df7 = pd.read_csv(filename + "202107.csv", index_col = 0)
    df8 = pd.read_csv(filename + "202108.csv", index_col = 0)
    df9 = pd.read_csv(filename + "202109.csv", index_col = 0)
    df = df.loc[(df['CPICode'] == CPICode1) | (df['CPICode'] == CPICode2)]
    path = os.path.join(outdir, outfilename)
    conn = sql.connect(path)
    cur = conn.cursor()
    df.to_sql('list', conn, if_exists='replace', index=True)
    df7.to_sql('item2107', conn, if_exists='replace', index=True)
    df8.to_sql('item2108', conn, if_exists='replace', index=True)
    df9.to_sql('item2109', conn, if_exists='replace', index=True)
    conn.commit()
    conn.close()
    return print(f"{outfilename} output done.")

def build_database(filename, code1, code2):
    if code2 != "":
        create_database_by_two_category(filename, code1, code2)
    else:
        create_database_by_one_category(filename, code1)

#function for output the result in .xlsx file
def output_database_result(filename, item):
    outdir = 'C:/Users/hhng/Desktop/New folder/result'
    os.chdir("C:/Users/hhng/Desktop/New folder/db")
    conn = sql.connect(filename + '.db')
    db = conn.cursor()
    query = """select item2109.item, item2109.productid, item2109.productname, item2109.salesnumber as '09sales', item2109.rank as '09rank', list.PopularityRank, item2107.salesnumber as '08sales', item2107.rank as '08rank' list.PopularityRank, item2108.salesnumber as '07sales', item2108.rank as '07rank', list.PopularityRank, list.PopularityRank, list.CPICode 
                from item2109
                left join item2108 on item2108.productid = item2109.productid
                left join item2107 on item2107.productid = item2109.productid
                left join list on list.productid = item2109.productid
                where item2109.item = (?)
                group by item2109.productid
                order by item2109.rank is null, item2109.rank asc, item2108.rank is null, item2108.rank asc, item2107.rank is null, item2107.rank asc, list.PopularityRank is null, list.PopularityRank asc"""

    result = db.execute(query, [item]).fetchall()
    list = [i[0] for i in db.description]
    df = pd.DataFrame.from_records(result, columns = list)
    conn.close()
    return df

def output_excel_file(filename, number):
    os.chdir("C:/Users/hhng/Desktop/New folder/classification")
    df = pd.read_csv(filename + "202109.csv", index_col=0)
    item = df['item'].dropna().unique().astype(int)
    items = []
    for i in item:
        items.append(i)
        items.sort()

    os.chdir("C:/Users/hhng/Desktop/New folder/db")
    conn = sql.connect(filename + '.db')
    cur = conn.cursor()
    cur.execute('''select list.ReferenceMonth, list.BatchNo, list."Set", list.CPICode, list.OutletID, list.ProductID, list.ProductName, list.ProductDesc, list.PopularityRank, list.status, item2109.item
                   from list
                   left join item2109 on item2109.productid = list.ProductID
                   group by list.ProductID
                   order by list.CPIcode asc, list.PopularityRank''')
    list = cur.fetchall()
    df_list = pd.DataFrame.from_records(list)
    list_col = [i[0] for i in cur.description]

    outdir = "C:/Users/hhng/Desktop/New folder/result/"
    outfilename = str(number) + "." + filename + "_result.xlsx"
    path = os.path.join(outdir, outfilename)

    writer = pd.ExcelWriter(path, engine='openpyxl', mode="w")
    df_list.to_excel(writer, sheet_name='list', encoding='utf-8-sig', index=False, header=list_col)
    writer.save()

    df_list.to_excel(writer, sheet_name='list', encoding='utf-8-sig', index=False, header=list_col)
    for i in items:
        df = output_database_result(filename, str(i))
        with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=str(i), encoding='utf-8-sig', index=False)
        writer.save()
    writer.close()

    wb = openpyxl.load_workbook(path)
    for sheet in wb.worksheets:
        sheet.column_dimensions['A'].width = 10
        sheet.column_dimensions['B'].width = 40
        sheet.column_dimensions['C'].width = 70
        sheet.column_dimensions['D'].width = 13
        sheet.column_dimensions['E'].width = 13
        sheet.column_dimensions['F'].width = 13
        sheet.column_dimensions['G'].width = 15
        sheet.column_dimensions['H'].width = 15
        sheet.column_dimensions['I'].width = 15
        sheet.column_dimensions['J'].width = 15
        sheet.column_dimensions['K'].width = 15
        sheet.freeze_panes = 'A2'
        sheet.auto_filter.ref = sheet.dimensions

    wb.worksheets[0].column_dimensions['A'].width = 20
    wb.worksheets[0].column_dimensions['B'].width = 10
    wb.worksheets[0].column_dimensions['C'].width = 10
    wb.worksheets[0].column_dimensions['D'].width = 15
    wb.worksheets[0].column_dimensions['E'].width = 9
    wb.worksheets[0].column_dimensions['F'].width = 40
    wb.worksheets[0].column_dimensions['G'].width = 70
    wb.save(path)
    return print(f"{outfilename} output done."
                 "\n==========================="
                 "\n")

#result function
def result(category1,category2, function, filename, code1, code2, number):
    classification_item(category1, category2, function, filename)
    build_database(filename, code1, code2)
    output_excel_file(filename, number)





