import pymysql
import pandas as pd
import sys,os


csv_file = '107_2_course.csv'
only_insert = 'ON'
make_table = []

def main():
    config = dict(host='vm.rish.com.tw', user='root', password='',
                cursorclass=pymysql.cursors.DictCursor
                )

    conn = pymysql.Connect(**config)
    conn.autocommit(1)
    cursor = conn.cursor()
    df = pd.read_csv(os.getcwd() + '\\course_data\\' + csv_file, encoding='utf-8',
                    usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 ,13 ,14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24])

    
    df['系統序號'] = df['系統序號'].fillna(0).astype('int')
    df['開課年度'] = df['開課年度'].fillna(0).astype('int')
    df['學分數'] = df['學分數'].fillna(0).astype('int')
    # 因為有A年級，因此無法使用int為astype
    # df['年級'] = df['年級'].fillna(0).astype('int')
    df['課程大綱'] = df['課程大綱'].astype('category')
    df['課程連結'] = df['課程連結'].astype('category')
    df['備註'] = df['備註'].astype('category')
    df['總修課人數'] = df['總修課人數'].fillna(0).astype('int')

    return df, conn, cursor 


# 根據pandas判定type來set table的type
def make_table_sql(df):
    columns = df.columns.tolist()
    
    types = df.ftypes
    global make_table
    
    for item in columns:
        if 'int' in types[item]:
            char = item + ' INT'
        elif 'float' in types[item]:
            char = item + ' FLOAT'
        elif 'object' in types[item]:
            char = item + ' VARCHAR(500)'
        elif 'category' in types[item]:
            char = item + ' Text'
        elif 'datetime' in types[item]:
            char = item + ' DATETIME'
        make_table.append(char)
    return ','.join(make_table)


def build_table(db_name, table_name, df, cursor, conn):
    # database
    cursor.execute('CREATE DATABASE IF NOT EXISTS {} CHARSET=utf8mb4'.format(db_name))
    # 選擇db
    conn.select_db(db_name)
    # 建table, 有的話會先drop後重建
    cursor.execute('DROP TABLE IF EXISTS {}'.format(table_name))
    cursor.execute('CREATE TABLE {}({})'.format(table_name,make_table_sql(df)))

  

def insert_Course(db_name, table_name, df, cursor, conn):
    conn.select_db(db_name)
    make_table_sql(df)
    df = df.astype(object).where(pd.notnull(df), None)
    # 數據轉list
    values = df.values.tolist()
    # df['日期'] = df['日期'].astype('str')
    # 根據column數
    s = ','.join(['%s' for _ in range(len(df.columns))])
    cursor.executemany('INSERT INTO {} VALUES ({})'.format(table_name,s), values)



if __name__ == "__main__":
    df, conn, cursor = main()
    if only_insert != 'ON':
        build_table('Course', 'class', df, cursor, conn)
    insert_Course('Course', 'class', df, cursor, conn)
    

