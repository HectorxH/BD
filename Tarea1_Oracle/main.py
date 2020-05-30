import pyodbc
import pandas as pd

connect_string = """DRIVER={Oracle in OraDB12Home1};
DBQ=testing;
Uid=admin;
Pwd=pass"""

cnxn = pyodbc.connect(connect_string)

def create_poyo():
    df = pd.read_csv('pokemon.csv', usecols=["#","Name","Type 1", "Type 2", "HP","Legendary"])

    sql = '''--sql
    CREATE TABLE POYO
    (
        pokedex int,
        poke_name varchar2(50),
        type1 varchar2(50),
        type2 varchar2(50),
        hp_total int,
        legendary number(1) not null check (legendary in (1, 0))
    );
    '''

    with cnxn.cursor() as cursor:
        try:
            cursor.execute(sql)
        except:
            print("POYO is already a table.")
            return

        insert = '''
        --sql
        INSERT INTO POYO (pokedex, poke_name, type1, type2, hp_total, legendary)
            VALUES (?, ?, ?, ?, ?, ?);
        '''

        for _, row in df.iterrows():
            cursor.execute(insert, *row)




def drop_all():
    cursor = cnxn.cursor()
    drop_cursor = cnxn.cursor()
    cursor.execute('SELECT table_name FROM user_tables;')

    for row in cursor:
        sql = "DROP TABLE {};".format(row.TABLE_NAME)

        print(sql)
        drop_cursor.execute(sql)
        print("Table sucesfully deleated.")

    cnxn.commit()


def print_full_table(table_name, size=25):
    table_name = table_name.upper()
    with cnxn.cursor() as cursor:
        cursor.execute("SELECT column_name FROM USER_TAB_COLUMNS WHERE table_name = ?;", table_name)
        cols = [col[0] for col in cursor]

        cursor.execute("SELECT * FROM {};".format(table_name))
        rows = cursor.fetchmany(size)
        while rows:
            rows = [cols]+rows
            max_lens = [0] * len(cols)
            for row in rows:
                for i, value in enumerate(row):
                    max_lens[i] = max(max_lens[i], len(str(value)))
            
            row_format = ''.join("{:<"+str(max_lens[i]+2)+"}" for i in range(len(cols)))

            print('', *[row_format.format(*row) for row in rows] , '', sep='\n')

            rows = cursor.fetchmany(size)