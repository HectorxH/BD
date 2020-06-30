from cmd import Cmd
import pyodbc
import pandas as pd
from random import randint, choice
from math import ceil
import datetime
import numbers
import decimal

connect_string = """DRIVER={Oracle in OraDB12Home1};
DBQ=testing;
Uid=admin;
Pwd=pass"""

cnxn = pyodbc.connect(connect_string)
cnxn.add_output_converter(pyodbc.SQL_TYPE_TIMESTAMP,
                        lambda x: str(x.decode("utf-8")))


########################################
#                CRUD                  #
########################################

def create(poke_name, hp_curr, debuff):
    """Adds a new pokemon to the table sansanito.

    Args:
        poke_name (string): The name of the pokemon.
        hp_curr (int): Current hp of the pokemon
        debuff (str): Debuff of the pokemon.
    """

    insert = '''--sql
    INSERT INTO sansanito (poke_name, type1, type2, hp_curr, hp_max, debuff, legendary)
        VALUES (?, ?, ?, ?, ?, ?, ?);
    '''
    check_capacity = '''--sql
    SELECT (
        (SELECT COUNT(*) FROM SANSANITO)
        +(SELECT 4*COUNT(*) FROM SANSANITO WHERE legendary = 1))
    FROM DUAL;
    '''
    drop = '''--sql
    DELETE FROM SANSANITO WHERE id = 
        (SELECT id FROM SANSANITO
        WHERE legendary = ?
        ORDER BY preference ASC
        FETCH first 1 row only);
    '''
    with cnxn.cursor() as cursor:
        cursor.execute("SELECT * FROM POYO WHERE poke_name = ?", poke_name)
        try:
            _, poke_name, type1, type2, hp_max, legendary = cursor.fetchone()
        except:
            print(poke_name + " no es reconocido como un nombre de pokemon.")
            return

        if hp_curr < 0 or hp_curr > hp_max:
            print("Cantidad de HP invalida.")
            return

        try:
            cursor.execute(insert, poke_name, type1,type2, hp_curr, hp_max, debuff, legendary)
        except:
            print("No se pudo ingresar el pokemon a sansanito.")
            return
        
        cap = cursor.execute(check_capacity).fetchone()[0]
        if cap > 50:
            try:
                cursor.execute(drop, legendary)
            except:
                cursor.rollback()
                print("No pudo ingresar el pokemon a sansanito.")


def read(id):
    """Prints a single pokemon from sansanito.

    Args:
        id (int): ID of the pokemon.
    """
    with cnxn.cursor() as cursor:
        try:
            row = cursor.execute("SELECT * FROM SANSANITO WHERE id = ?", id)
            print(row)
        except:
            print("No se encontro la entrada id="+id)


def update(id, hp=None, debuff=None, check_in_time=None):
    """Updates a row from the table sansanito.  

    Args:
        id (int): ID from the row to update.
        hp (int, optional): New hp value for the pokemon. Defaults to None.
        debuff (string, optional): New debuff for the pokemon. Defaults to None.
        check_in_time (string, optional): New check in time for the pokemon. Defaults to None.
    """
    update = '''--sql
    UPDATE SANSANITO
    SET
        hp_curr = ?,
        debuff = ?,
        check_in_time = ?
    WHERE
        id = ?;
    '''
    select = '''--sql
    SELECT hp_curr, debuff, check_in_time, id
    FROM SANSANITO
    WHERE id = ?;
    '''

    with cnxn.cursor() as cursor:
        try:
            row = cursor.execute(select, id).fetchone()
        except:
            print("No se encontro la entrada id="+id)
            return

        if hp != None: row.CURR_HP = hp
        if debuff != None: row.DEBUFF = debuff
        if check_in_time != None: row.CHECK_IN_TIME = check_in_time
        try:
            cursor.execute(update, *row)
        except:
            print("Error al actualizar los valores de id="+id)


def delete(id):
    """Delets a row from the table sansanito.  

    Args:
        id (int): ID of the pokemon to delete
    """
    delete = '''--sql
    DELETE FROM SANSANITO WHERE id = ?;
    '''
    with cnxn.cursor() as cursor:
        try:
            cursor.execute(delete, id)
        except:
            print("No se puedo eliminar id="+id)


########################################
#              Logica                  #
########################################

legal_views = ['poyo', 'sansanito', 'legendarios', 'mas_prioridad', 'menos_prioridad', 'mas_tiempo', 'mas_repeticiones', 'standar']
debuffs = ['ENVENENADO', 'PARALIZADO', 'QUEMADO', 'DORMIDO', 'CONGELADO', 'NONE']

connect_string = """DRIVER={Oracle in OraDB12Home1};
DBQ=testing;
Uid=admin;
Pwd=pass"""

cnxn = pyodbc.connect(connect_string)
cnxn.add_output_converter(pyodbc.SQL_TYPE_TIMESTAMP,
                        lambda x: str(x.decode("utf-8")))

def create_triggers():
    triggers = [
        '''--sql--sql--sql--sql 
        CREATE TRIGGER legen AFTER INSERT OR UPDATE ON sansanito
            DECLARE
                CURSOR c1 IS SELECT id, poke_name, check_in_time FROM sansanito WHERE legendary = 1;
            BEGIN
                FOR row IN c1 LOOP
                    DELETE FROM sansanito
                    WHERE check_in_time NOT IN (
                        SELECT MIN(check_in_time)
                        FROM sansanito
                        WHERE poke_name = row.poke_name
                    ) AND poke_name = row.poke_name;
                END LOOP;
            END;''',
    ]

    with cnxn.cursor() as cursor:
        for trigger in triggers:
            try:
                cursor.execute(trigger)
            except:
                print("Unable to create trigger.")

def create_views():
    views = [
        '''--sql
        CREATE VIEW legendarios AS
            SELECT * FROM sansanito WHERE legendary = 1;''',
        '''--sql
        CREATE VIEW mas_prioridad AS
            SELECT * FROM
            (SELECT * FROM sansanito ORDER BY preference DESC)
            WHERE ROWNUM <= 10;''',
        '''--sql
        CREATE VIEW menos_prioridad AS
            SELECT * 
            FROM (SELECT * FROM sansanito ORDER BY preference)
            WHERE ROWNUM <= 10;''',
        '''--sql
        CREATE VIEW mas_tiempo AS
            SELECT id, poke_name, check_in_time
            FROM sansanito 
            ORDER BY check_in_time 
            DESC FETCH first 1 rows only;''',
        '''--sql
        CREATE VIEW mas_repeticiones AS
            SELECT poke_name, COUNT(poke_name) AS cant 
            FROM sansanito 
            GROUP BY poke_name
            ORDER BY cant DESC 
            FETCH first 1 row only;
        ''',
        '''--sql
        CREATE VIEW standar AS
            SELECT poke_name, hp_curr, hp_max, preference
            FROM sansanito ORDER BY preference DESC;'''
    ]

    with cnxn.cursor() as cursor:
        for view in views:
            try:
                cursor.execute(view)
            except:
                print("Unable to create view.")

def drop_views():
    with cnxn.cursor() as cursor:
        for view in legal_views[2:]:
            try:
                cursor.execute("DROP VIEW {};".format(view))
            except:
                print("Unable to drop view {}.".format(view))

def create_poyo():
    """Creates the table POYO from the data provided in the file pokemon.csv
    """
    df = pd.read_csv('pokemon.csv', usecols=["#","Name","Type 1", "Type 2", "HP","Legendary"], encoding='UTF-8')
    sql = '''--sql
    CREATE TABLE POYO
    (
        pokedex number,
        poke_name nvarchar2(50) NOT NULL PRIMARY KEY,
        type1 varchar2(50),
        type2 varchar2(50),
        hp_max number,
        legendary number(1) NOT NULL CHECK (legendary in (1, 0))
    );
    '''

    with cnxn.cursor() as cursor:
        try:
            cursor.execute(sql)
            print("POYO successfully created.")
        except:
            print("POYO is already a table.")
            return

        insert = '''--sql
        INSERT INTO POYO (pokedex, poke_name, type1, type2, hp_max, legendary)
            VALUES (?, ?, ?, ?, ?, ?);
        '''

        for _, row in df.iterrows():
            cursor.execute(insert, *row)


def create_sansanito():
    """Creates the table SANSANITO.
    """
    sql = '''--sql
    CREATE TABLE SANSANITO (
        id number GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        poke_name nvarchar2(50) REFERENCES poyo (poke_name),
        type1 varchar2(50),
        type2 varchar2(50),
        hp_curr number,
        hp_max number,
        debuff varchar2(50) DEFAULT 'NONE' CHECK (debuff in ('ENVENENADO', 'PARALIZADO', 'QUEMADO', 'DORMIDO', 'CONGELADO', 'NONE')),
        legendary number(1) NOT NULL CHECK (legendary in (1, 0)),
        check_in_time timestamp(2) default LOCALTIMESTAMP,
        preference number GENERATED ALWAYS AS 
            (hp_max-hp_curr
            +(CASE WHEN debuff = 'NONE' THEN 0 ELSE 10 END)) VIRTUAL
    );
    '''

    with cnxn.cursor() as cursor:
        try:
            cursor.execute(sql)
            print("SANSANITO successfully created.")
        except:
            print("SANSANITO is already a table.")
            return


def populate_random(n=25):
    """Add n random rows to the table SANSANITO that are consistent with the data from table POYO.

    Args:
        n (int, optional): Number of new rows. Defaults to 25.
    """
    pokemons = '''--sql
    SELECT poke_name, hp_max FROM poyo;
    '''

    with cnxn.cursor() as cursor:
        rows = cursor.execute(pokemons).fetchall()
        for _ in range(n):
            poke_name, hp_max = choice(rows)

            hp = randint(0, hp_max)
            debuff = choice(debuffs)
            create(poke_name, hp, debuff)
            # try:
            #     create(poke_name, hp, debuff)
            # except:
            #     print("Error al añadir ({},{}/{},{})".format(poke_name, hp, hp_max, debuff))


def drop_tables():
    """Drops all the tables.
    """
    cursor = cnxn.cursor()
    drop_cursor = cnxn.cursor()
    cursor.execute('SELECT table_name FROM user_tables;')

    for row in cursor:
        sql = "DROP TABLE {} CASCADE CONSTRAINTS;".format(row.TABLE_NAME)

        drop_cursor.execute(sql)
        print("Table {} successfully deleted.".format(row.TABLE_NAME))

    cnxn.commit()

def print_query(query, size=25):
    """Prints the results of a given query in batches of size elements.

    Args:
        query (str): Query to execute.
        size (int, optional): Size of the batches. Defaults to 25.
    """
    with cnxn.cursor() as cursor:
        cursor.execute(query)
        cols = [col[0] for col in cursor.description]
        rows = cursor.fetchmany(size)

        while rows:
            rows = list(map(lambda x: list(map(lambda a: int(a) if isinstance(a, numbers.Number) else a, x)), rows))
            rows = [cols]+rows
            max_lens = [0] * len(cols)
            for row in rows:
                for i, value in enumerate(row):
                    max_lens[i] = max(max_lens[i], len(str(value)))
            row_format = ''.join("{:<"+str(max_lens[i]+2)+"}" for i in range(len(cols)))

            print('', *[row_format.format(*row) for row in rows] , '', sep='\n')

            rows = cursor.fetchmany(size)

def print_table(table_name, size=25):
    """Prints all rows and all columns from a given table or view in batches of size elements.

    Args:
        table_name (str): Target table or view.
        size (int, optional): Size of the batches. Defaults to 25.
    """
    print_query("SELECT * FROM {};".format(table_name), size)

def print_debuff(debuff):
    """Prints all rows from SANSANITO with a given debuff.

    Args:
        debuff (str): The debuff.
    """
    print_query("SELECT * FROM sansanito WHERE debuff = '{}';".format(debuff.upper()))


########################################
#                 CMD                  #
########################################


class PSQL(Cmd):
    prompt = 'PSQL>'
    intro = """
     _____                             _ _        
    /  ___|                           (_) |       
    \ `--.  __ _ _ __  ___  __ _ _ __  _| |_ ___  
     `--. \/ _` | '_ \/ __|/ _` | '_ \| | __/ _ \ 
    /\__/ / (_| | | | \__ \ (_| | | | | | || (_) |
    \____/ \__,_|_| |_|___/\__,_|_| |_|_|\__\___/ 
    _.----.        ____         ,'  _\   ___    ___     ____
_,-'       `.     |    |  /`.   \,-'    |   \  /   |   |    \  |`.
\      __    \    '-.  | /   `.  ___    |    \/    |   '-.   \ |  |
 \.    \ \   |  __  |  |/    ,','_  `.  |          | __  |    \|  |
   \    \/   /,' _`.|      ,' / / / /   |          ,' _`.|     |  |
    \     ,-'/  /   \    ,'   | \/ / ,`.|         /  /   \  |     |
     \    \ |   \_/  |   `-.  \    `'  /|  |    ||   \_/  | |\    |
      \    \ \      /       `-.`.___,-' |  |\  /| \      /  | |   |
       \    \ `.__,'|  |`-._    `|      |__| \/ |  `.__,'|  | |   |
        \_.-'       |__|    `-._ |              '-.|     '-.| |   |
                                `'    
==================================================================================
Bienvenido a la interfaz de Poke-Sansanito Query Language (PSQL)
Escribe ? para obtener informacion sobre los comandos.
=================================================================================="""

    def do_exit(self, inp):
        """
        Finaliza la aplicacion
        """
        print("Cerrando PSQL")
        return True

    def help_exit(self):
        print(self.do_exit.__doc__)
    
    def do_init(self, inp):
        """
        Realiza las siguiente operaciones:
            -Crea la tabla POYO cargando los datos del CSV.
            -Crea la tabla SANSANITO.
        """
        create_poyo()
        create_sansanito()
        create_views()
        create_triggers()

    def do_drop(self, inp):
        """
        Elimina todas las tablas y views.
        """
        drop_tables()
        drop_views()

    def do_reset(self, inp):
        """
        Vuelve a crear todas las tablas (equivalente a llamar a 'drop' y luego a 'init')
        """
        self.do_drop('')
        self.do_init('')
    
    def do_nuevo(self, inp):
        """
        Añade un nuevo pokemon a la tabla SANSANITO de la forma: nuevo <nombre_pokemon> <hp_actual> <estado>

        <estado> puede ser uno de los siguientes (no es case sensitive):
            ['ENVENENADO', 'PARALIZADO', 'QUEMADO', 'DORMIDO', 'CONGELADO', 'NONE']
        """
        inp = inp.strip().split(" ")
        try:
            inp[1] = int(inp[1])
        except ValueError:
            print("id debe ser un numero entero.")
        if len(inp) == 3:
            create(*inp)
        else:
            print("Cantidad de parametros ingresados invalida.")

    def do_buscar(self, inp):
        """
        Busca un pokemon en SANSANITO en base a un id de la forma: buscar <id>
        """
        try:
            read(int(inp))
        except ValueError:
            print("id debe ser un numero entero.")

    
    def do_actualizar(self, inp):
        """
        Actualiza los valores de una entrada en la tabal SANSANITO de la forma:
            actualizar <id> <nueva_hp> <nuevo_estado>
        <estado> puede ser uno de los siguientes (no es case sensitive):
            ['ENVENENADO', 'PARALIZADO', 'QUEMADO', 'DORMIDO', 'CONGELADO', 'NONE']
        """
        inp = inp.strip().split(" ")
        try:
            inp[1] = int(inp[1])
        except ValueError:
            print("id debe ser un numero entero.")
        if len(inp) == 3:
            update(*inp)
        else:
            print("Cantidad de parametros ingresados invalida.")

    def do_eliminar(self, inp):
        """
        Elimina una entrada de la tabla sansanito de la forma:
            eliminar <id>
        """
        try:
            delete(int(inp))
        except ValueError:
            print("id debe ser un numero entero.")



    def do_view(self, inp):
        """
        Recibe dos parametros de la forma view [vista] en donde vista puede ser uno de los siguiente valores:
            -<default>: Muestra Nombre, Hp actual, HP Max y prioridad de los pokemosn 
            en SANSANITO ordenados por prioridad de forma descendiente.
            -poyo: Muestra los datos de la tabla POYO.
            -sansanito: Muestra los datos de la tabla SANSANITO.
            -mas_prioridad: Muestra los 10 pokemons con mas prioridad en SANSANITO.
            -menos_prioridad: Muestra los 10 pokemons con menos prioridad en SANSANITO.
            -mas_tiempo: Muestra el pokemon que lleva mas tiempo ingresado en SANSANITO.
            -mas_repeticiones: Muestra el tipo de pokemon mas repetido y la cantidad de repeticiones
                               en Sansanito.
            -legendarios: Muestra todos los pokemons legendarios en SANSANITO
            -<estado>: Muestra todos los pokemons en sansanito con el estado <estado>.

        <estado> puede ser uno de los siguientes (no es case sensitive):
            ['ENVENENADO', 'PARALIZADO', 'QUEMADO', 'DORMIDO', 'CONGELADO', 'NONE']
        """
        if inp == "":
            print_table('standar')
        elif inp.lower() in legal_views:
            print_table(inp)
        elif inp.upper() in debuffs:
            print_debuff(inp.upper())
        else:
            print("Vista invalida.")


    def do_populate(self, inp):
        """
        Ingresa 25 nuevos pokemons de forma aleatoria al sansanito, se puede
        especificar la cantidad como populet [size].
        """
        if inp:
            try:
                populate_random(int(inp))
            except ValueError:
                print("Ingrese un valor valido.")
        else:
            populate_random()
    

    def help_exit(self):
        print(self.do_exit.__doc__)
    def help_help(self):
        print("Lista de comandos disponibles al escribir 'help' o da descripcion detallada al escribir 'help cmd'.")


    do_EOF = do_exit
    help_EOF = help_exit

    ruler = "=="
    doc_header = "Lista de comandos escribe help <comando>:"




#"Main"
PSQL().cmdloop()
