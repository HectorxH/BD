import pyodbc

connect_string = """DRIVER={Oracle in OraDB12Home1};
DBQ=testing;
Uid=admin;
Pwd=pass"""

cnxn = pyodbc.connect(connect_string)
cnxn.add_output_converter(pyodbc.SQL_TYPE_TIMESTAMP,
                        lambda x: str(x.decode("utf-8")))

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

