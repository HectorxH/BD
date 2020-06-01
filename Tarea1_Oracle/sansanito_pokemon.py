import logic
from cmd import Cmd

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
        logic.create_poyo()
        logic.create_sansanito()
        logic.create_views()
        logic.create_triggers()

    def do_drop(self, inp):
        """
        Elimina todas las tablas y views.
        """
        logic.drop_tables()
        logic.drop_views()

    def do_reset(self, inp):
        """
        Vuelve a crear todas las tablas (equivalente a llamar a 'drop' y luego a 'init')
        """
        self.do_drop('')
        self.do_init('')

    def do_view(self, inp):
        """
        Recibe dos parametros de la forma view [vista] en donde vista puede ser uno de los siguiente valores:
            -<default>: Muestra Nombre, Hp actual, HP Max y prioridad de los pokemosn 
            en SANSANITO ordenados por prioridad de forma descendiente.
            -poyo: Muestra los datos de la tabla POYO.
            -sansanito: Muestra los datos de la tabla SANSANITO.
            -mas_prioridad: Muestra los 10 pokemons con mas prioridad en SANSANITO.
            -menos_prioridad: Muestra los 10 pokemons con menos prioridad en SANSANITO.
            -legendarios: Muestra todos los pokemons legendarios en SANSANITO
            -<estado>: Muestra todos los pokemons en sansanito con el estado <estado>.

        <estado> puede ser uno de los siguientes (no es case sensitive):
            ['ENVENENADO', 'PARALIZADO', 'QUEMADO', 'DORMIDO', 'CONGELADO', 'NONE']
        """
        if inp == "":
            logic.print_table('standar')
        elif inp in logic.legal_views:
            logic.print_table(inp)
        elif inp.upper() in logic.debuffs:
            logic.print_debuff(inp.upper())
        else:
            print("Vista invalida.")


    def do_populate(self, inp):
        """
        Ingresa 25 nuevos pokemons de forma aleatoria al sansanito, se puede
        especificar la cantidad como populet [size].
        """
        if inp:
            try:
                logic.populate_random(int(inp))
            except ValueError:
                print("Ingrese un valor valido.")
        else:
            logic.populate_random()
    

    def help_exit(self):
        print(self.do_exit.__doc__)
    def help_help(self):
        print("Lista de comandos disponibles al escribir 'help' o da descripcion detallada al escribir 'help cmd'.")



    do_EOF = do_exit
    help_EOF = help_exit

    ruler = "=="
    doc_header = "Lista de comandos escribe help <comando>:"

PSQL().cmdloop()
