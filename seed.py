import psycopg2
from configparser import ConfigParser


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f"Section {section} not found in the {filename} file")

    return db


def check_connection():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


player_info = """CREATE TABLE player_info (
                 id VARCHAR(8) PRIMARY KEY,
                 name VARCHAR(255) NOT NULL,
                 dob DATE NOT NULL,
                 nation VARCHAR(255)
                 club VARCHAR(255),
                 main_position VARCHAR(20),
                 other_position VARCHAR(20),
                 current_value DECIMAL(4,1)
                 max_value DECIMAL(4,1),
                 )"""


def keeper(competition, season):
    return f"""CREATE TABLE {competition}{season}KEEPER (
               id VARCHAR(8) PRIMARY KEY,
               GA SMALLINT,
               SoTA SMALLINT,
               Saves SMALLINT,
               W SMALLINT,
               D SMALLINT,
               L SMALLINT,
               CS SMALLINT,
               PKatt SMALLINT,
               PKA SMALLINT,
               PKsv SMALLINT,
               PKm SMALLINT,
               )"""


def keeperadv(competition, season):
    return f"""CREATE TABLE {competition}{season}KEEPERADV (
               id VARCHAR(8) PRIMARY KEY,
               FK SMALLINT,
               CK SMALLINT,
               OG SMALLINT,
               PSxG DECIMAL(5,1),
               PSxGΔ DECIMAL(5,1),
               Cmp SMALLINT,
               Att SMALLINT,
               Att.1 SMALLINT,
               Thr SMALLINT,
               Att.2 SMALLINT,
               Opp SMALLINT,
               Stp SMALLINT,
               OPA SMALLINT
               )"""


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE player_info (
            id VARCHAR(8) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            dob DATE NOT NULL,
            nation VARCHAR(255)
            club VARCHAR(255),
            main_position VARCHAR(20),
            other_position VARCHAR(20),
            current_value DECIMAL(4,1)
            max_value DECIMAL(4,1),
        )
        """,
        """ CREATE TABLE parts (
                part_id SERIAL PRIMARY KEY,
                part_name VARCHAR(255) NOT NULL
                )
        """,
        """
        CREATE TABLE part_drawings (
                part_id INTEGER PRIMARY KEY,
                file_extension VARCHAR(5) NOT NULL,
                drawing_data BYTEA NOT NULL,
                FOREIGN KEY (part_id)
                REFERENCES parts (part_id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE vendor_parts (
                vendor_id INTEGER NOT NULL,
                part_id INTEGER NOT NULL,
                PRIMARY KEY (vendor_id , part_id),
                FOREIGN KEY (vendor_id)
                    REFERENCES vendors (vendor_id)
                    ON UPDATE CASCADE ON DELETE CASCADE,
                FOREIGN KEY (part_id)
                    REFERENCES parts (part_id)
                    ON UPDATE CASCADE ON DELETE CASCADE
        )
        """)
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_tables()

if __name__ == '__main__':
    check_connection()
#
# conn.autocommit = True
# cursor = conn.cursor()
#
# sql = """
# CREATE TABLE DETAILS(id VARCHAR(10) NOT NULL,\
# Player VARCHAR(100),\
# Nation varchar(30),\
# employee_salary float);
# """
#
# def
#
# cursor.execute(sql)
#
# sql2 = """
# COPY details(employee_id,\
# employee_name,\
# employee_email,\
# employee_salary)
# FROM '/private/tmp/details.csv'
# DELIMITER ','
# CSV HEADER;
# """
#
# cursor.execute(sql2)
#
# sql3 = '''select * from details;'''
# cursor.execute(sql3)
# for i in cursor.fetchall():
#     print(i)
#
# conn.commit()
# conn.close()
