from asyncio.windows_events import NULL
import csv
import mysql.connector
from mysql.connector import errorcode

USER = "root"
PASSWORD = "root"
HOST = "127.0.0.1" #localhost
DATABASE = "company"
SOURCES = ["employees.csv", "items.csv", "ledgers.csv"]
TABLES = {}
TABLES['employees'] = (
    "CREATE TABLE `employees` ("
    "  `emp_no` int(4) NOT NULL,"
    "  `first_name` char(20) NOT NULL,"
    "  `last_name` char(20) NOT NULL,"
    "  `gender` char(1) NOT NULL,"
    "  `weight_kg` decimal(5,1) NOT NULL,"
    "  PRIMARY KEY (`emp_no`)"
    ") ENGINE=InnoDB")

TABLES['items'] = (
    "CREATE TABLE `items` ("
    "  `item_id` int(7) NOT NULL,"
    "  `weight_kg` decimal(5,1) NOT NULL,"
    "  `friendly_name` char(24) NOT NULL,"
    "  PRIMARY KEY (`item_id`)"
    ") ENGINE=InnoDB")

TABLES['ledgers'] = (
    "CREATE TABLE `ledgers` ("
    "  `ledger_id` int(6) NOT NULL,"
    "  `emp_no` int(4) NOT NULL,"
    "  `item_id` int(5) NOT NULL,"
    "  `date_start` DATE NOT NULL,"
    "  `date_end` DATE NOT NULL,"
    "  PRIMARY KEY (`ledger_id`),"
    "  KEY `emp_no` (`emp_no`),"
    "  KEY `item_id` (`item_id`),"
    "  CONSTRAINT `FK_emp_no` FOREIGN KEY (`emp_no`) "
    "     REFERENCES `employees` (`emp_no`) ON DELETE CASCADE,"
    "  CONSTRAINT `FK_item_id` FOREIGN KEY (`item_id`) "
    "     REFERENCES `items` (`item_id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

# simple connect wrapper, unfortunately no function overload in python
def connectDB(user, password, host, database=NULL):
    if database == NULL:
        return mysql.connector.connect(
            user=user, password=password,
            host=host
        )
    return mysql.connector.connect(
        user=user, password=password,
        host=host, database=database
    )

# during debugging only
def debug():
    db = connectDB(USER, PASSWORD, HOST)
    db.cursor().execute(f"DROP DATABASE IF EXISTS {DATABASE}")

# connect and return valid connection, create if missing
def facilitateConnection(user, password, host, database):
    try:
        db =  connectDB(user, password, host, database)
    except mysql.connector.Error as e:
        if e.errno == errorcode.ER_BAD_DB_ERROR:
            db = connectDB(user, password, host)
            cursor = db.cursor()
            cursor.execute(f"CREATE DATABASE {database}")
            return connectDB(user, password, host, database), False
    else:
        return db, True

# take care of invalid or bad data fields
def validateDataIn(inp):
    if inp == "NA" or inp == "indefinite":
        return False
    return True

# create and fill table from TABLES and csv files
def populateDatabase(db, *args):
    cursor = db.cursor()
    for arg in args:
        for table, source in arg:
            table_desc = TABLES[table]
            cursor.execute(table_desc)
            
            # read all lines of the csv file
            with open(source, "r") as dict:
                reader = csv.DictReader(dict)
                headers = reader.fieldnames
                to_db = [([(i[j]) if validateDataIn(i[j]) else NULL for j in headers]) for i in reader] # list comprehensions ;-)

            # same as execute, but over a list
            cursor.executemany(f"INSERT INTO {table} ({', '.join(headers)}) VALUES ({', '.join(['%s' for _ in headers])});", to_db)
            db.commit()

# print the main menu
def printMain():
    print("1. List all employees", "2. List all currently borrowed items", 
        "3. See previously borrowed items", 
        "4. See coming reservations", 
        "5. List all items borrowed by a specific employee", 
        "6. Print the average borrow time in days", 
        "7. Quit", "---------", "Please choose one option:", sep="\n")

# only run if this file isn't imported
if __name__ == "__main__":
    debug()
    db, found = facilitateConnection(USER, PASSWORD, HOST, DATABASE)

    if not found:
        print("Not found, populating database")
        populateDatabase(db, tuple(zip(TABLES, SOURCES)))
    else:
        print("DB found")

    #create view
    cursor = db.cursor()
    cursor.execute("CREATE VIEW bundled_view AS SELECT e.emp_no, e.first_name, e.last_name, i.item_id, i.friendly_name, l.date_start, l.date_end FROM employees e JOIN ledgers l ON (e.emp_no = l.emp_no) JOIN items i ON (i.item_id = l.item_id)")
    cursor.close()

    loop = True
    while loop:
        printMain()
        inp = int(input())
        if inp == 1:
            cursor = db.cursor()
            cursor.execute("SELECT emp_no, first_name, last_name FROM employees ORDER BY emp_no")
            field_names = [i[0] for i in cursor.description]
            table_len = 14
            for names in field_names:
                print(names, end=(" "*(table_len-len(str(names)))))
            print("")
            for match in cursor:
                for field in match:
                    print(field, end=(" "*(table_len-len(str(field)))))
                print("")
            cursor.close()
            input("Press ENTER to continue...")
        elif inp == 2:
            cursor = db.cursor()
            query = f"SELECT * FROM bundled_view v WHERE v.date_end >= DATE(CURRENT_DATE()) AND v.date_start <= DATE(CURRENT_DATE()) ORDER BY v.date_end"
            cursor.execute(query)
            field_names = [i[0] for i in cursor.description]
            table_len = 20
            for names in field_names:
                print(names, end=(" "*(table_len-len(str(names)))))
            print("")
            for match in cursor:
                for field in match:
                    print(field, end=(" "*(table_len-len(str(field)))))
                print("")
            cursor.close()
            input("Press ENTER to continue...")
        elif inp == 3:
            cursor = db.cursor()
            query = f"SELECT * FROM bundled_view v WHERE v.date_end < DATE(CURRENT_DATE()) ORDER BY v.date_end"
            cursor.execute(query)
            field_names = [i[0] for i in cursor.description]
            table_len = 20
            for names in field_names:
                print(names, end=(" "*(table_len-len(str(names)))))
            print("")
            for match in cursor:
                for field in match:
                    print(field, end=(" "*(table_len-len(str(field)))))
                print("")
            cursor.close()
            input("Press ENTER to continue...")
        elif inp == 4:
            cursor = db.cursor()
            query = "SELECT * FROM bundled_view v WHERE v.date_start >= DATE(CURRENT_DATE()) ORDER BY v.date_end"
            cursor.execute(query)
            field_names = [i[0] for i in cursor.description]
            table_len = 20
            for names in field_names:
                print(names, end=(" "*(table_len-len(str(names)))))
            print("")
            for match in cursor:
                for field in match:
                    print(field, end=(" "*(table_len-len(str(field)))))
                print("")
            cursor.close()
            input("Press ENTER to continue...")
        elif inp == 5:
            cursor = db.cursor()
            key = input("Enter the employee number (1 to list all employees with numbers): ")
            query = f"SELECT i.item_id, i.friendly_name, l.date_start, l.date_end FROM employees e JOIN ledgers l ON (e.emp_no = l.emp_no) JOIN items i ON (i.item_id = l.item_id) WHERE e.emp_no = {key}"
            cursor.execute(query)
            field_names = [i[0] for i in cursor.description]
            table_len = 20
            for names in field_names:
                print(names, end=(" "*(table_len-len(str(names)))))
            print("")
            for match in cursor:
                for field in match:
                    print(field, end=(" "*(table_len-len(str(field)))))
                print("")
            cursor.close()
            input("Press ENTER to continue...")
        elif inp == 6:
            cursor = db.cursor()
            query = f"SELECT AVG(DATEDIFF(l.date_end, l.date_start))AS avgtime FROM ledgers l"
            cursor.execute(query)
            for match in cursor:
                for field in match:
                    print(f"The average time an item is borrowed is {float(field)} days")
            cursor.close()
            input("Press ENTER to continue...")
        elif inp == 7:
            loop = False
        else:
            print("invalid input")

    db.close()