import sqlite3
import pandas as pd
'''
    Super Maids Cleaning Company
    Project Part Three
    a. Develop SQL code to create the entire database schema
    b. Create at least 5 tuples for each relation in the database
    c. Develop 5 SQL queries using embedded SQL 
    d. Upload all the code and documentation to GitHub

    SQLITE Stores DATE datatypes as 'YYYY-MM-DD'
    SQLITE Stores TIME datatypes as 'HH:MM:SS'
'''

# Connect to an existing database file in the current directory
# If the file does not exist, it creates it in the new directory
db_connect = sqlite3.connect('Maids.db')

# Instantiate cursor object for exectuing queries
cursor = db_connect.cursor()

'''
    CLIENT TABLE: clientNo (PK), clientfName, clientlName, clientAddress, clientTelNo
    SERVICES TABLE: serviceID (PK), startDate, startTime, duration, comments, clientNo (FK)
    EMPLOYEE TABLE: empNo (PK), empfName, emplName, empAddress, salary, empTelNo
    EQUIPMENT TABLE: equipID (PK), description, usage, cost
'''
#
#   FOR THE TA GRADER: 
#

# FUNCTION DEFINITIONS START HERE
def dropTables(table):
    return ("DROP TABLE IF EXISTS " + table)


def clearDatabase(cursor):
    # Turn off foreign keys before dropping tables/views (CONSTRAINT ERRORS)
    query = "PRAGMA foreign_keys = OFF;"
    cursor.execute(query)
    query = dropTables('Client')
    cursor.execute(query)
    query = dropTables('Services')
    cursor.execute(query)
    query = dropTables('Employee')
    cursor.execute(query)
    query = dropTables('Equipment')
    cursor.execute(query)


def printTable(cursor):
    print()
    print()
    # Extract column names from cursor
    column_names = [row[0] for row in cursor.description]
    # Fetch data and load into pandas dataframe
    table_data = cursor.fetchall()
    df = pd.DataFrame(table_data, columns=column_names)
    print(df)


# FUNCTION DEFINITIONS END HERE
clearDatabase(cursor)
# Create the entire database schema
query = "PRAGMA foreign_keys = ON;"
cursor.execute(query)
query = """
    CREATE TABLE Client(
        clientNo INT NOT NULL,
        clientfName VARCHAR(50),
        clientlName VARCHAR(50),
        clientAddress VARCHAR(100),
        clientTelNo VARCHAR(11),
        PRIMARY KEY (clientNo)
    );
"""
cursor.execute(query)

query = """
    CREATE TABLE Services(
        serviceID INT NOT NULL,
        startDate DATE,
        startTime TIME,
        duration TIME,
        comments VARCHAR (100), 
        clientNo INT NOT NULL,
        FOREIGN KEY (clientNo) REFERENCES Client (clientNo) ON DELETE CASCADE
    );
"""
cursor.execute(query)

query = """
    CREATE TABLE Employee(
        empNo INT NOT NULL,
        empfName VARCHAR (50),
        emplName VARCHAR (50),
        empAddress VARCHAR (100),
        salary FLOAT,
        empTelNo VARCHAR (11),
        PRIMARY KEY (empNo)
    );
"""
cursor.execute(query)

query = """
    CREATE TABLE Equipment(
        equipID INT NOT NULL,
        description VARCHAR (100),
        usage INT,
        cost FLOAT,
        PRIMARY KEY (equipID)
    );
"""
cursor.execute(query)

# Create at least 5 tuples for each relation in the database

query = """
    INSERT INTO Client
    VALUES
    (1, 'Miguel', 'Gomez', '1585 Albenga Avenue', '19543196477'),
    (2, 'Jeremy', 'Saintyl', '1585 Albenga Avenue', '11234568910'),
    (3, 'Lydia', 'Dixon', '1280 Stanford Dr', '12223334444'),
    (4, 'James', 'Bond', '30 Wellington Square', '17892345678'),
    (5, 'Bruce', 'Wayne', '224 Park Drive', '11112345677');
"""
cursor.execute(query)

query = """
    INSERT INTO Services
    VALUES
    (1, '2023-12-06', '01:02:03', '10:05:00', 'Hella good service', 1),
    (2, '2023-12-07', '02:03:04', '15:10:05', 'Mediocre service', 2),
    (3, '2023-12-08', '03:04:05', '20:15:10', 'Lackluster service', 3),
    (4, '2023-12-09', '04:05:06', '21:20:15', 'Terrible Service', 4),
    (5, '2023-12-10', '05:06:07', '22:25:20', 'IM BATMAN', 5);
"""
cursor.execute(query)

query = """
    INSERT INTO Employee
    VALUES
    (100, 'Alexandra', 'Gordon-Smith', '1234 Collins Avenue', 123000.99, '1123456789'),
    (101, 'Frank', 'Gordon-Smith', '1234 Collins Avenue', 100000.99, '2394058893'),
    (102, 'Jeremiah', 'Beans', '1234 MadeUp St', 100000.99, '9879999912'),
    (103, 'Patrick', 'Janssens', '1858 Monroe St', 82340.99, '1203495890'),
    (104, 'Mariah', 'Carey', '987 Polar Express Avenue',10000000.98, '2983458023');
"""
cursor.execute(query)

query = """
    INSERT INTO Equipment
    VALUES
    (1000, 'Industrial Strength Vacuum', 14, 2500.00),
    (1001, 'Pressure Washer', 4, 1000.99),
    (1002, 'Industrial Rug Cleaner', 5, 4000.00),
    (1003, 'Zamboni Machine', 12, 10100.99),
    (1004, 'Auto Floor Cleaner', 17, 3500.99);
"""
cursor.execute(query)

query = """
    SELECT *
    FROM Client;
"""
cursor.execute(query)
printTable(cursor)

query = """
    SELECT *
    FROM Services;
"""
cursor.execute(query)
printTable(cursor)

query = """
    SELECT *
    FROM Employee;
"""
cursor.execute(query)
printTable(cursor)

query = """
    SELECT *
    FROM Equipment;
"""
cursor.execute(query)
printTable(cursor)

# Develop 5 SQL queries using embedded SQL
print('-----------------------------------------------------------------------------')
print("\n\nFind service attributes like startDate, startTime, etc. for clientNo 2")
query = """
    SELECT *
    FROM Services
    WHERE clientNo = 2;
"""
cursor.execute(query)
printTable(cursor)

print('-----------------------------------------------------------------------------')
print("""\n\nFind the employees who work for Super Maids Cleaning Company whose salary
is greater than 110,000""")
query = """
    SELECT empfName, emplName
    FROM Employee 
    WHERE salary > 110000;
"""
cursor.execute(query)
printTable(cursor)

print('-----------------------------------------------------------------------------')
print("""\n\nFind the equipment identifier where it has been used at least 10 times or
more and costs less than $5,000 to use""")
query = """
    SELECT equipID 
    FROM Equipment
    WHERE usage >= 10 AND cost < 5000;
"""
cursor.execute(query)
printTable(cursor)

print('-----------------------------------------------------------------------------')
print("""\n\nFind the comments made on a service from a specific client""")
query = """
    SELECT a.clientfName, a.clientlName, b.comments
    FROM Client a
    JOIN Services b ON b.clientNo = a.clientNo;
"""
cursor.execute(query)
printTable(cursor)

print('------------------------------------------------------------------------------')
print("""\n\nList the client, start date, and start time where the duration
is past 20 hours""")
query = """
    SELECT a.clientfName, a.clientlName, b.startDate, b.startTime
    FROM Client a
    JOIN Services b ON b.clientNo = a.clientNo
    WHERE b.duration > '20:00:00'
    ORDER BY b.startDate DESC;
"""
cursor.execute(query)
printTable(cursor)

# Upload all code and documentation to GitHub

# Commit any changes to the database
db_connect.commit()
# Close the connection if we are done with it
# Just be sure any changes have been committed or they will be lost
db_connect.close()