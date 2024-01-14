import sqlite3
import pandas as pd
# Connects to an existing database file in the current directory
# If the file does not exist, it creates it in the current directory
db_connect = sqlite3.connect('test.db')

# Instantiate cursor object for executing queries
cursor = db_connect.cursor()
'''
EMPLOYEE TABLE: emp_id (PK), fName, lName, department, pos_id
POSITION TABLE: pos_id (PK), posName, rate
TASK TABLE: task_id (PK), startDate, endDate, estimate
RESERVATION: emp_id (PK), task_id (PK), dateWorkedOn, hoursWorked

INSERT DATE for Oracle TO_DATE('1999-12-09','yyyy-mm-dd')
EVERYTHING ELSE 'yyyy-mm-dd'
'''
#
# FOR THE TA GRADER : https://www.youtube.com/watch?v=DCBmhs4dYzc&list=RDDCBmhs4dYzc&start_radio=1&ab_channel=Maroon5VEVO
#

# clearing the database so the tables/views get loaded fresh per execution
def dropTables(table):
    return ("DROP TABLE IF EXISTS " + table)
def dropViews(view):
    return ("DROP VIEW IF EXISTS " + view)
def clearDatabase(cursor):
    # need to turn off foreign keys so that constraint errors don't pop up
    query = """
        PRAGMA foreign_keys = OFF;
    """
    cursor.execute(query)
    query = dropTables("Employee")
    cursor.execute(query)
    query = dropTables("Position")
    cursor.execute(query)
    query = dropTables("Task")
    cursor.execute(query)
    query = dropTables("Reservation")
    cursor.execute(query)
    # Dropping views that will be created in the future
    query = dropViews("EmployeeDetails")
    cursor.execute(query)
    query = dropViews("TaskDetails")
    cursor.execute(query)

clearDatabase(cursor=cursor)  

query = "PRAGMA foreign_keys = ON;"
cursor.execute(query)
# String variable for passing queries to cursor
query = """
    CREATE TABLE Reservation(
    emp_id INT NOT NULL,
    task_id INT NOT NULL,
    dateWorkedOn DATE,
    hoursWorked INT,
    CONSTRAINT HoursRange CHECK (hoursWorked BETWEEN 0 AND 150),
    PRIMARY KEY (emp_id, task_id),
    FOREIGN KEY (emp_id) REFERENCES Employee (emp_id),
    FOREIGN KEY (task_id) REFERENCES Task (task_id) ON DELETE CASCADE
    );
"""
cursor.execute(query)
query = """
    CREATE TABLE Task(
    task_id INT NOT NULL,
    startDate DATE,
    endDate DATE,
    estimate INT,
    PRIMARY KEY (task_id)
    );
"""
cursor.execute(query)
# This trigger callback function acts to raise an error if startDate/endDate
#  is not greater than today's date  
query = """
    CREATE TRIGGER validate_date
    BEFORE INSERT ON Task
    BEGIN 
        SELECT 
            CASE
                WHEN DATE() > NEW.startDate OR DATE() > NEW.endDate THEN 
                    RAISE (ABORT, 'Invalid date entered')
            END;
    END;
"""
cursor.execute(query)
query = """
    CREATE TABLE Position(
    pos_id INT NOT NULL,
    posName VARCHAR(50),
    rate INT,
    CONSTRAINT PossiblePositions CHECK (posName IN ('Programmer', 'Analyst', 'Manager')),
    CONSTRAINT RateAmount CHECK (rate BETWEEN 15 AND 200),
    PRIMARY KEY (pos_id)
    );
    """
cursor.execute(query)
query = """
    CREATE TABLE Employee(
    emp_id INT NOT NULL,
    fName VARCHAR(50),
    lName VARCHAR(50),
    department VARCHAR(50),
    pos_id INT,
    PRIMARY KEY (emp_id),
    FOREIGN KEY (pos_id) REFERENCES Position (pos_id)
    );
    """
# Execute query, the result is stored in cursor
cursor.execute(query)

# Insert row into table
query = """
    INSERT INTO Position
    VALUES 
    (10, "Programmer", 125),
    (20, "Analyst", 100),
    (30, "Manager", 135),
    (40, "Programmer", 100),
    (50, "Analyst", 110);
"""
cursor.execute(query)

query = """
    INSERT INTO Employee
    VALUES 
    (100, "Miguel", "Gomez", "IT for Monkeys", 10),
    (101, "Jeremy", "Saintyl", "Marketing", 30),
    (102, "Joshua", "Sparro", "Research", 20),
    (103, "Adrian", "Ivancica", "Banana Sales", 40),
    (104, "Patrick", "Janssens", "Bubble Animals", 50);
    """
cursor.execute(query)


query = """
    INSERT INTO Task
    VALUES 
    (45, '2024-10-19', '2024-12-06', 365),
    (46, '2024-12-30', '2025-12-30', 365),
    (47, '2024-05-20', '2024-06-20', 40),
    (48, '2024-07-20', '2024-07-25', 10),
    (49, '2024-08-15', '2024-11-15', 200);
"""
cursor.execute(query)

query = """
    INSERT INTO Reservation
    VALUES 
    (100, 45, '2003-10-31', 120),
    (101, 46, '2004-11-12', 115),
    (102, 47, '2005-12-01', 110),
    (103, 48, '2006-09-15', 105),
    (104, 49, '2007-06-05', 100);
"""
cursor.execute(query)
# Select data

query = """
    SELECT *
    FROM Position a, Employee b
    WHERE a.pos_id = b.pos_id
    ORDER BY a.rate
"""
cursor.execute(query)
def printTable(cursor):
    print()
    print()
    # Extract column names from cursor
    column_names = [row[0] for row in cursor.description]
    # Fetch data and load into a pandas dataframe
    table_data = cursor.fetchall()
    df = pd.DataFrame(table_data, columns=column_names)
    # examine dataframe
    print(df)

printTable(cursor)

query = """
    SELECT *
    FROM Task a, Reservation b
    WHERE a.task_id = b.task_id
"""
cursor.execute(query)

printTable(cursor=cursor)
# print(df.columns)
# Example to extract a specific column
# print(df['name'])

print('After Updates in rates')

query = """
    UPDATE Position
    SET rate = rate*0.9
    WHERE posName = 'Analyst'
"""
cursor.execute(query)

query = """
    SELECT *
    FROM Position
    ORDER BY rate ASC
"""
cursor.execute(query)
printTable(cursor=cursor)

print('After Updates in changing a position from Programmer to Analyst')

query = """
    UPDATE Position
    SET posName = 'Analyst'
    WHERE pos_id = 10
"""
cursor.execute(query)
query = """
    SELECT * 
    FROM Employee
    
"""
cursor.execute(query)
printTable(cursor=cursor)
query = """
    SELECT *
    FROM Position
    ORDER BY rate ASC
"""
cursor.execute(query)
printTable(cursor=cursor)
# idea is to delete entire row based on emp_id and have delete cascade affect task table

print('After Deleting all tasks from task table where department name is "Research"')
query = """
    DELETE FROM Task
    WHERE task_id = (SELECT task_id
                    FROM Reservation 
                    WHERE emp_id = (SELECT emp_id
                                    FROM Employee
                                    WHERE department = 'Research'))
"""
cursor.execute(query)
query = """
    SELECT *
    FROM Task
"""
cursor.execute(query)
printTable(cursor=cursor)

# Adding the attribute taskManager to the Task table
print()
print()
print('Adding the attribute taskManager')
query = """
    ALTER TABLE Task
    ADD taskManager VARCHAR(50)
    DEFAULT 'Bob Jones'
"""
cursor.execute(query)

query = """
    SELECT *
    FROM Task
"""
cursor.execute(query)
printTable(cursor=cursor)

# Create a view of employee details

print()
print()
print('Creating view with employee details')
query = """
    CREATE VIEW EmployeeDetails
    AS SELECT a.emp_id, a.fName, a.lName, a.department, b.posName
    FROM Employee a, Position b
    WHERE a.pos_id = b.pos_id
"""
cursor.execute(query)
query = """
    SELECT *
    FROM EmployeeDetails
"""
cursor.execute(query)
printTable(cursor=cursor)

# Create a view of tasks currently being worked on
print()
print()
print('Creating view with task and employee details')
query = """
    CREATE VIEW TaskDetails
    AS SELECT c.task_id, a.fName, a.lName, a.department
    FROM Employee a, Reservation b, Task c
    WHERE a.emp_id = b.emp_id AND b.task_id = c.task_id
    ORDER BY c.task_id DESC
"""
cursor.execute(query)

query = """
    SELECT *
    FROM TaskDetails
"""
cursor.execute(query)
printTable(cursor=cursor)

# Commit any changes to the database
db_connect.commit()

# Close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
db_connect.close()
