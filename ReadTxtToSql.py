import sys
import csv
import pyodbc
from timeit import default_timer as timer

start = timer()
params = []
# Connect to the database
connection = pyodbc.connect('Driver={SQL Server};Server=5.79.11.132;Database=pmix;uid=sa;pwd=M@rk3tI.1999')   


# Get the cursor, which is used to traverse the database, line by line
cursor = connection.cursor()

# Using cursor to create database
#cursor.execute("DROP DATABASE insight")
#cursor.execute("CREATE DATABASE insight")
cursor.execute("USE pmix")

# Create Tables
cursor.execute("CREATE TABLE PMX_2021 ( \
STORE_NO VARCHAR(255) ,\
BUSINESS_DATE VARCHAR(255) ,\
MENU_CODE VARCHAR(255) ,\
MENU_NAME VARCHAR(255) ,\
PRICE_EAT_IN VARCHAR(255) ,\
PRICE_TAKE_AWAY VARCHAR(255) ,\
PRICE_MDS_UNITS VARCHAR(255) ,\
UNITS_EAT_IN VARCHAR(255) ,\
UNITS_TAKE_AWAY VARCHAR(255) ,\
UNITS_MDS VARCHAR(255) ,\
UNITS_PROMO VARCHAR(255) ,\
UNITS_WASTE VARCHAR(255) ,\
EMPLOYEE_MEAL VARCHAR(255) ,\
MANAGER_MEAL VARCHAR(255) ,\
UNITS_DISCOUNT VARCHAR(255) \
)")


count = 0
# Create the INSERT INTO sql query
query = """INSERT INTO PMX_2021 (
STORE_NO, BUSINESS_DATE,
MENU_CODE, MENU_NAME,
PRICE_EAT_IN, PRICE_TAKE_AWAY,
PRICE_MDS_UNITS, UNITS_EAT_IN,
UNITS_TAKE_AWAY, UNITS_MDS,
UNITS_PROMO, UNITS_WASTE,
EMPLOYEE_MEAL, MANAGER_MEAL,
UNITS_DISCOUNT) VALUES 
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

with open('PMX-2021.txt', newline = '', encoding='ISO-8859-1') as pmx_csv:
    csv_reader = csv.reader(pmx_csv, delimiter='\t')
    next(csv_reader)
    for cr in csv_reader:
        STORE_NO = cr[0]
        BUSINESS_DATE = cr[1]
        MENU_CODE = cr[2]
        MENU_NAME = cr[3]
        PRICE_EAT_IN = cr[4]
        PRICE_TAKE_AWAY = cr[5]
        PRICE_MDS_UNITS = cr[6]
        UNITS_EAT_IN = cr[7]
        UNITS_TAKE_AWAY = cr[8]
        UNITS_MDS = cr[9]
        UNITS_PROMO = cr[10]
        UNITS_WASTE = cr[11]
        EMPLOYEE_MEAL = cr[12]
        MANAGER_MEAL = cr[13]
        UNITS_DISCOUNT = cr[14]
        # Assign values from each row
        values = (STORE_NO, BUSINESS_DATE,
        MENU_CODE, MENU_NAME,
        PRICE_EAT_IN, PRICE_TAKE_AWAY,
        PRICE_MDS_UNITS, UNITS_EAT_IN,
        UNITS_TAKE_AWAY, UNITS_MDS,
        UNITS_PROMO, UNITS_WASTE,
        EMPLOYEE_MEAL, MANAGER_MEAL,
        UNITS_DISCOUNT)
        # Execute sql Querys
        insert_query = str(query) + str(values)
        #cursor.execute(query,values)    
        params.append(values);
        # cursor.fast_executemany = False
        # cursor.executemany(query,values)  
        # count = count + 1
        # if count == 200: 
        #     break

print(start)
cursor.fast_executemany = True
cursor.executemany(query,params)  


# Close the cursor
cursor.close()
# Commit the transaction
connection.commit()
# Close the database connection
connection.close()

end = timer()
print(end - start)  # Time in seconds, e.g. 5.38091952400282