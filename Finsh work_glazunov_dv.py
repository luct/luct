# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 20:00:25 2021

@author: First
"""


import re
import sqlalchemy as adb
import sqlalchemy_utils as udb
import cx_Oracle as ora
from sqlalchemy import MetaData, Table 
from sqlalchemy_utils import *
import datetime
import pandas as pd
import sys

l_user = 'glazunov_dv'
l_pass = '4582Luc'
l_tns = ora.makedsn('13.95.167.129', 1521, service_name = 'pdb1')

l_conn_ora = adb.create_engine(r'oracle://{p_user}:{p_pass}@{p_tns}'.format(
    p_user = l_user
    , p_pass = l_pass
    , p_tns = l_tns
    )
    )

print(l_conn_ora)

l_sql_execute = l_conn_ora.connect()
l_meta = MetaData(l_conn_ora)

l_ask = input('Создать таблицу glazunov_dv_employees? Y|N: ')
if l_ask.upper() == 'Y':
    try:
        p_create1  = open(r'C:\Users\First\Desktop\Reboot\Scripts\Домашки\create_employee.txt', 'r').read()
        l_sql_execute.execute(p_create1)
        print('Таблица glazunov_dv_employees создана')
    except:
        print('Неудача при создании таблицы glazunov_dv_employees...')
        sys.exit()

try:        
    l_table1 = Table('glazunov_dv_employees', l_meta, autoload = True, autoload_with = l_conn_ora)
    l_file1_csv = pd.read_csv(r'C:\Users\First\Desktop\Reboot\Tab\glazunov_dv_employees.csv')
    l_list1_csv = l_file1_csv.values.tolist()

    for i in l_list1_csv:
        l_table1.insert([l_table1.c.employee_id, l_table1.c.first_name, l_table1.c.last_name, l_table1.c.hire_date,
                         l_table1.c.job_id, l_table1.c.salary, l_table1.c.manager_id,
                         l_table1.c.department_id]
                       ).values (employee_id = i[0], first_name = i[1], last_name = i[2], 
                                    hire_date = datetime.datetime.strptime(i[3], '%d.%m.%y'),
                                 job_id = i[4], salary = i[5], manager_id = i[6], department_id = i[7]
                                ).execute()
        print(1)
    print('В таблицу glazunov_dv_employees загружены данные')
except:
    print('Неудача при загрузке данных в таблицу glazunov_dv_employees...')    
    sys.exit()
    
l_ask = input('Создать таблицу glazunov_dv_customers? Y|N: ')
if l_ask.upper() == 'Y':
    try:
        p_create2  = open(r'C:\Users\First\Desktop\Reboot\Scripts\Домашки\create_customers.txt', 'r').read()
        l_sql_execute.execute(p_create2)
        print('Таблица glazunov_dv_customers создана')
    except:
        print('Неудача при создании таблицы glazunov_dv_customers...')
        sys.exit()
        
try: 
    l_table2 = Table('glazunov_dv_customers', l_meta, autoload = True, autoload_with = l_conn_ora)
    l_file2_csv = pd.read_csv(r'C:\Users\First\Desktop\Reboot\Tab\glazunov_dv_customers.csv')
    l_list2_csv = l_file2_csv.values.tolist()

    for i in l_list2_csv:
        l_table2.insert([l_table2.c.customer_id, l_table2.c.date_register, l_table2.c.login, l_table2.c.first_name,
                         l_table2.c.last_name, l_table2.c.email]).values(
                         customer_id = i[0], date_register = datetime.datetime.strptime(i[1], '%d.%m.%y'), login = i[2], first_name = i[3],
                         last_name = i[4], email = i[5]
                       ).execute()
        print(1)
    print('В таблицу glazunov_dv_customers загружены данные')
except:
    print('Неудача при загрузке данных в таблицу glazunov_dv_customers...')    
    sys.exit()    

l_ask = input('Создать таблицу glazunov_dv_products? Y|N: ')
if l_ask.upper() == 'Y':
    try:
        p_create3  = open(r'C:\Users\First\Desktop\Reboot\Scripts\Домашки\create_products.txt', 'r').read()
        l_sql_execute.execute(p_create3)
        print('Таблица glazunov_dv_products создана')
    except:
        print('Неудача при создании таблицы glazunov_dv_products...')
        sys.exit()

try:         
    l_table3 = Table('glazunov_dv_products', l_meta, autoload = True, autoload_with = l_conn_ora)
    l_file3_csv = pd.read_csv(r'C:\Users\First\Desktop\Reboot\Tab\glazunov_dv_products.csv')
    l_list3_csv = l_file3_csv.values.tolist()

    for i in l_list3_csv:
        l_table3.insert([l_table3.c.product_id, l_table3.c.price, l_table3.c.name,
                         l_table3.c.descriptions]).values (product_id = i[0], price = i[1],
                         name = i[2], descriptions = i[3]
                       ).execute()
        print(1)
    print('В таблицу glazunov_dv_products загружены данные')
except:
    print('Неудача при загрузке данных в таблицу glazunov_dv_products...')    
    sys.exit()    
    
l_ask = input('Создать таблицу glazunov_dv_orders? Y|N: ')
if l_ask.upper() == 'Y':
    try:
        p_create4  = open(r'C:\Users\First\Desktop\Reboot\Scripts\Домашки\create_orders.txt', 'r').read()
        l_sql_execute.execute(p_create4)
        print('Таблица glazunov_dv_orders создана')
    except:
        print('Неудача при создании таблицы glazunov_dv_orders...')
        sys.exit()  
             
p_delete  = open(r'C:\Users\First\Desktop\Reboot\Scripts\Домашки\delete_orders.txt', 'r').read()
l_sql_execute.execute(p_delete)

try:
    l_table4 = Table('glazunov_dv_orders', l_meta, autoload = True, autoload_with = l_conn_ora)
    l_file4_csv = pd.read_csv(r'C:\Users\First\Desktop\Reboot\Tab\glazunov_dv_orders.csv')
    l_list4_csv = l_file4_csv.values.tolist()

    for i in l_list4_csv:
        l_table4.insert([l_table4.c.order_id, l_table4.c.employee_id, l_table4.c.order_date,
                         l_table4.c.product_id, l_table4.c.qty, l_table4.c.customer_id]).values (order_id = i[0], 
                         employee_id = i[1], order_date = datetime.datetime.strptime(i[2], '%d.%m.%y'), product_id = i[3], qty = i[4], customer_id = i[5] 
                         ).execute()
        print(1)
    print('В таблицу glazunov_dv_orders загружены данные') 
except:
    print('Неудача при загрузке данных в таблицу glazunov_dv_orders...')    
    sys.exit()

p_view_sales  = open(r'C:\Users\First\Desktop\Reboot\Scripts\Домашки\create_view_sales.txt', 'r').read()
l_sql_execute.execute(p_view_sales)
print('Витрина Sales создана')

p_view_agr_sales  = open(r'C:\Users\First\Desktop\Reboot\Scripts\Домашки\create_view_agr_sales.txt', 'r').read()
l_sql_execute.execute(p_view_agr_sales)
print('Витрина Agr_Sales создана')
sys.exit()











