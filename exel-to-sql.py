import pandas as pd
import psycopg2

def send_request(request:str):
    conn = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="localhost")
    cursor = conn.cursor()
    cursor.execute(request)
    conn.commit()
    cursor.close()
    conn.close()

class Item():
    def __init__(self,
                id:int,
                packageid:int,
                name:str=None,
                price:int=None,
                ref:str=None,
                warranty:str=None,
                duration:int=None):
        self.id = id
        self.name = name
        self.price = price
        self.ref = ref
        self.packageid = packageid
        self.warranty = warranty
        self.duration = duration

    def push_to_table(self):
        sql_query = (f'INSERT INTO items ('
                    'itemid, packageid, name, price, ref, warranty, duration)'
                    f' VALUES ({self.id}, {self.packageid}, '
                    f'{self.name or 'NULL'}, '
                    f'{self.price or 'NULL'}, '
                    f'{self.ref or 'NULL'}, '
                    f'{self.warranty or 'NULL'}, '
                    f'{self.duration or 'NULL'}'
                    ')')
        send_request(sql_query)

    def addvalue(self, label, value):
        if (label == "name"):
            self.name = value or None
        elif (label == "price"):
            try:
                self.duration = int(value)
            except ValueError:
                self.duration = None
            self.price = int(value) or None
        elif (label == "ref"):
            self.ref = value or None
        elif (label == "warranty"):
            self.warranty = value or None
        elif (label == "duration"):
            try:
                self.duration = int(value)
            except ValueError:
                self.duration = None

def load_excel_data(filename:str):
    xl = pd.ExcelFile(filename)
    orders = xl.sheet_names
    data = {}
    for order in orders:
        data[order] = xl.parse(order)
    orderid : int = 1
    for order in orders:
        create_order(orderid, order)
        get_order_from_sheet(data[order], orderid)
        orderid += 1

def get_order_from_sheet(order, orderid: int):
    labels = order.get('lables')
    packages = order.get('packages')
    items = order.get('items')
    values = order.get('values')
    i = 0
    packageid = 0
    itemId = 0
    item = Item(0, 0)
    while i < len(packages):
        if packages[i] != packageid:
            create_package(orderid, packageid)
            item.push_to_table()
            itemId = 0
            item = Item(items[i], packages[i])
        if items[i] != itemId:
            item.push_to_table()
            item = Item(items[i], packageid)
        packageid = packages[i]
        itemId = items[i]
        item.addvalue(labels[i], values[i])
        i += 1
    create_package(orderid, packageid)
    item.push_to_table()

def create_package(orderid:int , packageid: int):
    sql_query = f"INSERT INTO packages (orderid, packageid) VALUES ({orderid}, {packageid})"
    send_request(sql_query)

def create_order(orderid, order):
    sql_query = f"INSERT INTO orders (orderid, packageid) VALUES ({orderid}, {order})"
    send_request(sql_query)

load_excel_data('./Orders.xlsx')
