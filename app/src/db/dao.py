# Database Access Object: file to interface with the database
# CRUD operations:
# C: Create
# R: Read
# U: Update
# D: Delete
import typing as t
from mysql.connector import connect, cursor
from mysql.connector.connection import MySQLConnection
import config
from app.src.domain.Investor import Investor
from app.src.domain.Account import Account
from app.src.domain.Portfolio import Portfolio


def get_cnx() -> MySQLConnection:
    return connect(**config.dbparams)


'''
    Investor DAO functions
'''


def get_all_investor() -> list[Investor]:
    '''
        Get list of all investors [R]
    '''
    investors: list[Investor] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True)  # always pass dictionary = True
    sql: str = 'select * from investor'
    cursor.execute(sql)
    results: list[dict] = cursor.fetchall()
    for row in results:
        investors.append(Investor(row['name'], row['status'], row['id']))
    db_cnx.close()
    return investors


def get_investor_by_id(id: int) -> t.Optional[Investor]:
    '''
        Returns an investor object given an investor ID [R]
    '''
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True)  # always pass dictionary = True
    sql: str = 'select * from investor where id = %s'
    cursor.execute(sql, (id,))
    if cursor.rowcount == 0:
        return None
    else:
        row = cursor.fetchone()
        investor = Investor(row['name'], row['status'], row['id'])
        return investor


def get_investors_by_name(name: str) -> list[Investor]:
    '''
        Return a list of investors for a given name [R]
    '''
    investors: list[Investor] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True)  # always pass dictionary = True
    sql: str = 'select * from investor where name = %s'
    cursor.execute(sql, (name,))
    if cursor.rowcount == 0:
        investors = []
    else:
        rows = cursor.fetchall()
        for row in rows:
            investors.append(Investor(row['name'], row['status'], row['id']))
    db_cnx.close()
    return investors


def create_investor(investor: Investor) -> None:
    '''
        Create a new investor in the db given an investor object [C]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'insert into investor (name, status) values (%s, %s)'
    cursor.execute(sql, (investor.name, investor.status))
    db_cnx.commit()  # inserts, updates, and deletes
    db_cnx.close()


def delete_investor(id: int):
    '''
        Delete an investor given an id [D]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'delete from investor where id = %s'
    cursor.execute(sql, (id,))
    db_cnx.commit()  # inserts, updates, and deletes
    db_cnx.close()


def update_investor_name(id: int, name: str) -> None:
    '''
        Updates the investor name [U]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'update investor set name = %s where id = %s'
    cursor.execute(sql, (id, name))
    db_cnx.commit()  # inserts, updates, and deletes
    db_cnx.close()


def update_investor_status(id: int, status: str) -> None:
    '''
        Update the inestor status [U]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'update investor set status = %s where id = %s'
    cursor.execute(sql, (id, status))
    db_cnx.commit()  # inserts, updates, and deletes
    db_cnx.close()


'''
    Account DAO functions
'''


def get_all_accounts() -> list[Account]:
    accounts: list[Account] = []
    db_cnx: MySQLConnection = get_cnx()
    cur = db_cnx.cursor(dictionary=True)  # always pass dictionary = True
    sql: str = 'select * from account'
    cur.execute(sql)
    results: list[dict] = cursor.fetchall()
    for row in results:
        accounts.append(Account(row['investor_id'],
                        row['balance'], row['account_number']))
    db_cnx.close()
    return accounts


def get_account_by_id(account_number: int) -> Account:
    account: list[Account] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True)  # always pass dictionary = True
    sql: str = 'select * from account where account_number = %s'
    cursor.execute(sql, (account_number,))
    rows: list[dict] = cursor.fetchall()
    if cursor.rowcount == 0:
        print('No such account number found in the database!')
    else:
        for row in rows:
            account.append(
                Account(row['investor_id'], row['balance'], row['account_number']))
    db_cnx.close()
    return account


def get_accounts_by_investor_id(id: int) -> list[Account]:
    db_cnx: MySQLConnection = get_cnx()
    cur = db_cnx.cursor(Dictionary=True)
    sql = 'select account_number,investor_id,balance from account where investor_id=%s'
    cur.execute(sql, (id,))
    rows = cur.fetchall()
    if len(rows) == 0:
        return []
    accounts = []
    for row in rows:
        accounts.append(Account(row['investor_id'],
                        row['balance'], row['account_number']))
    db_cnx.close()
    return accounts


def delete_account(id: int) -> None:
    # Deletes an id from account
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'delete from account where id = %s'
    cursor.execute(sql, (id,))
    db_cnx.commit()  # inserts, updates, and deletes
    db_cnx.close()


def update_acct_balance(investor_id: int, balance: float) -> None:
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'update account set balance = %s where investor_id = %s'
    cursor.execute(sql, (balance, investor_id,))
    db_cnx.commit()  # inserts, updates, and deletes
    db_cnx.close()


def create_account(account: Account) -> None:
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'insert into account(investor_id, balance) values(%s, %s)'
    cursor.execute(sql, (account.investor_id, account.balance))
    db_cnx.commit()  # inserts, updates, and deletes
    db_cnx.close()


'''
    Portfolio DAO functions
'''


def get_all_portfolios() -> list[Portfolio]:
    Port_folio: list[Portfolio] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True)  # always pass dictionary = True
    sql: str = 'select * from portfolio'
    cursor.execute(sql)
    results: list[dict] = cursor.fetchall()
    for row in results:
        Port_folio.append(Portfolio(
            row['ticker'], row['quantity'], row['purchase_price'], row['account_number']))
    db_cnx.close()
    return Port_folio


def get_porfolios_by_acct_id(account_number: int) -> list[Portfolio]:
    port_folio: list[Portfolio] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True)  # always pass dictionary = True
    sql: str = 'select * from portfolio where account_number = %s'
    cursor.execute(sql, (account_number,))
    results: list[dict] = cursor.fetchall()
    if results.count == 0:
        print("No portfolio exists for given the account number")
    else:
        for row in results:
            port_folio.append(Portfolio(
                row['ticker'], row['quantity'], row['purchase_price'], row['account_number']))
    db_cnx.close()
    return port_folio


def get_portfolios_by_investor_id(investor_id: int) -> list[Portfolio]:
    accounts: list[Portfolio] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True)  # always pass dictionary = True
    sql: str = '''select ticker, quantity, purchase_price, j.investor_id, f.account_number
                from portfolio f 
                inner join account j
                on f.account_number = j.account_number
                where investor_id = %s'''
    cursor.execute(sql, (investor_id,))
    results: list[dict] = cursor.fetchall()
    if results.count == 0:
        print("No portfolio exists for the  given Investor id ")
    else:
        for row in results:
            accounts.append(Portfolio(
                row['account_number'], row['ticker'], row['quantity'], row['purchase_price']))
    db_cnx.close()
    return accounts


def delete_portfolio(id: int) -> None:
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'delete from portfolio where account_number = %s'
    cursor.execute(sql, (id, ))
    db_cnx.commit()  # inserts, updates, and deletes
    db_cnx.close()


def buy_stock(ticker: str, price: float, quantity: int) -> None:
    # code goes here creating a new row in portfolio
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'insert into porfolio(ticker,price, quantity) values(%s, %s,%s)'
    cursor.execute(sql, (ticker, price, quantity))
    db_cnx.commit()  # inserts, updates, and deletes
    db_cnx.close()


def sell_stock(ticket: str, quantity: int, sale_price: float) -> None:
    # 1. update quantity in portfolio table
    # 2. update the account balance:
    # Example: 10 APPL shares at $1/share with account balance $100
    # event: sale of 2 shares for $2/share
    # output: 8 APPLE shares at $1/share with account balance = 100 + 2 * (12 - 10) = $104

    def update_stockqty(ticker: str, quantity: int) -> None:
        db_cnx = get_cnx()
        cursor = db_cnx.cursor()
        sql = 'update portfolio set stock_qty=10 where ticker=MSFT'
        cursor.execute(sql, (ticker, quantity))
        db_cnx.commit()
        db_cnx.close()
        return update_stockqty

    def update_account_balance(investor_id: int, balance: float) -> None:
        db_cnx = get_cnx()
        cursor = db_cnx.cursor()
        sql = 'update account set balance=100 where investor_id=1'
        cursor.execute(sql, (investor_id, balance))
        db_cnx.commit()
        db_cnx.close()
        return update_account_balance

    def update_stockqty(ticker: str, quantity: int) -> None:
        db_cnx = get_cnx()
        cursor = db_cnx.cursor()
        sql = 'update portfolio set stock_qty=7 where ticker=MSFT'
        cursor.execute(sql, (ticker, quantity))
        db_cnx.commit()
        db_cnx.close()
        return update_stockqty

    def update_account_balance(investor_id: int, balance: float) -> None:
        db_cnx = get_cnx()
        cursor = db_cnx.cursor()
        sql = 'update account set balance=110 where investor_id=1'
        cursor.execute(sql, (investor_id, balance))
        db_cnx.commit()
        db_cnx.close()
        return update_account_balance
