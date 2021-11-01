class Account():
    # create a class that mimics the database table: account
    def __init__(self, investor_id: int, balance: float, account_number: int = -1):
        self.account_number = account_number
        self.investor_id = investor_id
        self.balance = balance
