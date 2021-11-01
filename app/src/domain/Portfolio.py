class Portfolio():
    def __init__(self, ticker=str, quantity=int, purchase_price=float, account_number: int = -1):
        self.account_number = account_number
        self.ticker = ticker
        self.quantity = quantity
        self.purchase_price = purchase_price
