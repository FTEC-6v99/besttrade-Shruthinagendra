class Investor():
    # class has same attributes as the investor db table
    def __init__(self, name: str, status: str, id=None):
        self.id = id
        self.name = name
        self.status = status
