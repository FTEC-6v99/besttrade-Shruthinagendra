from flask import Flask
from app.src.api.Blueprint.Investorbp import Investorbp
from app.src.api.Blueprint.accountbp import accountbp
from app.src.api.Blueprint.portfoliobp import portfoliobp
app = Flask(__name__)
app.register_blueprint(Investorbp)
app.register_blueprint(accountbp)
app.register_blueprint(portfoliobp)
if __name__ == '__main__':
    app.run(port=8080)
