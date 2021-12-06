from flask import Flask
from app.src.blueprint.Investorbp import investorbp
from app.src.blueprint.accountbp import accountbp
from app.src.blueprint.portfoliobp import portfoliobp


app = Flask(__name__)
app.register_blueprint(investorbp)
app.register_blueprint(accountbp)
app.register_blueprint(portfoliobp)


if __name__ == '__main__':
    app.run(port=8080)
