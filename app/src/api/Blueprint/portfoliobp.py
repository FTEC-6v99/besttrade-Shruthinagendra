import typing as t
import json
from flask import Blueprint
import app.src.db.dao as dao

from app.src.domain.Portfolio import Portfolio
#

portfolio_bp = bp = Blueprint('portfolio', __name__, url_prefix='/portfolio')


@bp.route('/get-all-portfolios')
def get_all_portfolios():
    portfolios: t.List[Portfolio] = dao.get_all_portfolios()
    if len(portfolios) == 0:
        return json.dumps([])
    else:
        return json.dumps(portfolios, default=lambda x: x.__dict__)


@bp.route('/get-portfolio-by-acct-no/<int:account_number>')
def get_portfolio_by_acct_no(account_number: int):
    portfolio: Portfolio = dao.get_portfolio_by_acct_no(account_number)
    if portfolio is None:
        return json.dumps('')
    return json.dumps(portfolio, default=lambda x: x.__dict__)


@bp.route('/get-portfolio-by-investor-id/<int:investor_id>')
def get_portfolio_by_investor_id(investor_id: int):
    portfolio: Portfolio = dao.get_portfolio_by_investor_id(investor_id)
    if portfolio is None:
        return json.dumps('')
    return json.dumps(portfolio, default=lambda x: x.__dict__)


@bp.route('/delete-portfolio/<account_number>/<ticker>', methods=['DELETE'])
def delete_portfolio(account_number, ticker):
    dao.delete_portfolio(account_number, ticker)
    return '', 200


@bp.route('/buy-stock/<account_number>/<ticker>/<quantity>/<purchase_price>', methods=['PUT'])
def buy_stock(ticker, quantity, price):
    portfolio = Portfolio(ticker, quantity, price)
    try:
        dao.buy_stock(portfolio)
        return json.dumps(portfolio.__dict__)
    except Exception as e:
        return 'Error occurred:' + str(e), 500


@bp.route('/sell-stock/<account_number>/<ticker>/<quantity>/<purchase_price>', methods=['PUT'])
def sell_stock(ticker, quantity, price):
    portfolio = Portfolio(ticker, quantity, price)
    try:
        dao.sell_stock(portfolio)
        return json.dumps(portfolio.__dict__)
    except Exception as e:
        return 'Error occurred:' + str(e), 500
