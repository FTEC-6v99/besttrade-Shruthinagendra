import typing as t
import json
from flask import Blueprint, request
import app.src.db.dao as dao

from app.src.domain.Portfolio import Portfolio

portfolio_bp = bp = Blueprint('portfolio', __name__, url_prefix='/portfolio')


@bp.route('/get-all-portfolios')
def get_all_portfolios():
    portfolios: t.List[Portfolio] = dao.get_all_portfolios()
    if len(portfolios) == 0:
        return json.dumps([])
    else:
        return json.dumps(portfolios, default=lambda x: x.__dict__)


@bp.route('/get_portfolio_by_id/<int:id>')
def get_portfolio_by_id(id: int):
    portfolio: Portfolio = dao.get_portfolio_by_id(id)
    if portfolio is None:
        return json.dumps('')
    return json.dumps(portfolio, default=lambda x: x.__dict__)


@bp.route('/get-portfolios-by-name/<name>')
def get_portfolios_by_name(name):
    portfolios: t.List[Portfolio] = dao.get_portfolios_by_name(name)
    if len(portfolios) == 0:
        return json.dumps([])
    else:
        return json.dumps(portfolios, default=lambda x: x.__dict__)


@ bp.route('/create-new-portfolio/<name>/<status>', methods=['POST'])
def create_portfolio(name, status):
    portfolio: Portfolio = Portfolio(name, status)
    dao.create_portfolio(portfolio)
    return '', 200


@ bp.route('/updateportfolio-name/<id>/<name>', methods=['PUT'])
def update_portfolio_name(id, name):
    dao.update_portfolio_name(id, name)
    return '', 200


@ bp.route('/update-portfolio-status/<id>/<status>', methods=['PUT'])
def update_portfolio_status(id, status):
    dao.update_portfolio_status(id, status)
    return '', 200


@ bp.route('/delete-portfolio/<id>', methods=['DELETE'])
def delete_portfolio(id):
    dao.delete_portfolio(id)
    return '', 200
