import typing as t
import json
from flask import Blueprint, request
import app.src.db.dao as dao

from app.src.domain.Investor import Investor

investor_bp = bp = Blueprint('investor', __name__, url_prefix='/investor')


@bp.route('/get-all-investors')
def get_all_investors():
    investors: t.List[Investor] = dao.get_all_investors()
    if len(investors) == 0:
        return json.dumps([])
    else:
        return json.dumps(investors, default=lambda x: x.__dict__)


@bp.route('/get_investor_by_id/<int:id>')
def get_investor_by_id(id: int):
    investor: Investor = dao.get_investor_by_id(id)
    if investor is None:
        return json.dumps('')
    return json.dumps(investor, default=lambda x: x.__dict__)


@bp.route('/get-investors-by-name/<name>')
def get_investors_by_name(name: str):
    investors: t.List[Investor] = dao.get_investors_by_name(name)
    if len(investors) == 0:
        return json.dumps([])
    else:
        return json.dumps(investors, default=lambda x: x.__dict__)


@bp.route('/create-new-investor/<name>/<status>', methods=['POST'])
def create_investor(name, status):
    investor: Investor = Investor(name, status)
    dao.create_investor(investor)
    return '', 200


@bp.route('/update-investor-name/<id>/<name>', methods=['PUT'])
def update_investor_name(id, name):
    dao.update_investor_name(id, name)
    return '', 200


@bp.route('/update-investor-status/<id>/<status>', methods=['PUT'])
def update_investor_status(id, status):
    dao.update_investor_status(id, status)
    return '', 200


@bp.route('/delete-investor/<id>', methods=['DELETE'])
def delete_investor(id):
    dao.delete_investor(id)
    return '', 200
