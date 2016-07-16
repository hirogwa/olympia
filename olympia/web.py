from olympia import app, operations
from flask import request


@app.route('/raw', methods=['POST'])
def generate_raw():
    args = request.get_json()
    return operations.s3_to_raw()


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'
