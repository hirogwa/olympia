from olympia import app, operations
from flask import request


@app.route('/day', methods=['POST'])
def generator_day():
    operations.hour_to_day()
    return 'done'


@app.route('/hour', methods=['POST'])
def generate_hour():
    operations.raw_to_hour()
    return 'done'


@app.route('/raw', methods=['POST'])
def generate_raw():
    args = request.get_json()
    return operations.s3_to_raw()


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'
