from olympia import app, aggregator_hour, aggregator_day, operations
from flask import request, jsonify


@app.route('/day', methods=['POST'])
def generator_day():
    result = aggregator_day.execute()
    return jsonify(result='success',
                   last_processed=dict(result) if result else {})


@app.route('/hour', methods=['POST'])
def generate_hour():
    result = aggregator_hour.execute()
    return jsonify(result='success',
                   last_processed=(dict(result) if result else {}))


@app.route('/raw', methods=['POST'])
def generate_raw():
    args = request.get_json()
    return operations.s3_to_raw()


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'
