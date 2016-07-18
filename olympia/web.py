from olympia import app, log_loader, aggregator_hour, aggregator_day
from flask import jsonify


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
    result = log_loader.execute()
    return jsonify(result='success', info=dict(result))


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'
