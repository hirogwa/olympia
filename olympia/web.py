from olympia import app, log_loader, aggregator_hour, aggregator_day, \
    stat_operation
from flask import request, jsonify


@app.route('/stat/key_by_day', methods=['GET'])
def stat_key_by_day():
    bucket = request.args.get('bucket')
    assert bucket, 'bucket required'
    result = stat_operation.key_by_date(bucket)
    return jsonify(
        status='success', bucket=bucket, result=result)


@app.route('/stat/key_cumulative', methods=['GET'])
def stat_key_cumulative():
    bucket = request.args.get('bucket')
    assert bucket, 'bucket required'
    result = stat_operation.key_cumulative(bucket)
    return jsonify(
        status='success', bucket=bucket, result=result)


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
