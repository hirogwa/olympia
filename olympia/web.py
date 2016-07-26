from olympia import app, log_loader, aggregator_datetime, aggregator_hour, \
    aggregator_day, stat_operation
from flask import request, jsonify


@app.route('/stat/key_by_day', methods=['GET'])
def stat_key_by_day():
    bucket = request.args.get('bucket')
    key_prefix = request.args.get('key_prefix')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    assert bucket, 'bucket required'
    result = stat_operation.key_by_date(bucket, key_prefix, date_from, date_to)
    return jsonify(
        status='success', bucket=bucket, keys=result)


@app.route('/stat/key_cumulative', methods=['GET'])
def stat_key_cumulative():
    bucket = request.args.get('bucket')
    key_prefix = request.args.get('key_prefix')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    assert bucket, 'bucket required'
    result = stat_operation.key_cumulative(
        bucket, key_prefix, date_from, date_to)
    return jsonify(
        status='success', bucket=bucket, keys=result)


@app.route('/day', methods=['POST'])
def generator_day():
    result = aggregator_day.aggregate(_arg_bucket_or_assert())
    return jsonify(result='success', info=dict(result))


@app.route('/hour', methods=['POST'])
def generate_hour():
    result = aggregator_hour.aggregate(_arg_bucket_or_assert())
    return jsonify(result='success', info=dict(result))


@app.route('/datetime', methods=['POST'])
def generate_datetime():
    result = aggregator_datetime.aggregate(_arg_bucket_or_assert())
    return jsonify(result='success', info=dict(result))


@app.route('/raw', methods=['POST'])
def generate_raw():
    result = log_loader.execute(_arg_bucket_or_assert())
    return jsonify(result='success', info=dict(result))


def _arg_bucket_or_assert():
    bucket = request.args.get('bucket')
    assert bucket, 'bucket required'
    return bucket


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'
