from olympia import app, log_loader, aggregator_hour, aggregator_day, \
    stat_operation
from flask import request, jsonify


@app.route('/stat/key_by_day', methods=['GET'])
def stat_key_by_day():
    bucket, key_prefix = _stat_essentials(request.args)
    result = stat_operation.key_by_date(bucket, key_prefix)
    return jsonify(
        status='success', bucket=bucket, keys=result)


@app.route('/stat/key_by_week', methods=['GET'])
def stat_key_by_week():
    args = request.args
    bucket, key_prefix = _stat_essentials(args)
    date_to = args.get('date_to')
    keys, date_from, date_to = \
        stat_operation.key_by_week(bucket, key_prefix, date_to)
    return jsonify(status='success', bucket=bucket, keys=keys,
                   date_from=date_from, date_to=date_to)


@app.route('/stat/key_cumulative', methods=['GET'])
def stat_key_cumulative():
    bucket, key_prefix = _stat_essentials(request.args)
    keys, date_from, date_to = \
        stat_operation.key_cumulative(bucket, key_prefix)
    return jsonify(status='success', bucket=bucket, keys=keys,
                   date_from=date_from, date_to=date_to)


def _stat_essentials(args):
    bucket = args.get('bucket')
    key_prefix = request.args.get('key_prefix')
    assert bucket, 'bucket required'
    return bucket, key_prefix


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
