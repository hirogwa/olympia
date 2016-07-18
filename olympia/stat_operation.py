from olympia import models


def key_by_date(bucket, key_prefix=None, date_from=None, date_to=None):
    '''
    Returns the number of distinct (remote_ip, user_agent) pairs
    that downloaded each (key, date) in the given date range.
    '''
    q = models.db.session.query(
        models.LogDay.bucket,
        models.LogDay.key,
        models.LogDay.date,
        models.db.func.count(1))

    q = _filter_base(q, bucket, key_prefix, date_from, date_to)

    q = q. \
        group_by(
            models.LogDay.bucket,
            models.LogDay.key,
            models.LogDay.date). \
        order_by(
            models.LogDay.bucket,
            models.LogDay.key,
            models.LogDay.date). \
        all()

    result = {}
    for x in q:
        bucket, key, date, count = x
        if key in result:
            result.get(key)[date] = count
        else:
            result[key] = {date: count}

    return result


def key_cumulative(bucket, key_prefix=None, date_from=None, date_to=None):
    '''
    Returns the number of distinct (remote_ip, user_agent) pairs
    that downloaded each key in the given date range.
    '''
    q = models.db.session.query(
        models.LogDay.bucket,
        models.LogDay.key,
        models.db.func.count(1))

    q = _filter_base(q, bucket, key_prefix, date_from, date_to)

    q = q. \
        group_by(
            models.LogDay.bucket,
            models.LogDay.key). \
        order_by(
            models.LogDay.bucket,
            models.LogDay.key). \
        all()

    return {k: v for _, k, v in q}


def _filter_base(q, bucket, key_prefix, date_from, date_to):
    q = q.filter(models.LogDay.bucket == bucket)
    if key_prefix:
        q = q.filter(models.LogDay.key.like(key_prefix + '%'))
    if date_from:
        q = q.filter(models.LogDay.date >= date_from)
    if date_to:
        q = q.filter(models.LogDay.date <= date_to)
    return q
