import datetime
from olympia import models

DATE_FORMAT = '%Y%m%d'


def key_by_date(bucket, key_prefix=None, date_from=None, date_to=None):
    '''
    Returns the number of distinct (remote_ip, user_agent) pairs
    and cumulative downloads for each (key, date) pair in the given date range.
    '''
    q = models.db.session.query(
        models.LogDay.bucket,
        models.LogDay.key,
        models.LogDay.date,
        models.db.func.count(1),
        models.db.func.sum(models.LogDay.download_count))

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
        bucket, key, date, distinct_user, download_count = x
        if key not in result:
            result[key] = {}
        result.get(key)[date] = {
            'users': distinct_user,
            'downloads': download_count
        }

    return result


def key_by_week(bucket, key_prefix=None, date_to=None):
    '''
    Returns the number of distinct (remote_ip, user_agent) pairs
    and cumulative downloads for the week until the given date for each key
    '''
    if date_to:
        date_to = datetime.datetime.strptime(date_to, DATE_FORMAT)
    else:
        date_to = datetime.date.today()
    date_from = date_to - datetime.timedelta(days=6)

    return key_cumulative(bucket,
                          key_prefix,
                          date_from.strftime(DATE_FORMAT),
                          date_to.strftime(DATE_FORMAT))


def key_cumulative(bucket, key_prefix=None, date_from=None, date_to=None):
    '''
    Returns the number of distinct (remote_ip, user_agent) pairs
    and cumulative downloads for each key in the given date range.
    '''
    q = models.db.session.query(
        models.LogDay.bucket,
        models.LogDay.key,
        models.db.func.count(1),
        models.db.func.sum(models.LogDay.download_count))

    q = _filter_base(q, bucket, key_prefix, date_from, date_to)

    q = q. \
        group_by(
            models.LogDay.bucket,
            models.LogDay.key). \
        order_by(
            models.LogDay.bucket,
            models.LogDay.key). \
        all()

    return {k: {'users': u, 'downloads': d} for _, k, u, d in q}, \
        date_from, date_to


def _filter_base(q, bucket, key_prefix, date_from, date_to):
    q = q.filter(models.LogDay.bucket == bucket)
    if key_prefix:
        q = q.filter(models.LogDay.key.like(key_prefix + '%'))
    if date_from:
        q = q.filter(models.LogDay.date >= date_from)
    if date_to:
        q = q.filter(models.LogDay.date <= date_to)
    return q
