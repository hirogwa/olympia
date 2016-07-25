from olympia import models, aggregator_day, aggregator_cumulative

STAT_USERS = 'users'
STAT_DOWNLOADS = 'downloads'


def key_by_date(bucket, key_prefix=None, date_from=None, date_to=None):
    '''
    Returns the number of distinct (remote_ip, user_agent) pairs
    and cumulative downloads for each (key, date) pair in the given date range.
    '''
    # from log_day
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

    # from log_hour
    q_from_hour = _log_day_list_from_hour_by_date(date_to, bucket, key_prefix)

    result = {}
    for x in q + q_from_hour:
        bucket, key, date, distinct_user, download_count = x
        if key not in result:
            result[key] = {}
        result.get(key)[date] = {
            STAT_USERS: distinct_user,
            STAT_DOWNLOADS: int(download_count)
        }

    return result


def key_cumulative(bucket, key_prefix=None, date_from=None, date_to=None):
    '''
    Returns the number of distinct (remote_ip, user_agent) pairs
    and cumulative downloads for each key in the given date range.
    '''
    # from cumulative
    q = _aggregated_stat_cumulative(date_from, date_to, bucket, key_prefix)
    result = {k: {STAT_USERS: u, STAT_DOWNLOADS: d} for _, k, u, d in q}

    # from log_hour
    q_h = _log_day_list_from_hour_cumulative(date_to, bucket, key_prefix)
    for _, k, u, d in q_h:
        if k not in result:
            result[k] = {STAT_USERS: 0, STAT_DOWNLOADS: 0}
        result.get(k)[STAT_USERS] += u
        result.get(k)[STAT_DOWNLOADS] += d

    return result


def _log_day_list_from_hour_by_date(date_to, bucket, key_prefix):
    ''' generates aggregated log_day out of log_hour
    '''
    sub_q = aggregator_day. \
        get_day_aggregation_query(
            bucket, upper_inclusive=True, date_lower=None, date_upper=date_to,
            key_prefix=key_prefix). \
        subquery()

    q = models.db.session. \
        query(
            sub_q.c.bucket,
            sub_q.c.key,
            sub_q.c.date,
            models.db.func.count(1),
            # returns Decimal
            models.db.func.sum(sub_q.c.download_count)). \
        group_by(
            sub_q.c.bucket,
            sub_q.c.key,
            sub_q.c.date). \
        order_by(
            sub_q.c.bucket,
            sub_q.c.key,
            sub_q.c.date). \
        all()
    return [(b, k, d, c, int(s)) for b, k, d, c, s in q]


def _log_day_list_from_hour_cumulative(date_to, bucket, key_prefix):
    ''' generates aggregated log_day out of log_hour
    '''
    sub_q = aggregator_day. \
        get_day_aggregation_query(
            bucket, upper_inclusive=True, date_lower=None,
            date_upper=date_to, key_prefix=key_prefix). \
        subquery()

    q = models.db.session. \
        query(
            sub_q.c.bucket,
            sub_q.c.key,
            models.db.func.count(1),
            # returns Decimal
            models.db.func.sum(sub_q.c.download_count)). \
        group_by(
            sub_q.c.bucket,
            sub_q.c.key). \
        order_by(
            sub_q.c.bucket,
            sub_q.c.key). \
        all()
    return [(b, k, c, int(s)) for b, k, c, s in q]


def _aggregated_stat_cumulative(date_from, date_to, bucket, key_prefix):
    sub_q = aggregator_cumulative. \
        get_cumulative_aggregation_query(
            date_from, date_to, True, bucket, key_prefix). \
        subquery()

    q = models.db.session. \
        query(
            sub_q.c.bucket,
            sub_q.c.key,
            models.db.func.count(1),
            # returns Decimal
            models.db.func.sum(sub_q.c.download_count)). \
        group_by(
            sub_q.c.bucket,
            sub_q.c.key). \
        order_by(
            sub_q.c.bucket,
            sub_q.c.key). \
        all()
    return [(b, k, c, int(s)) for b, k, c, s in q]


def _filter_base(q, bucket, key_prefix, date_from, date_to):
    q = q.filter(models.LogDay.bucket == bucket)
    if key_prefix:
        q = q.filter(models.LogDay.key.like(key_prefix + '%'))
    if date_from:
        q = q.filter(models.LogDay.date >= date_from)
    if date_to:
        q = q.filter(models.LogDay.date <= date_to)
    return q
