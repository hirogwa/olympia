from olympia import app, models


def aggregate():
    date_lower = _get_date_lower_limit()
    date_upper = _get_date_upper_limit()

    count_target = 0
    q = get_day_aggregation_query().all()
    for log_day in [models.LogDay(b, k, d, r, u, s) for b, k, d, r, u, s in q]:
        models.db.session.add(log_day)
        count_target += 1

    count_source = _get_source_count(date_lower, date_upper)
    result = models.AggregationLogHourToDay(
        date_lower, date_upper, count_source, count_target)
    models.db.session.add(result)
    models.db.session.commit()

    app.logger.info(
        '{} hour entries aggregated to {} day entries. Hour range:[{}, {})'.
        format(result.count_source,
               result.count_target,
               result.date_lower,
               result.date_upper))

    return result


def get_day_aggregation_query(upper_inclusive=False, date_lower=None,
                              date_upper=None, bucket=None, key_prefix=None):
    date_lower = date_lower or _get_date_lower_limit()
    date_upper = date_upper or _get_date_upper_limit()

    q = models.db.session.query(
        models.LogHour.bucket,
        models.LogHour.key,
        models.LogHour.date,
        models.LogHour.remote_ip,
        models.LogHour.user_agent,
        models.db.func.sum(
            models.LogHour.download_count).label('download_count'))

    if bucket:
        q = q.filter(models.LogHour.bucket == bucket)

    if key_prefix:
        q = q.filter(models.LogHour.key.like(key_prefix + '%'))

    if date_lower:
        q = q.filter(models.LogHour.date >= date_lower)

    if date_upper:
        if upper_inclusive:
            q = q.filter(models.LogHour.date <= date_upper)
        else:
            q = q.filter(models.LogHour.date < date_upper)

    return q. \
        group_by(
            models.LogHour.bucket,
            models.LogHour.key,
            models.LogHour.date,
            models.LogHour.remote_ip,
            models.LogHour.user_agent). \
        order_by(
            models.LogHour.bucket.asc(),
            models.LogHour.key.asc(),
            models.LogHour.date.asc(),
            models.LogHour.remote_ip.asc(),
            models.LogHour.user_agent.asc())


def _get_source_count(date_lower, date_upper):
    q = models.LogHour.query

    if date_lower:
        q = q.filter(
            models.LogHour.date >= date_lower)
    if date_upper:
        q = q.filter(
            models.LogHour.date < date_upper)

    return q.count()


def _get_date_lower_limit():
    ''' To be used with query, inclusive
    '''
    last_record = models.AggregationLogHourToDay.query. \
        order_by(models.AggregationLogHourToDay.id.desc()). \
        first()

    return last_record.date_upper if last_record else None


def _get_date_upper_limit():
    ''' To be used with query, exclusive
    '''
    latest_hour = models.LogHour.query. \
        order_by(
            models.LogHour.date.desc()). \
        first()

    return latest_hour.date if latest_hour else None
