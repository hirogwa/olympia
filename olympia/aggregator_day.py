from olympia import app, models


def aggregate():
    date_lower = _get_date_lower_limit()
    date_upper = _get_date_upper_limit()

    count_target = 0
    for log_date in get_log_day_entries(date_lower, date_upper):
        models.db.session.add(log_date)
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


def get_log_day_entries(date_lower, date_upper):
    q = models.db.session.query(
        models.LogHour.bucket,
        models.LogHour.key,
        models.LogHour.date,
        models.LogHour.remote_ip,
        models.LogHour.user_agent,
        models.db.func.sum(models.LogHour.download_count))

    if date_lower:
        q = q.filter(
            models.LogHour.date >= date_lower)

    if date_upper:
        q = q.filter(
            models.LogHour.date < date_upper)

    q = q. \
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
            models.LogHour.user_agent.asc()). \
        all()

    return [models.LogDay(b, k, d, r, u, s) for b, k, d, r, u, s in q]


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
