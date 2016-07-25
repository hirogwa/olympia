from olympia import models, app


def aggregate(bucket):
    hour_lower = _get_hour_lower_limit(bucket)
    hour_upper = _get_hour_upper_limit(bucket)

    count_source = 0
    count_target = 0
    for log_hour in get_log_hour_entries(hour_lower, hour_upper):
        models.db.session.add(log_hour)
        count_source += log_hour.download_count
        count_target += 1

    result = models.AggregationLogDatetimeToHour(
        bucket, hour_lower, hour_upper, count_source, count_target)
    models.db.session.add(result)
    models.db.session.commit()

    app.logger.info(
        '{} raw entries aggregated to {} hour entries for bucket {}. Hour range:[{}, {})'.
        format(result.count_source,
               result.count_target,
               result.bucket,
               result.hour_lower,
               result.hour_upper))

    return result


def get_log_hour_entries(hour_lower, hour_upper):
    q = models.db.session.query(
        models.LogDatetime.bucket,
        models.LogDatetime.key,
        models.LogDatetime.hour,
        models.LogDatetime.remote_ip,
        models.LogDatetime.user_agent,
        models.db.func.count(1))

    if hour_lower:
        q = q.filter(
            models.LogDatetime.hour >= hour_lower)

    if hour_upper:
        q = q.filter(
            models.LogDatetime.hour < hour_upper)

    q = q. \
        group_by(
            models.LogDatetime.bucket,
            models.LogDatetime.key,
            models.LogDatetime.hour,
            models.LogDatetime.remote_ip,
            models.LogDatetime.user_agent). \
        order_by(
            models.LogDatetime.bucket.asc(),
            models.LogDatetime.key.asc(),
            models.LogDatetime.hour.asc(),
            models.LogDatetime.remote_ip.asc(),
            models.LogDatetime.user_agent.asc()). \
        all()

    return [models.LogHour(b, k, h, r, u, c, h[:8]) for b, k, h, r, u, c in q]


def _get_hour_upper_limit(bucket):
    ''' To be used with query, exclusive
    '''
    latest_datetime = models.LogDatetime.query. \
        filter(
            models.LogDatetime.bucket == bucket). \
        order_by(
            models.LogDatetime.hour.desc()). \
        first()

    return latest_datetime.hour if latest_datetime else None


def _get_hour_lower_limit(bucket):
    ''' To be used with query, inclusive
    '''
    last_record = models.AggregationLogDatetimeToHour.query. \
        filter(models.AggregationLogDatetimeToHour.bucket == bucket). \
        order_by(models.AggregationLogDatetimeToHour.id.desc()). \
        first()

    return last_record.hour_upper if last_record else None
