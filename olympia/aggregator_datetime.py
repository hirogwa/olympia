from olympia import app, models


def aggregate():
    time_lower = _get_time_lower_limit()
    time_upper = _get_time_upper_limit()

    count = 0
    for raw, dt in get_log_datetime_entries(time_lower):
        models.db.session.add(dt)
        count += 1
    result = models.AggregationLogRawToDatetime(
        time_lower, time_upper, count, count)
    models.db.session.add(result)
    models.db.session.commit()
    app.logger.info(
        '{} raw entries converted to datetime entries. Time range:[{}, {})'.
        format(count, time_lower, time_upper))

    return result


def get_log_datetime_entries(time_lower):
    q = models.LogEntryRaw.query

    if time_lower:
        q = q.filter(
            models.LogEntryRaw.time >= time_lower)

    q = q. \
        filter(
            models.LogEntryRaw.user_agent.notlike('"S3Console%'),
            models.LogEntryRaw.user_agent.notlike('"aws-sdk-java%'),
            models.LogEntryRaw.user_agent.notlike('"facebookexternalhit%'),
            models.LogEntryRaw.user_agent.notlike('"Boto%')). \
        filter(
            models.LogEntryRaw.operation.like('REST.GET%') |
            models.LogEntryRaw.operation.like('WEBSITE.GET%')). \
        filter(
            models.LogEntryRaw.http_status.like('2%') |
            models.LogEntryRaw.http_status.like('3%')). \
        order_by(
            models.LogEntryRaw.bucket.asc(),
            models.LogEntryRaw.key.asc(),
            models.LogEntryRaw.remote_ip.asc(),
            models.LogEntryRaw.user_agent.asc(),
            models.LogEntryRaw.time.asc()). \
        all()

    return [(x, _raw_to_datetime(x)) for x in q]


def _get_time_lower_limit():
    ''' To be used with query, inclusive
    '''
    last_record = models.AggregationLogRawToDatetime.query. \
        order_by(models.AggregationLogRawToDatetime.id.desc()). \
        first()

    return last_record.time_upper if last_record else None


def _get_time_upper_limit():
    latest_raw = models.LogEntryRaw.query. \
        order_by(
            models.LogEntryRaw.time.desc()). \
        first()

    return latest_raw.time if latest_raw else None


def _raw_to_datetime(raw):
    hour = raw.time.strftime('%Y%m%d%H')
    return models.LogDatetime(
        raw.bucket, raw.key, hour, raw.time, raw.remote_ip, raw.user_agent)
