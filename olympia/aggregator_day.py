from olympia import app, models, aggregator_common


def execute():
    hour_lower = _get_hour_lower_limit()
    hour_upper = _get_hour_upper_limit()

    query = _query_hour_entry(hour_lower, hour_upper)

    count_source, count_target = \
        aggregator_common.convert_model(
            query,
            models.LogDay,
            lambda x: (x.bucket, x.key, x.hour[:8], x.remote_ip, x.user_agent),
            lambda x: x.download_count)
    result = models.AggregationLogHourToDay(
        hour_lower, hour_upper, count_source, count_target)
    models.db.session.add(result)
    models.db.session.commit()

    app.logger.info(
        '{} hour entries aggregated to {} day entries. Hour range:[{}, {})'.
        format(result.count_source,
               result.count_target,
               result.hour_lower,
               result.hour_upper))

    return result


def _get_hour_upper_limit():
    ''' To be used with query, exclusive
    '''
    latest_hour = models.LogHour.query. \
        order_by(
            models.LogHour.hour.desc()). \
        first()

    return latest_hour.hour[:8] + '00' if latest_hour else None


def _get_hour_lower_limit():
    ''' To be used with query, inclusive
    '''
    last_record = models.AggregationLogHourToDay.query. \
        order_by(models.AggregationLogHourToDay.id.desc()). \
        first()

    return last_record.hour_upper if last_record else None


def _query_hour_entry(hour_lower, hour_upper):
    q = models.LogHour.query

    if hour_upper:
        q = q.filter(
            models.LogHour.hour < hour_upper)

    if hour_lower:
        q = q.filter(
            models.LogHour.hour >= hour_lower)

    return q. \
        order_by(
            models.LogHour.bucket.asc(),
            models.LogHour.key.asc(),
            models.LogHour.remote_ip.asc(),
            models.LogHour.user_agent.asc(),
            models.LogHour.hour.asc()). \
        all()
