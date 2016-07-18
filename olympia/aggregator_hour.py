import datetime
from olympia import aggregator_common, models, app
from olympia.models import AggregationLogRawToHour


def execute():
    time_lower = _get_time_lower_limit()
    time_upper = _get_time_upper_limit()

    query = _query_filtered_raw_entry(time_lower, time_upper)

    _, _, count_source, count_target = \
        aggregator_common.convert_model(
            query,
            models.LogHour,
            lambda x: (x.bucket, x.key, x.time.strftime('%Y%m%d%H'),
                       x.remote_ip, x.user_agent))
    result = AggregationLogRawToHour(
        time_lower, time_upper, count_source, count_target)
    models.db.session.add(result)
    models.db.session.commit()

    app.logger.info(
        '{} raw entries aggregated to {} hour entries. Time range:[{}, {})'.
        format(result.count_source,
               result.count_target,
               result.time_lower,
               result.time_upper))

    return result


def _get_time_upper_limit():
    ''' To be used with query, exclusive
    '''
    latest_raw = models.LogEntryRaw.query. \
        order_by(
            models.LogEntryRaw.time.desc()). \
        first()

    if latest_raw:
        t = latest_raw.time
        return datetime.datetime(
            t.year, t.month, t.day, t.hour, tzinfo=t.tzinfo)
    else:
        return None


def _get_time_lower_limit():
    ''' To be used with query, inclusive
    '''
    last_record = models.AggregationLogRawToHour.query. \
        order_by(models.AggregationLogRawToHour.id.desc()). \
        first()

    return last_record.time_upper if last_record else None


def _query_filtered_raw_entry(time_lower, time_upper):
    q = models.LogEntryRaw.query

    if time_upper:
        q = q.filter(
            models.LogEntryRaw.time < time_upper)

    if time_lower:
        q = q.filter(
            models.LogEntryRaw.time >= time_lower)

    return q. \
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
