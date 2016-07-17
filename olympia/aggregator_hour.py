import datetime
import uuid
from olympia import aggregator_common, models, app
from olympia.models import AggregationLogRawToHour


def execute():
    record = AggregationLogRawToHour.query. \
        filter_by(
            bound_type=AggregationLogRawToHour.BOUND_TYPE_TO). \
        order_by(models.AggregationLogRawToHour.processed_datetime.desc()). \
        first()
    time_upper = _get_time_upper_limit()
    query = _query_filtered_raw_entry(time_upper, record)

    first_processed_raw, last_processed_raw, count_source, count_result = \
        aggregator_common.convert_model(
            query,
            models.LogHour,
            lambda x: (x.bucket, x.key, x.time.strftime('%Y%m%d%H'),
                       x.remote_ip, x.user_agent))

    if first_processed_raw:
        _record_aggregation_result(first_processed_raw, last_processed_raw)
        models.db.session.commit()
        app.logger.info(
            '{} raw entries aggregated to {} hour entries. Time upper bound:{}'.
            format(count_source, count_result, time_upper))
    else:
        app.logger.info('No raw entry aggregated to hour. Time upper bound:{}'.
                        format(time_upper))

    return last_processed_raw


def _record_aggregation_result(first, last):
    id_string = uuid.uuid4().hex

    bound_from = AggregationLogRawToHour(
        id_string,
        AggregationLogRawToHour.BOUND_TYPE_FROM,
        first.bucket, first.key, first.remote_ip, first.user_agent, first.time)
    models.db.session.add(bound_from)

    bound_to = AggregationLogRawToHour(
        id_string,
        AggregationLogRawToHour.BOUND_TYPE_TO,
        last.bucket, last.key, last.remote_ip, last.user_agent, last.time)
    models.db.session.add(bound_to)

    models.db.session.commit()


def _get_time_upper_limit():
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


def _query_filtered_raw_entry(time_upper, record_lower=None):
    q = models.LogEntryRaw.query

    if time_upper:
        q = q.filter(
            models.LogEntryRaw.time < time_upper)

    if record_lower:
        q = q.filter(
            models.LogEntryRaw.bucket > record_lower.bucket,
            models.LogEntryRaw.key > record_lower.key,
            models.LogEntryRaw.remote_ip > record_lower.remote_ip,
            models.LogEntryRaw.user_agent > record_lower.user_agent,
            models.LogEntryRaw.time > record_lower.time)

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
