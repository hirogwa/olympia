import uuid
from olympia import app, models, aggregator_common
from olympia.models import AggregationLogHourToDay


def execute():
    record = AggregationLogHourToDay.query. \
        filter_by(
            bound_type=AggregationLogHourToDay.BOUND_TYPE_TO). \
        order_by(
            models.AggregationLogHourToDay.processed_datetime.desc()). \
        first()
    hour_upper = _get_time_upper_limit()
    query = _query_hour_entry(hour_upper, record)

    first_processed_hour, last_processed_hour, count_source, count_result = \
        aggregator_common.convert_model(
            query,
            models.LogDay,
            lambda x: (x.bucket, x.key, x.hour[:8], x.remote_ip, x.user_agent))

    if first_processed_hour:
        _record_aggregation_result(first_processed_hour, last_processed_hour)
        app.logger.info(
            '{} hour entries aggregated to {} day entries. Hour upper bound:{}'.
            format(count_source, count_result, hour_upper))
    else:
        app.logger.info('No hour entry aggregated to day. Hour upper bound:{}'.
                        format(hour_upper))

    return last_processed_hour


def _record_aggregation_result(first, last):
    id_string = uuid.uuid4().hex

    bound_from = AggregationLogHourToDay(
        id_string,
        AggregationLogHourToDay.BOUND_TYPE_FROM,
        first.bucket, first.key, first.remote_ip, first.user_agent, first.hour)
    models.db.session.add(bound_from)

    bound_to = AggregationLogHourToDay(
        id_string,
        AggregationLogHourToDay.BOUND_TYPE_TO,
        last.bucket, last.key, last.remote_ip, last.user_agent, last.hour)
    models.db.session.add(bound_to)

    models.db.session.commit()


def _get_time_upper_limit():
    latest_hour = models.LogHour.query. \
        order_by(
            models.LogHour.hour.desc()). \
        first()

    return latest_hour.hour[:8] + '00' if latest_hour else None


def _query_hour_entry(hour_upper, record_lower):
    q = models.LogHour.query

    if hour_upper:
        q = q.filter(
            models.LogHour.hour < hour_upper)

    if record_lower:
        q = q.filter(
            models.LogHour.bucket > record_lower.bucket,
            models.LogHour.key > record_lower.key,
            models.LogHour.remote_ip > record_lower.remote_ip,
            models.LogHour.user_agent > record_lower.user_agent,
            models.LogHour.hour > record_lower.hour)

    return q. \
        order_by(
            models.LogHour.bucket.asc(),
            models.LogHour.key.asc(),
            models.LogHour.remote_ip.asc(),
            models.LogHour.user_agent.asc(),
            models.LogHour.hour.asc()). \
        all()
