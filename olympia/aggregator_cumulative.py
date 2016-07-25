from olympia import models


def get_cumulative_aggregation_query(
        date_lower, date_upper, upper_inclusive=False, bucket=None,
        key_prefix=None):
    q = models.db.session.query(
        models.LogDay.bucket,
        models.LogDay.key,
        models.LogDay.remote_ip,
        models.LogDay.user_agent,
        models.db.func.sum(
            models.LogDay.download_count).label('download_count'))

    if bucket:
        q = q.filter(models.LogDay.bucket == bucket)

    if key_prefix:
        q = q.filter(models.LogDay.key.like(key_prefix + '%'))

    if date_lower:
        q = q.filter(models.LogDay.date >= date_lower)

    if date_upper:
        if upper_inclusive:
            q = q.filter(models.LogDay.date <= date_upper)
        else:
            q = q.filter(models.LogDay.date < date_upper)

    return q. \
        group_by(
            models.LogDay.bucket,
            models.LogDay.key,
            models.LogDay.remote_ip,
            models.LogDay.user_agent). \
        order_by(
            models.LogDay.bucket.asc(),
            models.LogDay.key.asc(),
            models.LogDay.remote_ip.asc(),
            models.LogDay.user_agent.asc())
