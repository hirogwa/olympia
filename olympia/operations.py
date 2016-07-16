from olympia import s3_log_access, settings, models, app


def raw_to_hour():
    return _convert_model(
        _query_filtered_raw_entry(),
        models.LogHour,
        lambda x: (x.bucket, x.key, x.time.strftime('%Y%m%d%H'),
                   x.remote_ip, x.user_agent))


def hour_to_day():
    return _convert_model(
        _query_hour(),
        models.LogDay,
        lambda x: (x.bucket, x.key, x.hour[:8],
                   x.remote_ip, x.user_agent))


def s3_to_raw():
    count = 0
    prefix = 'logs/'
    for e in s3_log_access.log_entries(settings.LOG_BUCKET, prefix, True):
        raw = models.LogEntryRaw(*e)
        models.db.session.add(raw)
        count += 1
        if count % 150 == 0:
            app.logger.info('entries added:{}'.format(count))

    models.db.session.commit()
    app.logger.info('entries added total:{}'.format(count))
    return count


def _convert_model(source_query, model_const, chunk_def):
    chunk_unit, chunk_unit_current = None, None
    chunk_count, dl_count, total = 0, 0, 0
    for x in source_query:
        total += 1
        dl_count += 1
        chunk_unit = chunk_def(x)
        if chunk_unit_current is None:
            chunk_unit_current = chunk_unit
        if chunk_unit != chunk_unit_current:
            args = chunk_unit_current + (dl_count,)
            models.db.session.add(model_const(*args))
            chunk_unit_current = chunk_unit
            dl_count = 0
            chunk_count += 1

    if chunk_unit != chunk_unit_current:
        args = chunk_unit + (dl_count,)
        models.db.session.add(model_const(*args))
        chunk_count += 1

    models.db.session.commit()
    app.logger.info(
        '{} entries converted to {} entries'. format(
            total, chunk_count))


def _query_hour():
    return models.LogHour.query. \
        order_by(
            models.LogHour.bucket,
            models.LogHour.key,
            models.LogHour.remote_ip,
            models.LogHour.user_agent,
            models.LogHour.hour). \
        all()


def _query_filtered_raw_entry():
    return models.LogEntryRaw.query. \
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
