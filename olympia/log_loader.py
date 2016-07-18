from olympia import s3_log_access, settings, models, app


def execute():
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
    return {'total': count}
