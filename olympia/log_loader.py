from olympia import s3_log_access, models, app


def execute(bucket):
    count = 0
    prefix = 'logs/'
    for e in s3_log_access.log_entries(bucket, prefix, True):
        raw = models.LogEntryRaw(*e)
        models.db.session.add(raw)
        count += 1
        if count % 150 == 0:
            models.db.session.commit()
            app.logger.info('entries added:{}'.format(count))

    models.db.session.commit()
    app.logger.info('entries added total:{}'.format(count))
    return {'total': count}
