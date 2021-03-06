import datetime
from flask_sqlalchemy import SQLAlchemy
from olympia import app

db = SQLAlchemy(app)


class LogDay(db.Model):
    bucket = db.Column(db.String(63), primary_key=True)
    key = db.Column(db.String(1024), primary_key=True)
    date = db.Column(db.String(8), primary_key=True)
    remote_ip = db.Column(db.String(15), primary_key=True)
    user_agent = db.Column(db.String(65536), primary_key=True)
    download_count = db.Column(db.Integer)

    def __init__(self, bucket, key, date, remote_ip, user_agent,
                 download_count):
        self.bucket = bucket
        self.key = key
        self.date = date
        self.remote_ip = remote_ip
        self.user_agent = user_agent
        self.download_count = download_count

    def __iter__(self):
        for key in ['bucket', 'key', 'date', 'remote_ip', 'user_agent',
                    'download_count']:
            yield(key, getattr(self, key))


class LogHour(db.Model):
    bucket = db.Column(db.String(63), primary_key=True)
    key = db.Column(db.String(1024), primary_key=True)
    hour = db.Column(db.String(10), primary_key=True)
    remote_ip = db.Column(db.String(15), primary_key=True)
    user_agent = db.Column(db.String(65536), primary_key=True)
    download_count = db.Column(db.Integer)
    date = db.Column(db.String(8), index=True)

    __table_args__ = (
        db.Index('idx_log_hour_01', 'bucket', 'key', 'date'),
    )

    def __init__(self, bucket, key, hour, remote_ip, user_agent,
                 download_count, date):
        self.bucket = bucket
        self.key = key
        self.hour = hour
        self.remote_ip = remote_ip
        self.user_agent = user_agent
        self.download_count = download_count
        self.date = date

    def __iter__(self):
        for key in ['bucket', 'key', 'hour', 'remote_ip', 'user_agent',
                    'download_count', 'date']:
            yield(key, getattr(self, key))


class LogDatetime(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    bucket = db.Column(db.String(63))
    key = db.Column(db.String(1024))
    hour = db.Column(db.String(10), index=True)
    datetime = db.Column(db.DateTime(timezone=True))
    remote_ip = db.Column(db.String(15))
    user_agent = db.Column(db.String(65536))

    __table_args__ = (
        db.Index('idx_log_datetime', 'bucket', 'key', 'hour', 'remote_ip',
                 'user_agent'),
    )

    def __init__(self, bucket, key, hour, datetime, remote_ip, user_agent):
        self.bucket = bucket
        self.key = key
        self.hour = hour
        self.datetime = datetime
        self.remote_ip = remote_ip
        self.user_agent = user_agent

    def __iter__(self):
        for key in ['bucket', 'key', 'hour', 'datetime', 'remote_ip',
                    'user_agent']:
            yield key, getattr(self, key)


class LogEntryRaw(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    bucket_owner = db.Column(db.String(64))
    bucket = db.Column(db.String(63), index=True)
    time = db.Column(db.DateTime(timezone=True), index=True)
    remote_ip = db.Column(db.String(15))
    requester = db.Column(db.String(64))
    request_id = db.Column(db.String(16))
    operation = db.Column(db.String(50))
    key = db.Column(db.String(1024))
    request_uri = db.Column(db.String(1200))
    http_status = db.Column(db.String(3))
    error_code = db.Column(db.String(50))
    bytes_sent = db.Column(db.Integer)
    object_size = db.Column(db.Integer)
    total_time = db.Column(db.Integer)
    turnaround_time = db.Column(db.Integer)
    referrer = db.Column(db.String(2083))
    user_agent = db.Column(db.String(65536))
    version_id = db.Column(db.String(100))

    __table_args__ = (
        db.Index('idx_log_hour', 'bucket', 'key', 'remote_ip',
                 'user_agent', 'time'),
    )

    def __init__(self,
                 bucket_owner,
                 bucket,
                 time,
                 remote_ip,
                 requester,
                 request_id,
                 operation,
                 key,
                 request_uri,
                 http_status,
                 error_code,
                 bytes_sent,
                 object_size,
                 total_time,
                 turnaround_time,
                 referrer,
                 user_agent,
                 version_id):
        self.bucket_owner = bucket_owner
        self.bucket = bucket
        self.time = _time(time)
        self.remote_ip = remote_ip
        self.requester = requester
        self.request_id = request_id
        self.operation = operation
        self.key = key
        self.request_uri = request_uri
        self.http_status = http_status
        self.error_code = error_code
        self.bytes_sent = _int(bytes_sent)
        self.object_size = _int(object_size)
        self.total_time = _int(total_time)
        self.turnaround_time = _int(turnaround_time)
        self.referrer = referrer
        self.user_agent = user_agent
        self.version_id = version_id

    def __iter__(self):
        for key in ['bucket', 'key', 'remote_ip', 'user_agent', 'time']:
            yield(key, getattr(self, key))


class AggregationLogHourToDay(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    bucket = db.Column(db.String(63), index=True)
    date_lower = db.Column(db.String(8))
    date_upper = db.Column(db.String(8))
    count_source = db.Column(db.Integer)
    count_target = db.Column(db.Integer)
    processed_datetime = db.Column(
        db.DateTime(timezone=True),
        default=lambda x: datetime.datetime.now(datetime.timezone.utc))

    def __init__(self, bucket,  date_lower, date_upper, count_source,
                 count_target):
        self.bucket = bucket
        self.date_lower = date_lower
        self.date_upper = date_upper
        self.count_source = count_source
        self.count_target = count_target

    def __iter__(self):
        for key in ['bucket', 'date_lower', 'date_upper', 'count_source',
                    'count_target']:
            yield key, getattr(self, key)


class AggregationLogDatetimeToHour(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    bucket = db.Column(db.String(63), index=True)
    hour_lower = db.Column(db.String(10))
    hour_upper = db.Column(db.String(10))
    count_source = db.Column(db.Integer)
    count_target = db.Column(db.Integer)
    processed_datetime = db.Column(
        db.DateTime(timezone=True),
        default=lambda x: datetime.datetime.now(datetime.timezone.utc))

    def __init__(self, bucket, hour_lower, hour_upper, count_source,
                 count_target):
        self.bucket = bucket
        self.hour_lower = hour_lower
        self.hour_upper = hour_upper
        self.count_source = count_source
        self.count_target = count_target

    def __iter__(self):
        for key in ['bucket', 'hour_lower', 'hour_upper', 'count_source',
                    'count_target', 'processed_datetime']:
            yield key, getattr(self, key)


class AggregationLogRawToDatetime(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    bucket = db.Column(db.String(63), index=True)
    time_lower = db.Column(db.DateTime(timezone=True))
    time_upper = db.Column(db.DateTime(timezone=True))
    count_source = db.Column(db.Integer)
    count_target = db.Column(db.Integer)
    processed_datetime = db.Column(
        db.DateTime(timezone=True),
        default=lambda x: datetime.datetime.now(datetime.timezone.utc))

    def __init__(self, bucket, time_lower, time_upper, count_source,
                 count_target):
        self.bucket = bucket
        self.time_lower = time_lower
        self.time_upper = time_upper
        self.count_source = count_source
        self.count_target = count_target

    def __iter__(self):
        for key in ['bucket', 'time_lower', 'time_upper', 'count_source',
                    'count_target', 'processed_datetime']:
            yield key, getattr(self, key)


def _time(s):
    return datetime.datetime.strptime(s, '[%d/%b/%Y:%H:%M:%S %z]')


def _int(s):
    return 0 if s == '-' else int(s)
