import boto3
from olympia import settings


client = boto3.client(
    's3',
    region_name=settings.S3_REGION,
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
)


def log_entries(bucket, prefix, delete_when_done=True):
    ''' Max 1000 keys
    '''
    response = client.list_objects_v2(
        Bucket=bucket,
        EncodingType='url',
        Prefix=prefix
    )
    if response.get('KeyCount') != 0:
        for key in [x.get('Key') for x in response.get('Contents', [])]:
            for x in _load_log_key(bucket, key, delete_when_done):
                yield(x)


def _load_log_key(bucket, key, delete_when_done):
    obj = client.get_object(Bucket=bucket, Key=key)
    body = obj.get('Body').read().decode('utf-8')
    for x in body.split('\n')[:-1]:
        delim = '\n'
        line = _replace_delimiter(x, delim)
        yield line.split(delim)

    if delete_when_done:
        client.delete_object(Bucket=bucket, Key=key)


def _replace_delimiter(s, delim='\n'):
    original_delim = ' '
    openers = ['[', '"']
    closers = [']', '"']
    inside_block = False
    target_indices = []
    for i, c in enumerate(s):
        if c in openers and s[i-1] == original_delim:
            inside_block = True
            continue
        if c in closers and s[i+1] == original_delim:
            inside_block = False
            continue
        if c == original_delim and not inside_block:
            target_indices.append(i)
    return ''.join(
        [delim if i in target_indices else x for i, x in enumerate(s)])
