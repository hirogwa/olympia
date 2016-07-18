from olympia import models


def convert_model(source_query, model_const, chunk_def, commit=False):
    chunk_unit, chunk_unit_current = None, None
    chunk_count, dl_count, total = 0, 0, 0

    key_source = None
    for key_source in source_query:
        total += 1
        dl_count += 1
        chunk_unit = chunk_def(key_source)
        if chunk_unit_current is None:
            chunk_unit_current = chunk_unit
        if chunk_unit != chunk_unit_current:
            args = chunk_unit_current + (dl_count,)
            models.db.session.add(model_const(*args))
            chunk_unit_current = chunk_unit
            dl_count = 0
            chunk_count += 1

    if chunk_unit:
        args = chunk_unit + (dl_count,)
        models.db.session.add(model_const(*args))
        chunk_count += 1

    if commit:
        models.db.session.commit()
    return total, chunk_count
