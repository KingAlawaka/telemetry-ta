def get_by_path(payload, path):
    # path like 'payload.id' or 'payload.data.value'
    try:
        parts = path.split('.')
        # If the first part is 'payload', skip it
        if parts and parts[0] == 'payload':
            parts = parts[1:]
        val = payload
        for p in parts:
            if isinstance(val, dict):
                val = val.get(p)
            else:
                return None
        return val
    except Exception:
        return None