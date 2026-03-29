import hashlib
import uuid

def generate_hash(namespace, source_id):
    value = f"{namespace}_{source_id}"
    return hashlib.md5(value.encode()).hexdigest()

def generate_uuid():
    return uuid.uuid4().hex
