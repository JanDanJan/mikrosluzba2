import json, logging, sys

def setup_logging():
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter('%(message)s')
    h.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers = [h]

def log_json(**kwargs):
    print(json.dumps(kwargs), flush=True)
