import json, logging, sys

def setup_logging():
    """
    Configure simple stdout logging suitable for JSON application logs.

    I set the root logger to INFO level with a basic formatter that prints the
    message only (since we usually log structured JSON lines).

    @return None
    """
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter('%(message)s')
    h.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers = [h]

def log_json(**kwargs):
    """
    Emit a single line of JSON to stdout for structured logging.

    @param kwargs: Arbitrary key/value pairs to serialize as JSON.
    @return None
    """
    print(json.dumps(kwargs), flush=True)
