import threading, signal, sys
from app.db import init_db
from app.consumer import run_consumer
from app.dispatcher import run_outbox_dispatcher
from app.health import start_health_server

def main():
    init_db()
    stop_flag = threading.Event()
    def _sig(*_): stop_flag.set()
    signal.signal(signal.SIGINT, _sig); signal.signal(signal.SIGTERM, _sig)

    # Start health server
    threading.Thread(target=start_health_server, args=(stop_flag,), daemon=True).start()

    t1 = threading.Thread(target=run_consumer, args=(stop_flag,), daemon=True)
    t2 = threading.Thread(target=run_outbox_dispatcher, args=(stop_flag,), daemon=True)
    t1.start(); t2.start()
    t1.join(); t2.join()

if __name__ == "__main__":
    main()
