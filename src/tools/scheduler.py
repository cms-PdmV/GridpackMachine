import threading
import logging


class Scheduler:
    def __init__(self):
        self.threads = []
        self.running = False

    def add_job(self, func, interval, *args, **kwargs):
        e = threading.Event()

        def thread_function():
            while self.running:
                try:
                    func(*args, **kwargs)
                except Exception as ex:
                    logger = logging.getLogger()
                    logger.error("Exception in scheduler %s", ex, exc_info=True)

                e.wait(timeout=interval)

        thread = threading.Thread(target=thread_function, args=[])
        self.threads.append((e, thread))
        return thread

    def start(self):
        self.running = True
        for _, t in self.threads:
            t.start()

    def stop(self):
        self.running = False
        for e, t in self.threads:
            e.set()
            t.join()

    def notify(self):
        for e, _ in self.threads:
            e.set()
            e.clear()
