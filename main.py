"""
Module that contains start of the program
"""
import logging
import os
from app import app, set_app, scheduler


def main():
    """
    Main function,
    Start Flask web server
    """
    logger = logging.getLogger()
    logger.info("%s: Set Flask application configuration", __name__)
    host, port, _, debug = set_app()
    if os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        # Do only once, before the reloader
        pid = os.getpid()
        logger.info("PID: %s", pid)
        with open("gridpack.pid", "w", encoding="utf-8") as pid_file:
            pid_file.write(str(pid))

    logger.info("Starting scheduler")
    scheduler.start()
    try:
        app.run(host=host, port=port, debug=debug, threaded=True)
    finally:
        scheduler.stop()


if __name__ == "__main__":
    main()
