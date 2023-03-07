"""
Module that contains start of the program
"""
import argparse
import logging
import os
from src.app import app, set_app, scheduler


def main():
    """
    Main function, parse arguments, create a controller and start Flask web server
    """
    parser = argparse.ArgumentParser(description="Gridpack Service")
    parser.add_argument("--debug", help="Debug mode", action="store_true")
    parser.add_argument("--port", help="Port, default is 8001", default=8001)
    parser.add_argument("--host", help="Host IP, default is 0.0.0.0", default="0.0.0.0")
    parser.add_argument(
        "--dev", help="Run a DEV version of gridpack service", action="store_true"
    )
    args = vars(parser.parse_args())
    debug = args.get("debug", False)
    dev = args.get("dev", False)
    logger = logging.getLogger()

    logger.info("%s: Set Flask application configuration", __name__)
    set_app(mode=dev, debug=debug)

    port = args.get("port")
    host = args.get("host")
    logger.info("Will run on %s:%s", host, port)
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
