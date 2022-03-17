"""
Module that contains start of the program, tick scheduler and web APIs
"""
import argparse
import logging
import json
import os
from flask import Flask, send_file, request, make_response
from flask_restful import Api
from scheduler import Scheduler
from controller import Controller
from gridpack import Gridpack
from database import Database
from config import Config
from user import User


app = Flask(__name__,
            static_folder="./frontend/static",
            template_folder="./frontend")
api = Api(app)
scheduler = Scheduler()
controller = None


@app.route('/')
def index_page():
    """
    Return index.html
    """
    return send_file('frontend/index.html')


def output_text(data, code=200, headers=None):
    """
    Makes a Flask response with a plain text encoded body
    """
    resp = make_response(json.dumps(data, indent=1, sort_keys=True), code)
    resp.headers.extend(headers or {})
    resp.headers['Content-Type'] = 'application/json'
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp


@app.route('/api/tick')
def api_tick():
    """
    API to trigger a controller tick
    """
    if not is_user_authorized():
        return output_text({'message': 'Unauthorized'}, code=403)

    tick()
    return output_text({'message': 'OK'})


@app.route('/api/tick_repository')
def api_tick_repository():
    """
    API to trigger a controller tick for repository data
    """
    if not is_user_authorized():
        return output_text({'message': 'Unauthorized'}, code=403)

    tick_repository()
    return output_text({'message': 'OK'})


@app.route('/api/user')
def user_info():
    """
    API for user info
    """
    return output_text(user_info_dict())


@app.route('/api/system_info')
def system_info():
    """
    API for system info
    """
    return output_text({'last_tick': controller.last_tick,
                        'last_repository_tick': controller.last_repository_tick,
                        'options': controller.repository_tree,
                        'gen_repository': Config.get('gen_repository')})


@app.route('/api/create', methods=['PUT'])
def create_gridpack():
    """
    API to create a gridpack
    """
    if not is_user_authorized():
        return output_text({'message': 'Unauthorized'}, code=403)

    logging.info('DATA %s', request.data.decode('utf-8'))
    gridpack_dict = json.loads(request.data.decode('utf-8'))
    gridpack = Gridpack(gridpack_dict)
    error = gridpack.validate()
    if error:
        return output_text({'message': error}, code=400)

    gridpack_id = controller.create(gridpack)
    tick()
    return output_text({'message': gridpack_id})


@app.route('/api/reset', methods=['POST'])
def reset_gridpack():
    """
    API to reset a gridpack
    """
    if not is_user_authorized():
        return output_text({'message': 'Unauthorized'}, code=403)

    gridpack_dict = json.loads(request.data.decode('utf-8'))
    gridpack_id = gridpack_dict.get('_id')
    if not gridpack_id:
        return output_text({'message': 'No ID'})

    controller.reset(gridpack_id)
    tick()
    return output_text({'message': 'OK'})


@app.route('/api/delete', methods=['DELETE'])
def delete_gridpack():
    """
    API to delete a gridpack
    """
    if not is_user_authorized():
        return output_text({'message': 'Unauthorized'}, code=403)

    gridpack_dict = json.loads(request.data.decode('utf-8'))
    gridpack_id = gridpack_dict.get('_id')
    if not gridpack_id:
        return output_text({'message': 'No ID'})
    
    controller.delete(gridpack_id)
    tick()
    return output_text({'message': 'OK'})


@app.route('/api/get')
def get_gridpacks():
    """
    API to fetch gridpacks from database
    """
    database = Database()
    return output_text(database.get_gridpacks())


def user_info_dict():
    """
    Get user name, login, email and authorized flag from request headers
    """
    return User().get_user_info()


def is_user_authorized():
    """
    Return whether user is a member of administrators e-group
    """
    return User().is_authorized()


def tick():
    """
    Trigger controller to perform a tick
    """
    if controller:
        controller.tick()

def tick_repository():
    """
    Trigger controller to perform a tick on repository data
    """
    if controller:
        controller.update_repository_tree()


def setup_console_logging(debug):
    """
    Setup logging to console
    """
    logging.basicConfig(format='[%(asctime)s][%(levelname)s] %(message)s',
                        level=logging.DEBUG if debug else logging.INFO)


def main():
    """
    Main function, parse arguments, create a controller and start Flask web server
    """
    parser = argparse.ArgumentParser(description='Gridpack Service')
    parser.add_argument('--debug',
                        help='Debug mode',
                        action='store_true')
    parser.add_argument('--port',
                        help='Port, default is 8001',
                        default=8001)
    parser.add_argument('--host',
                        help='Host IP, default is 0.0.0.0',
                        default='0.0.0.0')
    args = vars(parser.parse_args())
    debug = args.get('debug', False)
    setup_console_logging(debug)
    logger = logging.getLogger()
    Config.load('config.cfg', 'DEFAULT')
    database_auth = Config.get('database_auth')
    if database_auth:
        Database.set_credentials_file(database_auth)

    global controller
    controller = Controller()
    if not debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        tick_interval = int(Config.get('tick_interval', 600))
        repository_update_interval = int(Config.get('repository_update_interval', 1200))
        scheduler.add_job(tick, tick_interval)
        scheduler.add_job(tick_repository, repository_update_interval)

    scheduler.start()
    port = args.get('port')
    host = args.get('host')
    logger.info('Will run on %s:%s', host, port)
    if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        # Do only once, before the reloader
        pid = os.getpid()
        logger.info('PID: %s', pid)
        with open('gridpack.pid', 'w') as pid_file:
            pid_file.write(str(pid))

    try:
        app.run(host=host,
                port=port,
                debug=debug,
                threaded=True)
    finally:
        scheduler.stop()


if __name__ == '__main__':
    main()
