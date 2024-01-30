"""
Module that contains start of the program, tick scheduler and web APIs
"""
import logging
import json
import os
from flask import Flask, send_file, request, make_response
from flask_restful import Api
from scheduler import Scheduler
from controller import Controller
from gridpack import Gridpack
from database import Database
from environment import (
    GEN_REPOSITORY,
    MONGO_DB_HOST,
    MONGO_DB_PASSWORD,
    MONGO_DB_PORT,
    MONGO_DB_USER,
    TICK_INTERVAL,
    REPOSITORY_UPDATE_INTERVAL,
    DEBUG,
    HOST,
    PORT
)
from user import User
from utils import include_gridpack_ids


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
    if data and isinstance(data, (dict, list)):
        data = json.dumps(data, indent=1, sort_keys=True)

    resp = make_response(data, code)
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
                        'gen_repository': GEN_REPOSITORY,
                        'job_cores': controller.job_cores,
                        'job_memory': controller.job_memory})


@app.route('/api/mcm', methods=['POST'])
def force_mcm_request():
    """
    This endpoint forces the creation for a request in McM
    for a completed Gridpack
    """
    if not is_user_authorized():
        return output_text({'message': 'Unauthorized'}, code=403)
    
    gridpack_id: str = request.args.get('gridpack_id', '')
    if not gridpack_id:
        return output_text(
            {'message': 'Please choose a Gridpack via request parameter "gridpack_id"'},
            code=400
        )
    
    force_status = controller.force_request_for_gridpack(gridpack_id=gridpack_id)
    if isinstance(force_status, dict):
        return output_text(data=force_status, code=400)
    
    return output_text({'message': 'Request forced for %s' % gridpack_id})

@app.route('/api/create', methods=['PUT'])
def create_gridpack():
    """
    API to create a gridpack or list of gridpacks
    """
    if not is_user_authorized():
        return output_text({'message': 'Unauthorized'}, code=403)

    logging.info('DATA %s', request.data.decode('utf-8'))
    gridpacks = json.loads(request.data.decode('utf-8'))
    if not isinstance(gridpacks, list):
        gridpacks = [gridpacks]

    gridpack_ids = []
    for gridpack_dict in gridpacks:
        try:
            gridpack = Gridpack.make(gridpack_dict)
        except Exception as ex:
            return output_text({'message': str(ex)}, code=400)

        error = gridpack.validate()
        if error:
            return output_text({'message': error}, code=400)

        gridpack_id = controller.create(gridpack)
        gridpack_ids.append(gridpack_id)

    scheduler.notify()
    return output_text({'message': gridpack_ids})


@app.route('/api/create_approve', methods=['PUT'])
def create_approve_gridpack():
    """
    API to create and approve a gridpack or list of gridpacks
    """
    if not is_user_authorized():
        return output_text({'message': 'Unauthorized'}, code=403)

    logging.info('DATA %s', request.data.decode('utf-8'))
    gridpacks = json.loads(request.data.decode('utf-8'))
    if not isinstance(gridpacks, list):
        gridpacks = [gridpacks]

    gridpack_ids = []
    for gridpack_dict in gridpacks:
        try:
            gridpack = Gridpack.make(gridpack_dict)
        except Exception as ex:
            return output_text({'message': str(ex)}, code=400)

        error = gridpack.validate()
        if error:
            return output_text({'message': error}, code=400)

        gridpack_id = controller.create(gridpack)
        _ = controller.approve(gridpack_id)
        gridpack_ids.append(gridpack_id)

    scheduler.notify()
    return output_text({'message': gridpack_ids})


@app.route('/api/approve', methods=['POST'])
def approve_gridpack():
    """
    API to approve a gridpack
    """
    if not is_user_authorized():
        return output_text({'message': 'Unauthorized'}, code=403)

    gridpack_dict = json.loads(request.data.decode('utf-8'))
    gridpack_id = gridpack_dict.get('_id')
    if not gridpack_id:
        return output_text({'message': 'No ID'})

    controller.approve(gridpack_id)
    scheduler.notify()
    return output_text({'message': 'OK'})


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
    scheduler.notify()
    return output_text({'message': 'OK'})


@app.route('/api/create_request', methods=['POST'])
def create_request():
    """
    API to create a request in McM
    """
    if not is_user_authorized():
        return output_text({'message': 'Unauthorized'}, code=403)

    gridpack_dict = json.loads(request.data.decode('utf-8'))
    gridpack_id = gridpack_dict.get('_id')
    if not gridpack_id:
        return output_text({'message': 'No ID'})

    controller.create_request(gridpack_id)
    scheduler.notify()
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
    scheduler.notify()
    return output_text({'message': 'OK'})


@app.route('/api/get')
def get_gridpacks():
    """
    API to fetch gridpacks from database
    """
    database = Database()
    gridpacks, count = database.get_gridpacks()
    return output_text([gridpacks, count])


@app.route('/api/get_fragment/<string:gridpack_id>')
def get_fragment(gridpack_id):
    """
    API to get gridpack's fragment
    """
    database = Database()
    gridpack_json = database.get_gridpack(gridpack_id)
    if not gridpack_json:
        return output_text({'message': 'Gridpack not found'}, code=404)

    gridpack = Gridpack.make(gridpack_json)
    fragment, _ = controller.get_fragment(gridpack)

    return output_text(fragment, headers={'Content-Type': 'text/plain'})


@app.route('/api/get_run_card/<string:gridpack_id>')
def get_run_card(gridpack_id):
    """
    API to get gridpack's run card
    """
    try:
        gridpack: Gridpack = controller.get_original_gridpack(
            gridpack_id=gridpack_id
        )
        content = include_gridpack_ids(
            gridpack_id=gridpack_id,
            effective_gridpack_id=gridpack.get_id(),
            content=gridpack.get_run_card()
        )
        return output_text(
            content, 
            headers={'Content-Type': 'text/plain'}
        )
    except ValueError:
        return output_text({'message': 'Gridpack not found'}, code=404)
    except AssertionError as a:
        return output_text({'message': str(a)}, code=400)
    except Exception:
        logging.error(
            "Unable to retrieve `run_card` for Gridpack: %s",
            gridpack,
            exc_info=True
        )
        return output_text({'message': 'Unable to retrieve the element'}, code=400)


@app.route('/api/get_customize_card/<string:gridpack_id>')
def get_customize_card(gridpack_id):
    """
    API to get gridpack's fragment
    """
    try:
        gridpack: Gridpack = controller.get_original_gridpack(
            gridpack_id=gridpack_id
        )
        content = include_gridpack_ids(
            gridpack_id=gridpack_id,
            effective_gridpack_id=gridpack.get_id(),
            content=gridpack.get_customize_card()
        )
        return output_text(
            content, 
            headers={'Content-Type': 'text/plain'}
        )
    except ValueError:
        return output_text({'message': 'Gridpack not found'}, code=404)
    except AssertionError as a:
        return output_text({'message': str(a)}, code=400)
    except Exception:
        logging.error(
            "Unable to retrieve `customize_card` for Gridpack: %s",
            gridpack,
            exc_info=True
        )
        return output_text({'message': 'Unable to retrieve the element'}, code=400)


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


def set_scheduler():
    """
    Set the automatic jobs for the application scheduler.
    """
    logger = logging.getLogger()
    logger.info('Adding machine tick with interval %ss', TICK_INTERVAL)
    scheduler.add_job(tick, TICK_INTERVAL)
    logger.info('Adding repository update with interval %ss', REPOSITORY_UPDATE_INTERVAL)
    scheduler.add_job(tick_repository, REPOSITORY_UPDATE_INTERVAL)


def set_app():
    """
    Set the required configuration to start.
    """
    global controller
    setup_console_logging(DEBUG)
    Database.set_credentials(MONGO_DB_USER, MONGO_DB_PASSWORD)
    Database.set_host_port(MONGO_DB_HOST, MONGO_DB_PORT)
    controller = Controller()


def main():
    """
    Main function, parse arguments, create a controller and start Flask web server
    """
    set_app()
    logger = logging.getLogger()

    # Scheduler config - Flask
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        set_scheduler()
        scheduler.start()
    
    logger.info('Will run on %s:%s', HOST, PORT)
    try:
        app.run(host=HOST,
                port=PORT,
                debug=DEBUG,
                threaded=True)
    finally:
        scheduler.stop()


if __name__ == '__main__':
    main()
