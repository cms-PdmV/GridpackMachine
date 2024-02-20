"""
This module parses some configuration variables from
the runtime environment to use them in different sections
from this application

Attributes:
    TICK_INTERVAL (int): Interval window (in seconds) to perform an automatic
        internal tick. This processes the submission queue,
        sending new jobs or retriving its status.
    REPOSITORY_UPDATE_INTERVAL (int): Interval window (in seconds) to perform an automatic
        update for the GridpackFiles repository. This repository has all the configs
        required to generate Gridpacks.
    SERVICE_URL (str): This is the url for the GridpackMachine application.
        For example: "https://cms-pdmv-prod.web.cern.ch/gridpack"
        It is used to include the application's url into email notifications.
    SUBMISSION_HOST (str): This is the server where this application will open an SSH session
        to execute tasks like submitting jobs through HTCondor or running McM submission scripts.
        For example: "lxplus.cern.ch".
    SERVICE_ACCOUNT_USERNAME (str): Username to authenticate to `SUBMISSION_HOST`
    SERVICE_ACCOUNT_PASSWORD (str): Password to authenticate to `SUBMISSION_HOST`
    REMOTE_DIRECTORY (str): This is the absolute folder path (into AFS or EOS) that stores
        all the required bundle files to submit a HTCondor job.
    TICKETS_DIRECTORY (str): This is the absolute folder path (into AFS or EOS) that stores
        all the required bundle files to submit a MC request into McM based
        on a created Gridpack.
    GEN_REPOSITORY (str): This is the name of the CMS GEN repository.
        For the purpose of this application, it is used to retrieve its available
        branches and this data is used in the Gridpack request validation.
        CMS GEN repository is available at: https://github.com/cms-sw/genproductions.
    REPOSITORY_TICK_PAUSE (int): Minimum interval window (in seconds) to wait before
        performing an internal tick.
    AUTHORIZED (str): Authorized roles enabled to submit Gridpack jobs.
        The format for this field is the following: <ROLE_1>,<ROLE_2>,...,<ROLE_N>
    GRIDPACK_DIRECTORY (str): This is the absolute folder path (into AFS or EOS) that stores
        the produced Gridpack after the HTCondor job finishes. 
        Please remember this value is overwritten into a special
        path in /eos/ for the production environment.
    GRIDPACK_FILES_PATH (str): This is the absolute folder path (into AFS or EOS)
        where the GridpackFiles repository is located.
    GRIDPACK_FILES_REPOSITORY (str): Remote origin for the GridpackFiles repository.
        GridpackFiles repository: https://github.com/cms-PdmV/GridpackFiles.git
    PUBLIC_STREAM_FOLDER (str): This is the absolute folder path (into AFS or EOS),
        available for CMS users, where the Gridpack creation logs for running HTCondor jobs
        are transmitted.
    USE_HTCONDOR_CMS_CAF (bool): If enabled, CMS CAF HTCondor
        nodes are going to be used to submit Gridpack jobs.
        More details are available at:
        https://batchdocs.web.cern.ch/local/specifics/CMS_CAF_tzero.html
    PRODUCTION (bool): Enables the application to run in production.
    EMAIL_AUTH (bool): Send credentials when setting up the SMTP client.
    MONGO_DB_HOST (str): MongoDB host for opening a client session.
    MONGO_DB_PORT (int): MongoDB port for opening a client session.
    MONGO_DB_USER (str): MongoDB user to authenticate a new client session.
    MONGO_DB_PASSWORD (str): MongoDB password to authenticate a new client session.
    HOST (str): Web server listening hostname.
    PORT (int): Web server port.
    DEBUG (bool): Enables the DEBUG mode for the logger.
"""

import os
import inspect

# GridpackMachine application
TICK_INTERVAL: int = int(os.getenv("TICK_INTERVAL", "600"))
REPOSITORY_UPDATE_INTERVAL: int = int(os.getenv("REPOSITORY_UPDATE_INTERVAL", "1800"))
SERVICE_URL: str = os.getenv("SERVICE_URL", "")
SUBMISSION_HOST: str = os.getenv("SUBMISSION_HOST", "")
SERVICE_ACCOUNT_USERNAME: str = os.getenv("SERVICE_ACCOUNT_USERNAME", "")
SERVICE_ACCOUNT_PASSWORD: str = os.getenv("SERVICE_ACCOUNT_PASSWORD", "")
REMOTE_DIRECTORY: str = os.getenv("REMOTE_DIRECTORY", "")
TICKETS_DIRECTORY: str = os.getenv("TICKETS_DIRECTORY", "")
GEN_REPOSITORY: str = os.getenv("GEN_REPOSITORY", "cms-sw/genproductions")
REPOSITORY_TICK_PAUSE: int = int(os.getenv("REPOSITORY_TICK_PAUSE", "60"))
AUTHORIZED: str = os.getenv("AUTHORIZED", "")
GRIDPACK_DIRECTORY: str = os.getenv("GRIDPACK_DIRECTORY", "")
GRIDPACK_FILES_PATH: str = os.getenv("GRIDPACK_FILES_PATH", "")
GRIDPACK_FILES_REPOSITORY: str = os.getenv(
    "GRIDPACK_FILES_REPOSITORY", "https://github.com/cms-PdmV/GridpackFiles.git"
)
PUBLIC_STREAM_FOLDER: str = os.getenv("PUBLIC_STREAM_FOLDER", "")
USE_HTCONDOR_CMS_CAF: bool = bool(os.getenv("USE_HTCONDOR_CMS_CAF"))
PRODUCTION: bool = bool(os.getenv("PRODUCTION"))
EMAIL_AUTH: bool = bool(os.getenv("EMAIL_AUTH"))

# MongoDB database
MONGO_DB_HOST: str = os.getenv("MONGO_DB_HOST", "")
MONGO_DB_PORT: int = int(os.getenv("MONGO_DB_PORT", "27017"))
MONGO_DB_USER: str = os.getenv("MONGO_DB_USER", "")
MONGO_DB_PASSWORD: str = os.getenv("MONGO_DB_PASSWORD", "")

# Web server settings
HOST: str = os.getenv("HOST", "0.0.0.0")
PORT: int = int(os.getenv("PORT", "8000"))
DEBUG: bool = bool(os.getenv("DEBUG"))

# Check that all environment variables are provided
missing_environment_variables: dict[str, str] = {
    k: v
    for k, v in globals().items()
    if not k.startswith("__")
    and not inspect.ismodule(v)
    and not isinstance(v, bool)
    and not v
}

if missing_environment_variables:
    msg: str = (
        "There are some environment variables "
        "required to be set before running this application\n"
        "Please set the following values via environment variables\n"
        "For more details, please see the description available into `environment.py` module\n"
        f"{list(missing_environment_variables.keys())}"
    )
    raise RuntimeError(msg)
