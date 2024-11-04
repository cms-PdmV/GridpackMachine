"""
This module wraps some auxiliary functions
to check job statuses in HTCondor, query GitHub,
handle strings, pulling repositories, among others.
"""

import os
import json
import logging
import subprocess
import pathlib
import re
import datetime
import importlib.util
from typing import Optional
from os import listdir
from os.path import isdir
from os.path import join as path_join
from src.tools.connection_wrapper import ConnectionWrapper
from src.tools.ssh_executor import SSHExecutor
from environment import PUBLIC_STREAM_FOLDER, GRIDPACK_FILES_PATH


CONDOR_STATUS = {
    "0": "UNEXPLAINED",
    "1": "IDLE",
    "2": "RUN",
    "3": "REMOVED",
    "4": "DONE",
    "5": "HOLD",
    "6": "SUBMISSION ERROR",
}


BRANCHES_CACHE = {}
CAMPAIGNS_CACHE = {}
CARDS_CACHE = {}
TUNES_CACHE = []
UNABLE_CHECK_FILES = re.compile(r"ls: cannot")
EMPTY_SPACE = " "


def clean_split(string, separator=",", maxsplit=-1):
    """
    Split a string by separator and collect only non-empty values
    """
    return [x.strip() for x in string.split(separator, maxsplit) if x.strip()]


def run_command(command):
    """
    Run a bash command and return stdout, stderr and exit code
    """
    if isinstance(command, list):
        command = "\n".join(command)

    with subprocess.Popen(command, shell=True, stdout=subprocess.PIPE) as process:
        stdout, stderr = process.communicate()
        code = process.returncode
        stdout = stdout.decode("utf-8") if stdout is not None else None
        stderr = stderr.decode("utf-8") if stderr is not None else None
        return stdout, stderr, code


def get_jobs_in_condor(ssh=None):
    """
    Fetch jobs from HTCondor
    Return a dictionary where key is job id and value is status (IDLE, RUN, ...)
    """
    cmd = "condor_q -af:h ClusterId JobStatus Cmd"
    logger = logging.getLogger()

    if ssh:
        stdout, stderr, exit_code = ssh.execute_command(cmd)
    else:
        stdout, stderr, exit_code = run_command(cmd)

    if exit_code != 0:
        logger.error("HTCondor is failing (%s):\n%s\n%s", exit_code, stdout, stderr)
        raise Exception(f"HTCondor status check returned {exit_code}")

    lines = stdout.split("\n")
    if not lines or "ClusterId JobStatus Cmd" not in lines[0]:
        logger.error("HTCondor is failing (%s):\n%s\n%s", exit_code, stdout, stderr)
        raise Exception("HTCondor is not working")

    jobs_dict = {}
    lines = lines[1:]
    for line in lines:
        if "GRIDPACK_" not in line:
            continue

        columns = line.split()
        if not columns:
            continue

        job_id = columns[0]
        job_status = columns[1]
        jobs_dict[job_id] = CONDOR_STATUS.get(job_status, "REMOVED")

    logger.info("Job status in HTCondor: %s", json.dumps(jobs_dict))
    return jobs_dict


def get_latest_log_output_in_condor(gridpack, ssh=None):
    """
    Scan the job output log and stores it into the filesystem
    """
    logger = logging.getLogger()
    condor_id = gridpack.get_condor_id()
    public_stream_folder = PUBLIC_STREAM_FOLDER
    generation_log_file = (
        f"{public_stream_folder}/GRIDPACK_GENERATION_{gridpack.get_id()}.log"
    )

    if condor_id == 0:
        raise AssertionError(
            (
                "This Gridpack should be already submitted and running. "
                "Its ID must not be zero"
            )
        )

    cmd = f"condor_ssh_to_job {condor_id} 'cat _condor_stdout' > {generation_log_file}"
    if ssh:
        stdout, stderr, exit_code = ssh.execute_command(cmd)
    else:
        stdout, stderr, exit_code = run_command(cmd)

    if exit_code != 0:
        logger.error("HTCondor is failing (%s):\n%s\n%s", exit_code, stdout, stderr)
        raise Exception(f"HTCondor status check returned {exit_code}")


def _get_git_branches(
    repository: str, max_pages: int = 25, branches_per_page: int = 30
) -> list[str]:
    """
    Return a list with all branches available in a GitHub repository up to a maximum default page.

    Args:
        repository: Repository name in GitHub.
        max_pages: Maximum number of pages to scan.
        branches_per_page: Number of branches to retrieve per page.

    Returns:
        All the branches' names available up to the limit.
    """
    logger = logging.getLogger()
    logger.debug(
        "Scanning a maximum of %d branches from repo: %s",
        max_pages * branches_per_page,
        repository,
    )
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    all_branches = []

    with ConnectionWrapper("https://api.github.com") as conn:
        for page in range(1, max_pages + 1):
            response = conn.api(
                "GET",
                f"/repos/{repository}/branches?per_page={branches_per_page}&page={page}",
                headers=headers,
            )
            response = json.loads(response.decode("utf-8"))
            branches = [b["name"] for b in response if b.get("name")]
            logger.debug(
                "Found %s branches in %s on page %s", len(branches), repository, page
            )
            if not branches:
                break

            all_branches += branches

    logger.debug(
        "Found %s branches in total for repo: %s", len(all_branches), repository
    )
    return all_branches


def get_git_branches(repository, cache=True):
    """
    Return list of branches in the repostory
    """
    if not cache or repository not in BRANCHES_CACHE:
        branches = _get_git_branches(repository=repository)
        BRANCHES_CACHE[repository] = branches

    return BRANCHES_CACHE[repository]


def pull_git_repository(path: str, expected_remote: str) -> None:
    """
    Updates the content related to the GridpackFiles
    repository.

    Args:
        path (str): Absolute path to the repository folder.
        expected_remote (str): Expected remote origin for the repository.

    Raises:
        RuntimeError: If there is an issue executing git instructions or
            if the remote origin is not the expected.
    """
    logger = logging.getLogger()
    logger.debug("Pulling a repository located at: %s", path)

    # Check the repository stored is the expected
    stdout, stderr, code = run_command(
        [
            f"cd {path}",
            f"git config --global --add safe.directory {path}",
            "git remote get-url origin",
        ]
    )
    if code != 0:
        raise RuntimeError(
            (
                "Unable to check repository origin. "
                "Please see the detailed stacktrace. "
                f"Standard output: {stdout} - "
                f"Standard error: {stderr}"
            )
        )
    if stdout.strip() != expected_remote:
        raise RuntimeError(
            f"The remote origin doesn't match. Received: {stdout} - Expected: {expected_remote}"
        )

    # Perform the update
    stdout, stderr, code = run_command(
        [
            f"cd {path}",
            f"git config --global --add safe.directory {path}",
            "git checkout .",
            "git pull",
        ]
    )
    if code != 0:
        raise RuntimeError(
            (
                "Unable to update the repository. "
                "Please see the detailed stacktrace. "
                f"Standard output: {stdout} - "
                f"Standard error: {stderr}"
            )
        )


def get_available_campaigns(cache=True):
    """
    Get campaigns and campaign templates
    """
    global CAMPAIGNS_CACHE  # pylint: disable=global-statement
    if not cache or not CAMPAIGNS_CACHE:
        CAMPAIGNS_CACHE = {}
        campaigns_dir = os.path.join(GRIDPACK_FILES_PATH, "Campaigns")
        campaigns = [
            c for c in listdir(campaigns_dir) if isdir(path_join(campaigns_dir, c))
        ]
        for name in campaigns:
            campaign_path = os.path.join(campaigns_dir, name)
            generators = [
                g for g in listdir(campaign_path) if isdir(path_join(campaign_path, g))
            ]
            with open(
                path_join(campaign_path, f"{name}.json"), encoding="utf-8"
            ) as campaign_json:
                campaign_dict = json.load(campaign_json)

            CAMPAIGNS_CACHE[name] = {
                "generators": generators,
                "tune": campaign_dict.get("tune", ""),
            }

    return CAMPAIGNS_CACHE


def get_available_cards(cache=True):
    """
    Get generators, processes and datasets
    """
    global CARDS_CACHE  # pylint: disable=global-statement
    if not cache or not CARDS_CACHE:
        CARDS_CACHE = {}
        cards_dir = os.path.join(GRIDPACK_FILES_PATH, "Cards")
        generators = [c for c in listdir(cards_dir) if isdir(path_join(cards_dir, c))]
        for generator in generators:
            generator_path = os.path.join(cards_dir, generator)
            processes = [
                p
                for p in listdir(generator_path)
                if isdir(path_join(generator_path, p))
            ]
            for process in processes:
                process_path = os.path.join(generator_path, process)
                datasets = [
                    d
                    for d in listdir(process_path)
                    if isdir(path_join(process_path, d))
                ]
                CARDS_CACHE.setdefault(generator, {})[process] = datasets

    return CARDS_CACHE


def get_available_tunes(cache=True):
    """
    Get list of available tunes
    """
    global TUNES_CACHE  # pylint: disable=global-statement
    if not cache or not TUNES_CACHE:
        imports_path = os.path.join(GRIDPACK_FILES_PATH, "Fragments", "imports.json")
        if not os.path.isfile(imports_path):
            TUNES_CACHE = []
            return TUNES_CACHE

        with open(imports_path, encoding="utf-8") as imports_file:
            imports = json.load(imports_file)

        TUNES_CACHE = sorted(list(set(imports.get("tune", []))))

    return TUNES_CACHE


def get_indentation(phrase, text):
    """
    Return indentation (number of spaces in the beginning of the line) for the
    first line in text that has a "phrase"
    """
    lines = [l for l in text.split("\n") if phrase in l]
    if not lines:
        return 0

    line = lines[0]
    return len(line) - len(line.lstrip())


def include_gridpack_ids(gridpack_id: str, effective_gridpack_id: str, content: str):
    """
    For content like text files, include the Gridpack ID
    that is related to it.
    """
    gridpack_label: str = (
        f"# Gridpack that reused this artifact: {gridpack_id}"
        if gridpack_id != effective_gridpack_id
        else ""
    )
    labeled_content: str = f"# Related to Gridpack ID: {effective_gridpack_id}\n"
    if gridpack_label:
        labeled_content += f"{gridpack_label}\n"
    labeled_content += "\n"
    labeled_content += content

    return labeled_content


def get_module_path(module: str) -> Optional[pathlib.Path]:
    """
    Retrieves the absolute path for the desired module if exists.

    Args:
        module (str): Module name to look.
    Returns:
        pathlib.Path | None: Module's path if it exists.
    """
    spec = importlib.util.find_spec(module)
    if not spec:
        return None
    return pathlib.Path(spec.origin)


def check_append_path(root: str, relative: str) -> pathlib.Path:
    """
    Check that two provided paths are valid and
    that they are absolute or relative depending on its case.
    Returns its concatenation.

    Args:
        root (str): Root absolute path
        relative (str): Relative path to be appended.

    Returns:
        pathlib.Path: Concatenation of both paths

    Raises:
        ValueError: If the provided paths are not absolute or relative
            depending on its case.
    """
    relative_path = pathlib.Path(relative)
    root_path = pathlib.Path(root)
    if relative_path.is_absolute():
        raise ValueError(
            f"Please provide a relative path - Relative path provided: {relative_path}"
        )
    if not root_path.is_absolute():
        raise ValueError(
            f"Please provide an absolute path - Absolute path provided: {root_path}"
        )
    return root_path / relative_path


def retrieve_all_files_available(folders: list, ssh_session: SSHExecutor) -> dict:
    """
    For a given group of folders, retrieve all the files available
    into them, their absolute path and the last modification date.
    Sort all the elements based on this date.

    Args:
        folders (list[pathlib.Path]): List of folders to check
        ssh_session (SSHExecutor): SSH session to a remote host
            where all the folders are reachable.

    Returns
        dict: Folder and files available ordered by last modification time
    """

    def __parse_stdout(stdout_content: str):
        lines = [l for l in stdout_content.splitlines() if l]
        content = []
        for file_data in lines:
            data = file_data.split(EMPTY_SPACE)
            record = (
                datetime.datetime.fromtimestamp(int(data[0]), datetime.timezone.utc),
                data[1],
            )
            content.append(record)
        content.sort(key=lambda e: e[0], reverse=True)
        return content

    result: dict = {}
    only_files: str = "grep '^[^d|p|total]'"
    only_date_name: str = "awk '{print $6,$7}'"
    for folder_metadata in folders:
        # Retrieve only the date and the name
        folder_path = str(folder_metadata.parent)
        regex_pattern = f"^{folder_metadata.name}"
        file_filter = re.compile(regex_pattern)

        scan_command: str = (
            f"ls -l --time-style=+%s '{folder_path}' | "
            f"{only_files} | "
            f"{only_date_name}"
        )
        stdout, stderr, exit_code = ssh_session.execute_command(scan_command)
        if exit_code != 0 or UNABLE_CHECK_FILES.findall(stderr):
            continue

        files_data = __parse_stdout(stdout)
        files_parsed_content = []
        for file in files_data:
            file_name = file[1]
            last_date_unix = file[0]
            if file_filter.match(file_name):
                files_content = {}
                files_content["file_name"] = file_name
                files_content["file_path"] = check_append_path(
                    root=folder_path, relative=file[1]
                )
                files_content["last_modification_date"] = last_date_unix
                files_parsed_content.append(files_content)

        result[folder_path] = files_parsed_content

    return result


def wrap_into_singularity(
    script_name: str,
    content: list,
    desired_os: str,
) -> list:
    """
    Wraps a script to run via singularity using cmssw containers.

    Args:
        script_name (str): Name of the subscript that groups the given instructions
            to be executed via singularity.
        content (list[str]): Instructions to execute via singularity.
        desired_os (str): CERN OS tag related to the desired OS to use.

    Returns:
        list[str]: Instructions provided with some extra code to handle
            singularity.
    """
    # CMSSW container folder
    container_path = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw"

    # Wrapper instructions
    wrapper_placeholder = "EndOfSingularityWrapper"
    wrapper_header = ["", f"cat <<'{wrapper_placeholder}' > {script_name}"]
    # Execution command
    singularity_run = (
        "singularity run "
        "-B /afs -B /cvmfs -B /etc/grid-security -B /etc/pki/ca-trust "
        "--no-home /cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/$CONTAINER_NAME "
        f"$(echo $(pwd)/{script_name})"
    )
    wrapper_close = [
        "",
        f"# End of {script_name} file",
        f"{wrapper_placeholder}",
        "",
        f"# Make {script_name} file executable",
        f"chmod +x {script_name}",
        "",
        "# Check the proper tag for the architectue",
        f'if [ -e "{container_path}/{desired_os}:amd64" ]; then',
        f'  CONTAINER_NAME="{desired_os}:amd64"',
        f'elif [ -e "{container_path}/{desired_os}:x86_64" ]; then',
        f'  CONTAINER_NAME="{desired_os}:x86_64"',
        "else",
        f'  echo "Could not find amd64 or x86_64 for {desired_os}"',
        "  exit 1",
        "fi",
        "",
        "# Running into a singularity container",
        'export SINGULARITY_CACHEDIR="/tmp/$(whoami)/singularity"',
        f"{singularity_run}",
        "",
    ]

    # Wrap all together
    result = []
    result += wrapper_header
    result += content.copy()
    result += wrapper_close
    return result
