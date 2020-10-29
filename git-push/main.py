import configparser
import json
import os

from git import Repo

from prefect import task, Flow
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

@task
def load_config() -> dict:

    config = configparser.ConfigParser()
    config.read("/home/sidhu/prefect-home-automation/git-push/conf.cfg")

    return config

@task
def commit_and_push(path: str):

    repo = Repo(path)

    repo.git.add(all=True)
    repo.index.commit("Automatic commit done by Sidhu automation.")

    if "blog" in path:
        repo.remote("origin").push("master")
    else:
        repo.remote("origin").push("develop")

@task
def get_projects(config):

    base_path = config["general"]["basepath"]
    projects = json.loads(config["general"]["repos"])

    project_paths = [os.path.join(base_path, project) for project in projects]

    return project_paths

schedule = Schedule(clocks=[CronClock("0 4 * * *")])

with Flow("Git-Push", schedule=schedule) as flow:

    # Load config
    config = load_config()

    # Get projects to sync
    projects = get_projects(config)

    # Commit and push every project
    commit_and_push.map(projects)

flow.register(project_name="Git-Sync")
