"""Fetching raw repo traffic data."""

import json
import os
from argparse import ArgumentParser, Namespace
from time import time
from typing import Any, Union

from github import Github
from github.Clones import Clones
from github.Repository import Repository
from github.View import View


def parse_args() -> Namespace:
    """Parse command-line arguments.

    Returns:
        Namespace: Command-line arguments.
    """
    args = ArgumentParser()
    args.add_argument('--config', type=str, default='data/config.json')
    return args.parse_args()


def day_to_json(obj: Union[Clones, View]) -> dict[str, Any]:
    """Dump one day of a timeseries to JSON.

    Args:
        obj (Clones | View): The clones or views statistics for one day.

    Returns:
        dict[str, Any]: JSON dict of the same info.
    """
    s = str(obj.timestamp)
    date = s[:s.index(' ')]
    return {
        'date': date,
        'count': obj.count,
        'uniques': obj.uniques,
    }


def traffic_to_json(obj: dict[str, Any], key: str) -> dict[str, Any]:
    """Dump clones or views timeseries data to JSON.

    Args:
        obj (dict[str, Any]): The raw clones or views dict.
        key (str): Whether clones or views.

    Returns:
        dict[str, Any]: JSON dict of the same info.
    """
    return {
        'count': obj['count'],
        'uniques': obj['uniques'],
        'daily': list(map(day_to_json, obj[key])),
    }


def clones_traffic_to_json(repo: Repository) -> dict[str, Any]:
    """Dump repo clones traffic to JSON.

    Args:
        repo (Repository): The repo object.

    Returns:
        dict[str, Any]: JSON dict of the same info.
    """
    obj = repo.get_clones_traffic()
    return traffic_to_json(obj, 'clones')


def views_traffic_to_json(repo: Repository) -> dict[str, Any]:
    """Dump repo views traffic to JSON.

    Args:
        repo (Repository): The repo object.

    Returns:
        dict[str, Any]: JSON dict of the same info.
    """
    obj = repo.get_views_traffic()
    return traffic_to_json(obj, 'views')


def repo_to_json(repo: Repository) -> dict[str, Any]:
    """Dump repo traffic to JSON.

    Args:
        repo (Repository): The repo object.

    Returns:
        dict[str, Any]: JSON dict of the fields we want.
    """
    clones = clones_traffic_to_json(repo)
    views = views_traffic_to_json(repo)
    return {
        'time': time(),
        'repo': repo.full_name,
        'stars': repo.stargazers_count,
        'watchers': repo.subscribers_count,
        'forks': repo.forks_count,
        'clones': clones,
        'views': views,
    }


def fetch_repos(github: Github, repo_names: list[str], filename: str) -> None:
    """Fetch traffic data for the given repos, saving to file.

    The data is stored as JSON per line. There is one linle per repo per fetch.

    Args:
        github (Github): Github API instance.
        repo_names (str): Names of the repos to fetch traffic for.
        filename (str): Where to save crawled data.
    """
    dirname = os.path.dirname(filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    with open(filename, 'a') as out:
        for repo_name in repo_names:
            repo = github.get_repo(repo_name)
            obj = repo_to_json(repo)
            line = json.dumps(obj, sort_keys=True) + '\n'
            out.write(line)


def main(args: Namespace) -> None:
    """Main method.

    Args:
        args (Namespace): Command-line arguments.
    """
    config = json.load(open(args.config))
    token = config['token']
    github = Github(token)
    repo_names = config['repos']
    filename = config['raw']
    fetch_repos(github, repo_names, filename)


if __name__ == '__main__':
    main(parse_args())
