"""Processing fetched raw repo traffic into clean data."""

import json
import os
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from datetime import date, datetime, timedelta
from shutil import rmtree
from time import mktime
from typing import Any, Iterator


def parse_args() -> Namespace:
    """Parse command-line arguments.

    Returns:
        Namespace: Command-line arguments.
    """
    args = ArgumentParser()
    args.add_argument('--config', type=str, default='data/config.json')
    return args.parse_args()


def date_range(start: date, stop: date) -> Iterator[date]:
    """Iterate over a date range.

    Args:
        start (date): Start date.
        stop (date): Stop date.

    Returns:
        Iterator[date]: Each date, from start date to stop date.
    """
    count = (stop - start).days
    for i in range(count):
        yield start + timedelta(i)


def each_day(days: list[dict[str, Any]], fetch_time: float,
             window_days: int) -> Iterator[tuple[str, int, int]]:
    """Get each day of a timeseries.

    This is complicated by the fact that days with zero traffic are dropped.

    Args:
        days (list[dict[str, Any]]): The timeseries.
        fetch_time (float): Timestamp of when the data was pulled, also the end
            of the timeseries window.
        window_days (int): How many days the timeseries window extends at most.

    Returns:
        Iterator[tuple[str, int, int]]: Each day, including zero days.
    """
    zero_pair = 0, 0
    ymd2pair = defaultdict(lambda: zero_pair)
    for day in days:
        ymd = day['date']
        assert ymd not in ymd2pair
        ymd2pair[ymd] = day['count'], day['uniques']

    fetch_date = date.fromtimestamp(fetch_time)
    for ymd in date_range(fetch_date - timedelta(window_days - 2),
                          fetch_date - timedelta(1)):
        ymd = ymd.strftime('%Y-%m-%d')
        count, unique = ymd2pair[ymd]
        yield ymd, count, unique


def get_daily_stats(objs: list[dict[str, Any]], key: str, window_days: int) \
        -> tuple[list[str], list[int], list[int]]:
    """Collect daily stats (timeseries data).

    Args:
        objs (list[dict[str, Any]]): List of JSON dict per crawl.
        key (str): The name of the statistic.
        window_days (int): How many days the timeseries window extends at most.

    Returns:
        tuple[list[str], list[int], list[int]]: Timestamps, counts, uniques.
    """
    date2pair = {}
    for obj in objs:
        days = obj[key]['daily']
        fetch_time = obj['time']
        for date, count, unique in each_day(days, fetch_time, window_days):
            if date not in date2pair:
                date2pair[date] = count, unique
            else:
                assert date2pair[date] == (count, unique)

    dates = sorted(date2pair)
    pairs = []
    for date in dates:
        pair = date2pair[date]
        pairs.append(pair)

    counts, uniques = zip(*pairs)
    return dates, counts, uniques  # type: ignore


def get_point_stats(objs: list[dict[str, Any]], key: str) -> \
        tuple[list[float], list[int]]:
    """Collect point stats (non-timeseries data).

    Args:
        objs (list[dict[str, Any]]): List of JSON dict per crawl.
        key (str): The name of the statistic.

    Returns:
        tuple[list[float], list[int]]: Timestamps, values.
    """
    times = []
    values = []
    for obj in objs:
        times.append(obj['time'])
        values.append(obj[key])
    return times, values


def noon_time_from_date(date: str) -> float:
    """Get the timestamp for the exact middle of the given day.

    Args:
        date (str): Year-month-day.

    Returns:
        float: Timestamp of noon that day.
    """
    noon_secs = 12 * 60 * 60
    return mktime(datetime.strptime(date, '%Y-%m-%d').timetuple()) + noon_secs


def process_repo(repo: str, objs: list[dict[str, Any]],
                 proc_file: str) -> None:
    """Process one repo given the crawls of that repo.

    Args:
        repo (str): Repo name.
        objs (list[dict[str, Any]]): List of JSON dict per crawl.
        proc_file (str): File to save processed run info to as JSON.
    """
    window_days = 14
    dates, clones, cloners = get_daily_stats(objs, 'clones', window_days)
    dates2, views, viewers = get_daily_stats(objs, 'views', window_days)
    assert dates == dates2
    daily_times = list(map(noon_time_from_date, dates))

    point_times, forks = get_point_stats(objs, 'forks')
    times2, stars = get_point_stats(objs, 'stars')
    times3, watchers = get_point_stats(objs, 'watchers')
    assert point_times == times2 == times3

    obj = {
        'repo': repo,
        'daily': {
            'dates': dates,
            'times': daily_times,
            'clones': clones,
            'cloners': cloners,
            'views': views,
            'viewers': viewers,
        },
        'point': {
            'times': point_times,
            'forks': forks,
            'stars': stars,
            'watchers': watchers,
        },
    }

    with open(proc_file, 'w') as out:
        json.dump(obj, out)


def process_repos(raw_file: str, proc_dir: str) -> None:
    """Process all the repos that we have crawled.

    Args:
        raw_file (str): File containing raw crawl data in JSON.
        proc_dir (str): Directory to contain a processed JSON file per repo.
    """
    if not os.path.isfile(raw_file):
        raise ValueError(f'Raw file is missing: {raw_file}.')

    objs = open(raw_file)
    objs = map(json.loads, objs)
    objs = list(objs)

    if not objs:
        raise ValueError(f'Raw file contains no crawl data: {raw_file}.')

    repo2objs = defaultdict(list)
    for obj in objs:
        repo = obj['repo']
        repo2objs[repo].append(obj)

    if os.path.exists(proc_dir):
        rmtree(proc_dir)
    os.makedirs(proc_dir)

    for repo in sorted(repo2objs):
        objs = repo2objs[repo]
        repo_basename = repo.replace('/', '.') + '.json'
        proc_file = os.path.join(proc_dir, repo_basename)
        process_repo(repo, objs, proc_file)


def main(args: Namespace) -> None:
    """Main method.

    Args:
        args (Namespace): Command-line arguments.
    """
    config = json.load(open(args.config))
    raw_file = config['raw']
    proc_dir = config['proc']
    process_repos(raw_file, proc_dir)


if __name__ == '__main__':
    main(parse_args())
