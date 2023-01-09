"""Plotting processed repo traffic data."""

import json
import os
from argparse import ArgumentParser, Namespace
from datetime import date
from shutil import rmtree

from matplotlib import pyplot as plt


def parse_args() -> Namespace:
    """Parse command-line arguments.

    Returns:
        Namespace: Command-line arguments.
    """
    args = ArgumentParser()
    args.add_argument('--config', type=str, default='data/config.json')
    return args.parse_args()


def plot_repo(proc_file: str, plot_file: str) -> None:
    """Plot one repo.

    Args:
        proc_file (str): File containing processed repo info as JSON.
        plot_file (str): File to contain the resulting plot.
    """
    line_width = 0.5

    fields = [
        ('views', 'daily', '#48f', ':'),
        ('viewers', 'daily', '#48f', '-'),
        ('clones', 'daily', '#0b0', ':'),
        ('cloners', 'daily', '#0b0', '-'),
        ('stars', 'point', '#fc0', '-'),
        ('forks', 'point', '#f80', '-'),
        ('watchers', 'point', '#f00', '-'),
    ]

    obj = json.load(open(proc_file))
    repo = obj['repo']

    plt.rcParams.update({'font.size': 6})
    plt.yscale('log')
    plt.title(f'{repo} traffic')

    which2dates = {}
    for which in ['daily', 'point']:
        which2dates[which] = list(map(date.fromtimestamp, obj[which]['times']))

    for key, which, color, line_style in fields:
        dates = which2dates[which]
        values = obj[which][key]
        plt.plot(dates, values, label=key, color=color, lw=line_width, ls=line_style)

    plt.grid(color='#ccc', ls=':', lw=0.5)
    plt.legend()
    plt.savefig(plot_file, dpi=400)
    plt.clf()


def plot_repos(proc_dir: str, plot_dir: str) -> None:
    """Plot all repos.

    Args:
        proc_dir (str): Directory containing one processed JSON file per repo.
        plot_dir (str): Directory to contain one plot per repo.
    """
    if os.path.exists(plot_dir):
        rmtree(plot_dir)
    os.makedirs(plot_dir)

    proc_basenames = sorted(os.listdir(proc_dir))
    for proc_basename in proc_basenames:
        assert proc_basename.endswith('.json')
        plot_basename = proc_basename[:-5] + '.png'
        proc_file = os.path.join(proc_dir, proc_basename)
        plot_file = os.path.join(plot_dir, plot_basename)
        plot_repo(proc_file, plot_file)


def main(args: Namespace) -> None:
    """Main method.

    Args:
        args (Namespace): Command-line arguments.
    """
    config = json.load(open(args.config))
    proc_dir = config['proc']
    plot_dir = config['plot']
    plot_repos(proc_dir, plot_dir)


if __name__ == '__main__':
    main(parse_args())
