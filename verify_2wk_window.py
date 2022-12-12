"""Verify the clones/views timeseries data must be within a two-week window."""

import json
from argparse import ArgumentParser, Namespace
from datetime import datetime
from time import mktime


def parse_args() -> Namespace:
    """Parse command-line arguments.

    Returns:
        Namespace: Command-line arguments.
    """
    args = ArgumentParser()
    args.add_argument('--data', type=str, required=True)
    return args.parse_args()


def main(args: Namespace) -> None:
    """Main method.

    Args:
        args (Namespace): Command-line arguments.
    """
    lines = open(args.data)
    objs = map(json.loads, lines)
    gaps = []
    for obj in objs:
        now = obj['time']
        thens = []
        for key in ['clones', 'views']:
            days = obj[key]['daily']
            date = days[0]['date']
            then = mktime(datetime.strptime(date, '%Y-%m-%d').timetuple())
            thens.append(then)
        then = min(thens)
        gap = now - then
        gaps.append(gap)

    print('Expect to see:')
    print('- If every day has traffic, 13-14 days (all have complete lists)')
    print('- If traffic is sparse, 0-14 days (due to incomplete lists)')
    print()

    sec_per_day = 24 * 60 * 60
    two_weeks = 14
    gaps = list(map(lambda gap: gap / sec_per_day, gaps))
    gaps.sort()

    print('Values:')
    for gap in gaps:
        print(f'- {gap:2.3f} days')
    assert 0 <= gaps[0]
    assert gaps[-1] < two_weeks


if __name__ == '__main__':
    main(parse_args())
