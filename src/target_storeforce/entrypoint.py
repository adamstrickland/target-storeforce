import sys

import singer
from singer import utils

from .target import Target


LOGGER = singer.get_logger()


REQUIRED_CONFIG_KEYS = [
    "host",
    "port",
    "username",
    "password",
    "header",
    "delimiter",
    "quotechar",
    "destination_path",
]


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    target = Target(args.config)

    target.run(sys.stdin, sys.stdout)


if __name__ == "__main__":
    main()
