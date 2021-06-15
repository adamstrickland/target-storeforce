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
    LOGGER.info("Executing target-storeforce.entrypoint.main")

    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    LOGGER.info(f"Configured target using args: {args}")

    target = Target(args.config)
    LOGGER.info("Initialized Target")

    LOGGER.info("Running Target")
    target.run(sys.stdin, sys.stdout)
    LOGGER.info("Complete")


if __name__ == "__main__":
    main()
