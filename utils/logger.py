import logging
import os


def get_logger(name="pipeline"):
    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        filename="logs/execution.log",
        filemode="a",  # append logs
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )

    return logging.getLogger(name)
