import logging

def get_logger():
    logging.basicConfig(
        filename='logs/execution.log',
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    return logging.getLogger()
