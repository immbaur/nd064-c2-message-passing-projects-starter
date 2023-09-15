import logging
import sys
import time

def main():
    # Configure logging to stdout
    logger = logging.getLogger("my-logger")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create a StreamHandler to log to stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    # Add the StreamHandler to the logger
    logger.addHandler(stream_handler)

    # output hello world every second through the logger
    while True:
        logger.info("Hello World")
        time.sleep(1)

if __name__ == "__main__":
    main()