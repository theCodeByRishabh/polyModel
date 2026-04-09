import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    normalized_level = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=normalized_level,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)],
        )
    else:
        root.setLevel(normalized_level)

    # Keep app logs informative while reducing third-party network noise.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
