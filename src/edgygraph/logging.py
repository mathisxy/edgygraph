import logging
from rich.logging import RichHandler

def setup_logging(level: int = logging.WARNING):
    """Setup logging with Rich handler. Call once at startup."""
    logger = logging.getLogger("edgygraph")
    
    # Check if already configured
    if any(isinstance(h, RichHandler) for h in logger.handlers):
        return
    
    # Only configure edgygraph logger, not root
    handler = RichHandler(rich_tracebacks=True, show_path=False)
    handler.setFormatter(logging.Formatter("%(message)s"))
    
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False  # Don't propagate to root logger

def get_logger(name: str):
    """Get a logger instance."""
    # Ensure it's under edgygraph namespace
    if not name.startswith("edgygraph"):
        name = f"edgygraph.{name}"
    
    # Lazy setup with default WARNING level
    edgy_logger = logging.getLogger("edgygraph")
    if not edgy_logger.handlers:
        setup_logging()
    
    return logging.getLogger(name)