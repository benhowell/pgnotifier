"""
A simple little utility to capture and process Postgresql NOTIFY streams
"""

from .notify import Notifier

__all__ = (
	"Notifier",
)
