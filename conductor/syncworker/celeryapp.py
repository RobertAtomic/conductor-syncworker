"""
Cerlery App
"""

from celery import Celery
from .celeryconfig import CeleryConfig

APP = Celery()
APP.config_from_object(CeleryConfig)
