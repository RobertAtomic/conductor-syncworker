"""
Decompose task
"""

from .. import app


@app.task()
def decompose(upload):
    """
    Decompose an Upload object into syncable chunks
    """
    return upload.decompose()
