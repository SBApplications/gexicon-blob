"""Gexicon GEX pipeline: fetch CBOE delayed chains, compute GEX, encode the blob.

Standard library only. No third-party dependencies, by design -- this runs from a
bare scheduler with no virtualenv to maintain.
"""

__version__ = "2.0.0"
BLOB_PREFIX = "NSGEX2"
