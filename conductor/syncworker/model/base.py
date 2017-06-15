"""
Base classes for Conductor objects
"""
# pylint: disable=too-few-public-methods

import copy
import requests

from .util import get_token, get_uri


class ConductorWorkerException(Exception):
    """
    Exception class for use in tasks
    """
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class ConductorBase(object):
    """Base class for Conductor objects"""
    def __init__(self, kind, key, base_uri=None, token=None):
        self._dict = {}
        self._exists = False
        self._key = key
        self._kind = kind
        self._api_ver = 1
        self._token = token or get_token()
        self._base_uri = base_uri or get_uri()
        if not self._kind.endswith('s'):
            self._kind += 's'

    def _fetch(self):
        response = requests.get(
            self.uri,
            headers={'Authorization': 'Token ' + self._token}
            )
        response.raise_for_status()
        self._dict = response.json()['data']
        if self._dict:
            self._exists = True
        return

    def __getitem__(self, key):
        return self.dict[key]

    def __len__(self):
        return len(self.dict)

    def __getattr__(self, attr):
        return self.dict[attr]

    def __str__(self):
        return str(self.dict)

    __repr__ = __str__

    @property
    def exists(self):
        """
        Check if object exists in underlying datastore
        """
        self._fetch()
        return self._exists

    @property
    def uri(self):
        """
        Return uri of this resource
        """
        return '{}/api/v{}/{}/{}'.format(
            self._base_uri,
            self._api_ver,
            self._kind,
            self._key
            )

    @property
    def dict(self):
        """
        Return dict representing object
        """
        self._fetch()
        if not self._exists:
            self._dict = {}
        return copy.deepcopy(self._dict)


class ConductorListBase(object):
    """
    Base class for list of Conductor objects
    """
    def __init__(self, kind, base_uri=None, token=None, filter=None):
        self._list = []
        self._filter = filter or ''
        self._kind = kind
        self._token = token or get_token()
        self._base_uri = base_uri or get_uri()
        if not self._kind.endswith('s'):
            self._kind += 's'

    def _fetch(self):
        response = requests.get(
            self.uri,
            headers={'Authorization': 'Token ' + self._token}
            )
        self._list = response.json()

    def __getitem__(self, idx):
        return self.list[idx]

    def __len__(self):
        return len(self.list)

    def __str__(self):
        return str(self.list)

    def __repr__(self):
        return repr(self.list)

    @property
    def uri(self):
        """
        Return uri of this resource
        """
        return '{}/{}/?filter={}'.format(
            self._base_uri,
            self._kind,
            self._filter
            )

    @property
    def list(self):
        """
        Return list of matching objects
        """
        self._fetch()
        return self._list
