from typing import Any  # pylint: disable=unused-import
from typing import Dict  # pylint: disable=unused-import
from typing import Optional  # pylint: disable=unused-import


class DatastoreOperation(object):
    def __init__(self,
                 name,  # type: str
                 done,  # type: bool
                 metadata=None,  # type: Optional[Dict[str, Any]]
                 error=None,  # type: Optional[Dict[str, str]]
                 response=None  # type: Optional[Dict[str, Any]]
                 ):
        # type: (...) -> None
        self.name = name
        self.done = done

        self.metadata = metadata
        self.error = error
        self.response = response

    @classmethod
    def from_repr(cls, data):
        # type: (Dict[str, Any]) -> DatastoreOperation
        return cls(data['name'], data['done'], data.get('metadata'),
                   data.get('error'), data.get('response'))

    def to_repr(self):
        # type: () -> Dict[str, Any]
        return {
            'done': self.done,
            'error': self.error,
            'metadata': self.metadata,
            'name': self.name,
            'response': self.response,
        }
