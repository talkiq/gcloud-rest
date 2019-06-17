from typing import Any  # pylint: disable=unused-import
from typing import Dict  # pylint: disable=unused-import
from typing import List  # pylint: disable=unused-import
from typing import Optional  # pylint: disable=unused-import


class PathElement(object):
    def __init__(self, kind, id_=None, name=None):
        # type: (str, Optional[int], Optional[str]) -> None
        self.kind = kind

        self.id = id_
        self.name = name
        if self.id and self.name:
            raise Exception('invalid PathElement contains both ID and name')

    def __eq__(self, other):
        # type: (Any) -> bool
        if not isinstance(other, PathElement):
            return False

        return bool(self.kind == other.kind and self.id == other.id
                    and self.name == other.name)

    def __repr__(self):
        # type: () -> str
        return str(self.to_repr())

    @classmethod
    def from_repr(cls, data):
        # type: (Dict[str, Any]) -> PathElement
        kind = data['kind']  # type: str
        id_ = data.get('id')  # type: Optional[int]
        name = data.get('name')  # type: Optional[str]
        return cls(kind, id_=id_, name=name)

    def to_repr(self):
        # type: () -> Dict[str, Any]
        data = {'kind': self.kind}
        if self.id:
            data['id'] = self.id
        elif self.name:
            data['name'] = self.name

        return data


class Key(object):
    path_element_kind = PathElement

    def __init__(self, project, path, namespace=''):
        # type: (str, List[PathElement], str) -> None
        self.project = project
        self.namespace = namespace
        self.path = path

    def __eq__(self, other):
        # type: (Any) -> bool
        if not isinstance(other, Key):
            return False

        return bool(self.project == other.project
                    and self.namespace == other.namespace
                    and self.path == other.path)

    def __repr__(self):
        # type: () -> str
        return str(self.to_repr())

    @classmethod
    def from_repr(cls, data):
        # type: (Dict[str, Any]) -> Key
        return cls(data['partitionId']['projectId'],
                   path=[cls.path_element_kind.from_repr(p)
                         for p in data['path']],
                   namespace=data['partitionId'].get('namespaceId', ''))

    def to_repr(self):
        # type: () -> Dict[str, Any]
        return {
            'partitionId': {
                'projectId': self.project,
                'namespaceId': self.namespace,
            },
            'path': [p.to_repr() for p in self.path],
        }
