from typing import Tuple, Optional


class Uri:
    def __init__(self, uri_string: str):
        self.__protocol, path = uri_string.split(':', 1)
        self.__query = None
        if '?' in path:
            path, self.__query = path.split('?', 1)
        self.__path_elements = path.split('/')

        # cached
        self.__path = None
        
    @property
    def path_elements(self) -> Tuple[str]:
        return tuple(self.__path_elements)

    @property
    def path(self) -> str:
        if self.__path is None:
            self.__path = '/'.join(self.path_elements)
        return self.__path
    
    @property
    def protocol(self) -> str:
        return self.__protocol

    @property
    def query(self) -> Optional[str]:
        return self.__query

    def __str__(self):
        return f'{self.__protocol}:{"/".join(self.__path_elements)}' + (f'?{self.__query}' if self.__query is not None else '')

    def __repr__(self):
        return f'<Uri: "{str(self)}">'
