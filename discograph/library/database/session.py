# noinspection PyPackageRequirements
from contextvars import ContextVar

from sqlalchemy.engine import ResultProxy
from sqlalchemy.exc import IntegrityError, InvalidRequestError
from sqlalchemy.orm import Session, scoped_session

from discograph.database import get_concurrency_count
from discograph.exceptions import DatabaseError
from discograph.library.database.database_helper import DatabaseHelper


# __all__ = ("create_session", "CTX_SESSION")


def get_session() -> Session:
    """Creates a new session to execute SQL queries."""
    if get_concurrency_count() > 1:
        session = scoped_session(DatabaseHelper.session_factory)
    else:
        session = DatabaseHelper.session_factory
    return session()


CTX_SESSION: ContextVar[Session] = ContextVar("session")
# CTX_SESSION: ContextVar[Session] = ContextVar("session", default=get_session())


class WrappedSession:
    """The basic class to perform database operations within the session."""

    # All sqlalchemy errors that can be raised
    _ERRORS = (IntegrityError, InvalidRequestError)

    def __init__(self) -> None:
        self._ctx_session = None
        # self._session: Session = CTX_SESSION.get()

    def execute(self, query) -> ResultProxy:
        try:
            result = self._session.execute(query)
            return result
        except self._ERRORS:
            raise DatabaseError

    @property
    def _session(self) -> Session:
        if not self._ctx_session:
            try:
                self._ctx_session: Session = CTX_SESSION.get()
            except LookupError:
                raise DatabaseError(message="Not in a transaction")
        return self._ctx_session
