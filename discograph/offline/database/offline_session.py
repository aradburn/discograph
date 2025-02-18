# noinspection PyPackageRequirements
from contextvars import ContextVar

from sqlalchemy.engine import ResultProxy
from sqlalchemy.exc import IntegrityError, InvalidRequestError
from sqlalchemy.orm import Session, scoped_session

from discograph.exceptions import DatabaseError


def get_offline_session() -> Session:
    """Creates a new session to execute SQL queries."""
    from discograph.offline.offline_database_manager import OfflineDatabaseManager

    if OfflineDatabaseManager.get_concurrency_count() > 1:
        session = scoped_session(
            OfflineDatabaseManager.offline_database_helper.offline_session_factory
        )
    else:
        session = OfflineDatabaseManager.offline_database_helper.offline_session_factory
    return session()


CTX_OFFLINE_SESSION: ContextVar[Session] = ContextVar("offline_session")


class OfflineSession:
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
                self._ctx_session: Session = CTX_OFFLINE_SESSION.get()
            except LookupError:
                raise DatabaseError(message="Not in a transaction")
        return self._ctx_session
