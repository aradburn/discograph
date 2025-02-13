import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy.exc import IntegrityError, InvalidRequestError
from sqlalchemy.orm import Session

from discograph.exceptions import DatabaseError
from discograph.runtime.runtime_database.runtime_session import (
    get_runtime_session,
    CTX_RUNTIME_SESSION,
)

log = logging.getLogger(__name__)


@contextmanager
def transaction() -> Generator[Session, None, None]:
    """Use this context manager to perform database transactions. in any coroutine in the source code."""

    session: Session = get_runtime_session()
    CTX_RUNTIME_SESSION.set(session)

    try:
        yield session
        session.commit()
    except DatabaseError as error:
        # NOTE: If any sort of issues are occurred in the code
        #       they are handled on the BaseCRUD level and raised
        #       as a DatabseError.
        #       If the DatabseError is handled within domain/application
        #       levels it is possible that `await session.commit()`
        #       would raise an error.
        log.error(f"Rolling back changes. {error}")
        session.rollback()
        raise DatabaseError
    except (IntegrityError, InvalidRequestError) as error:
        # NOTE: Since there is a session commit on this level it should
        #       be handled because it can raise some errors also
        log.error(f"Rolling back changes. {error}")
        session.rollback()
    finally:
        session.close()
