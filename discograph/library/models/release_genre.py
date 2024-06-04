import logging

log = logging.getLogger(__name__)


# class ReleaseGenre(Base, LoaderBase):
#     __tablename__ = "release_genre"
#
#     # COLUMNS
#     release_id: Mapped[Integer] = mapped_column(
#         Integer, ForeignKey("release.release_id")
#     )
#     genre_id: Mapped[Integer] = mapped_column(Integer, ForeignKey("genre.genre_id"))
#
#     genres = relationship("Genre", backref="release_genres")
#
#     __table_args__ = (
#         PrimaryKeyConstraint(release_id, genre_id),
#         {},
#     )

# CLASS VARIABLES

# PUBLIC METHODS
