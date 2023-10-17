import multiprocessing
import threading
import traceback

import peewee
from abjad import sequence, Timer
from playhouse import sqlite_ext

from discograph.library.bootstrapper import Bootstrapper
from discograph.library.discogs_model import DiscogsModel


class SqliteRelease(DiscogsModel):
    # CLASS VARIABLES

    _artists_mapping = {}

    _companies_mapping = {}

    _tracks_mapping = {}

    class BootstrapPassTwoWorker(threading.Thread):

        def __init__(self, indices):
            threading.Thread.__init__(self)
            self.indices = indices

        def run(self):
            proc_name = self.name
            corpus = {}
            total = len(self.indices)
            for i, release_id in enumerate(self.indices):
                with DiscogsModel.connection_context():
                    progress = float(i) / total
                    try:
                        SqliteRelease.bootstrap_pass_two_single(
                            release_id=release_id,
                            annotation=proc_name,
                            corpus=corpus,
                            progress=progress,
                        )
                    except peewee.PeeweeException:
                        print('ERROR in SqliteRelease BootstrapPassTwoWorker:', release_id, proc_name)
                        traceback.print_exc()

    # PEEWEE FIELDS

    id = peewee.IntegerField(primary_key=True)
    artists = sqlite_ext.JSONField(null=True, index=False)
    companies = sqlite_ext.JSONField(null=True, index=False)
    country = peewee.TextField(null=True, index=False)
    extra_artists = sqlite_ext.JSONField(null=True, index=False)
    formats = sqlite_ext.JSONField(null=True, index=False)
    genres = sqlite_ext.JSONField(null=True, index=False)
    identifiers = sqlite_ext.JSONField(null=True, index=False)
    labels = sqlite_ext.JSONField(null=True, index=False)
    master_id = peewee.IntegerField(null=True, index=False)
    notes = peewee.TextField(null=True, index=False)
    release_date = peewee.DateTimeField(null=True, index=False)
    styles = sqlite_ext.JSONField(null=True, index=False)
    title = peewee.TextField(index=False)
    tracklist = sqlite_ext.JSONField(null=True, index=False)

    # PEEWEE META

    class Meta:
        db_table = 'releases'

    # PUBLIC METHODS

    @classmethod
    def bootstrap(cls):
        cls.drop_table(True)
        cls.create_table()
        cls.bootstrap_pass_one()
        cls.bootstrap_pass_two()

    @classmethod
    def bootstrap_pass_one(cls, **kwargs):
        DiscogsModel.bootstrap_pass_one(
            model_class=cls,
            xml_tag='release',
            name_attr='title',
            skip_without=['title'],
            **kwargs
        )

    @classmethod
    def get_indices(cls, pessimistic=False):
        indices = []
        if not pessimistic:
            maximum_id = cls.select(
                peewee.fn.Max(cls.id)).scalar()
            step = maximum_id // multiprocessing.cpu_count()
            for start in range(0, maximum_id, step):
                stop = start + step
                indices.append(range(start, stop))
        else:
            query = cls.select(cls.id)
            query = query.order_by(cls.id)
            query = query.tuples()
            all_ids = tuple(_[0] for _ in query)
            ratio = [1] * (multiprocessing.cpu_count() * 2)
            for chunk in sequence.partition_by_ratio_of_lengths(all_ids, tuple(ratio)):
                indices.append(chunk)
        return indices

    @classmethod
    def get_release_iterator(cls, pessimistic=False):
        if not pessimistic:
            maximum_id = cls.select(peewee.fn.Max(cls.id)).scalar()
            for i in range(1, maximum_id + 1):
                query = cls.select().where(cls.id == i)
                if not query.count():
                    continue
                document = query.get()
                yield document
        else:
            id_query = cls.select(cls.id)
            for release in id_query:
                release_id = release.id
                release = cls.select().where(cls.id == release_id).get()
                yield release

    @classmethod
    def bootstrap_pass_two(cls, pessimistic=False, **kwargs):
        indices = cls.get_indices(pessimistic=pessimistic)
        workers = [cls.BootstrapPassTwoWorker(x) for x in indices]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        # for worker in workers:
        #     worker.terminate()

    @classmethod
    def bootstrap_pass_two_single(
            cls,
            release_id,
            annotation='',
            corpus=None,
            progress=None,
    ):
        skipped_template = u'{} (Pass 2) {:.3%} [{}]\t[SKIPPED] (id:{}) [{:.8f}]: {}'
        changed_template = u'{} (Pass 2) {:.3%} [{}]\t          (id:{}) [{:.8f}]: {}'
        query = cls.select().where(cls.id == release_id)
        if not query.count():
            return
        document = query.get()
        with Timer(verbose=False) as timer:
            changed = document.resolve_references(corpus)
        if not changed:
            message = skipped_template.format(
                cls.__name__.upper(),
                progress,
                annotation,
                document.id,
                timer.elapsed_time,
                document.title,
            )
            if Bootstrapper.is_test:
                print(message)
            return
        message = changed_template.format(
            cls.__name__.upper(),
            progress,
            annotation,
            document.id,
            timer.elapsed_time,
            document.title,
        )
        if Bootstrapper.is_test:
            print(message)
        document.save()

    @classmethod
    def element_to_artist_credits(cls, element):
        result = []
        if element is None or not len(element):
            return result
        for subelement in element:
            data = cls.tags_to_fields(
                subelement,
                ignore_none=True,
                mapping=cls._artists_mapping,
            )
            result.append(data)
        return result

    @classmethod
    def element_to_company_credits(cls, element):
        result = []
        if element is None or not len(element):
            return result
        for subelement in element:
            data = cls.tags_to_fields(
                subelement,
                ignore_none=True,
                mapping=cls._companies_mapping,
            )
            result.append(data)
        return result

    @classmethod
    def element_to_formats(cls, element):
        result = []
        if element is None or not len(element):
            return result
        for subelement in element:
            document = {
                'name': subelement.get('name'),
                'quantity': subelement.get('qty'),
            }
            if subelement.get('text'):
                document['text'] = subelement.get('text')
            if len(subelement):
                subelement = subelement[0]
                descriptions = Bootstrapper.element_to_strings(subelement)
                document['descriptions'] = descriptions
            result.append(document)
        return result

    @classmethod
    def element_to_identifiers(cls, element):
        result = []
        if element is None or not len(element):
            return result
        for subelement in element:
            data = {
                'description': subelement.get('description'),
                'type': subelement.get('type'),
                'value': subelement.get('value'),
            }
            result.append(data)
        return result

    @classmethod
    def element_to_label_credits(cls, element):
        result = []
        if element is None or not len(element):
            return result
        for subelement in element:
            data = {
                'catalog_number': subelement.get('catno'),
                'name': subelement.get('name'),
            }
            result.append(data)
        return result

    @classmethod
    def element_to_roles(cls, element):
        def from_text(text):
            name = ''
            current_buffer = ''
            details = []
            had_detail = False
            _bracket_depth = 0
            for _character in text:
                if _character == '[':
                    _bracket_depth += 1
                    if _bracket_depth == 1 and not had_detail:
                        name = current_buffer
                        current_buffer = ''
                        had_detail = True
                    elif 1 < _bracket_depth:
                        current_buffer += _character
                elif _character == ']':
                    _bracket_depth -= 1
                    if not _bracket_depth:
                        details.append(current_buffer)
                        current_buffer = ''
                    else:
                        current_buffer += _character
                else:
                    current_buffer += _character
            if current_buffer and not had_detail:
                name = current_buffer
            name = name.strip()
            detail = ', '.join(_.strip() for _ in details)
            result = {'name': name}
            if detail:
                result['detail'] = detail
            return result

        credit_roles = []
        if element is None or not element.text:
            return credit_roles or None
        current_text = ''
        bracket_depth = 0
        for character in element.text:
            if character == '[':
                bracket_depth += 1
            elif character == ']':
                bracket_depth -= 1
            elif not bracket_depth and character == ',':
                current_text = current_text.strip()
                if current_text:
                    credit_roles.append(from_text(current_text))
                current_text = ''
                continue
            current_text += character
        current_text = current_text.strip()
        if current_text:
            credit_roles.append(from_text(current_text))
        return credit_roles or None

    @classmethod
    def element_to_tracks(cls, element):
        result = []
        if element is None or not len(element):
            return result
        for subelement in element:
            data = cls.tags_to_fields(
                subelement,
                ignore_none=True,
                mapping=cls._tracks_mapping,
            )
            result.append(data)
        return result

    @classmethod
    def from_element(cls, element):
        data = cls.tags_to_fields(element)
        data['id'] = int(element.get('id'))
        # noinspection PyArgumentList
        return cls(**data)

    def resolve_references(self, corpus, spuriously=False):
        changed = False
        spurious_id = 0
        for entry in self.labels:
            name = entry['name']
            entity_key = (2, name)
            if not spuriously:
                from discograph.library.sqlite.sqlite_entity import SqliteEntity
                SqliteEntity.update_corpus(corpus, entity_key)
            if entity_key in corpus:
                entry['id'] = corpus[entity_key]
                changed = True
            elif spuriously:
                spurious_id -= 1
                corpus[entity_key] = spurious_id
                entry['id'] = corpus[entity_key]
                changed = True
        return changed


SqliteRelease._tags_to_fields_mapping = {
    'artists': ('artists', SqliteRelease.element_to_artist_credits),
    'companies': ('companies', SqliteRelease.element_to_company_credits),
    'country': ('country', Bootstrapper.element_to_string),
    'extraartists': ('extra_artists', SqliteRelease.element_to_artist_credits),
    'formats': ('formats', SqliteRelease.element_to_formats),
    'genres': ('genres', Bootstrapper.element_to_strings),
    'identifiers': ('identifiers', SqliteRelease.element_to_identifiers),
    'labels': ('labels', SqliteRelease.element_to_label_credits),
    'master_id': ('master_id', Bootstrapper.element_to_integer),
    'released': ('release_date', Bootstrapper.element_to_datetime),
    'styles': ('styles', Bootstrapper.element_to_strings),
    'title': ('title', Bootstrapper.element_to_string),
    'tracklist': ('tracklist', SqliteRelease.element_to_tracks),
}

SqliteRelease._artists_mapping = {
    'id': ('id', Bootstrapper.element_to_integer),
    'name': ('name', Bootstrapper.element_to_string),
    'anv': ('anv', Bootstrapper.element_to_string),
    'join': ('join', Bootstrapper.element_to_string),
    'role': ('roles', SqliteRelease.element_to_roles),
    'tracks': ('tracks', Bootstrapper.element_to_string),
}

SqliteRelease._companies_mapping = {
    'id': ('id', Bootstrapper.element_to_integer),
    'name': ('name', Bootstrapper.element_to_string),
    'catno': ('catalog_number', Bootstrapper.element_to_string),
    'entity_type': ('entity_type', Bootstrapper.element_to_integer),
    'entity_type_name': ('entity_type_name', Bootstrapper.element_to_string),
}

SqliteRelease._tracks_mapping = {
    'position': ('position', Bootstrapper.element_to_string),
    'title': ('title', Bootstrapper.element_to_string),
    'duration': ('duration', Bootstrapper.element_to_string),
    'artists': ('artists', SqliteRelease.element_to_artist_credits),
    'extraartists': ('extra_artists', SqliteRelease.element_to_artist_credits),
}
