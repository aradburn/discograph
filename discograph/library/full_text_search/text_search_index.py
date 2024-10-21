import logging
import math
import sys
from collections import Counter

from discograph.utils import calculate_size

log = logging.getLogger(__name__)


class TextSearchIndex:
    STOP_WORDS = {
        "the",
        "and",
        "a",
        "of",
        "studio",
        "studios",
        "productions",
        "music",
        "records",
        "recordings",
        "entertainment",
    }

    def __init__(self):
        self.index: dict[str, set[int]] = {}
        self.documents: dict[int, str] = {}

    def index_entry(self, id_: int, text: str) -> None:
        from discograph.library.data_access_layer.entity_data_access import (
            EntityDataAccess,
        )

        # Save the original document to return when searched for
        self.documents[id_] = text

        normalised_text = EntityDataAccess.normalise_search_content(text)
        for token in normalised_text.split():
            if token in TextSearchIndex.STOP_WORDS:
                continue
            if token not in self.index:
                self.index[token] = set[int]()
            self.index[token].add(id_)
            # log.debug(f"search add: {token}: {self.index[token]}")

        # Handle cases like hyphens in surnames
        if "-" in normalised_text:
            normalised_text = normalised_text.replace("-", " ")
            for token in normalised_text.split():
                if token not in self.index:
                    self.index[token] = set[int]()
                self.index[token].add(id_)
                # log.debug(f"search add: {token}: {self.index[token]}")

    def document_frequency(self, token: str) -> int:
        return len(self.index.get(token, set[int]()))

    def inverse_document_frequency(self, token: str) -> float:
        # Manning, Hinrich and Schütze use log10, so we do too, even though it
        # doesn't really matter which log we use anyway
        # https://nlp.stanford.edu/IR-book/html/htmledition/inverse-document-frequency-1.html
        return math.log10(len(self.documents) / self.document_frequency(token))

    def _results(self, analyzed_query: list[str]) -> list[set[int]]:
        return [self.index.get(token, set()) for token in analyzed_query]

    def search(self, query: str) -> list[tuple[int, str]]:
        """
        Search; this will return documents that contain words from the query,
        and rank them if requested (sets are fast, but unordered).

        Parameters:
          - query: the query string
        """
        analyzed_query = query.split()
        # log.debug(f"search analyzed_query: {analyzed_query}")

        results = self._results(analyzed_query)
        # all tokens must be in the document
        documents = [
            (doc_id, self.documents[doc_id])
            for doc_id in set[int].intersection(*results)
        ]
        return self.rank(analyzed_query, documents)

    def rank(
        self, analyzed_query: list[str], documents: list[tuple[int, str]]
    ) -> list[tuple[int, str]]:
        from discograph.library.data_access_layer.entity_data_access import (
            EntityDataAccess,
        )

        results: list[tuple[tuple[int, str], float]] = []
        if not documents:
            return list[tuple[int, str]]()
        for document in documents:

            normalised_name = EntityDataAccess.normalise_search_content(document[1])
            term_frequencies = Counter(normalised_name.split())

            score = 0.0
            for token in analyzed_query:
                if token in TextSearchIndex.STOP_WORDS:
                    continue
                tf = term_frequencies.get(token, 0)
                # tf = document.term_frequency(token)
                idf = self.inverse_document_frequency(token)
                score += tf * idf
            results.append((document, score))
        ranked = sorted(results, key=lambda doc: doc[1], reverse=True)
        return [ranked_item[0] for ranked_item in ranked]

    def list_stop_words(self) -> list[str]:
        results = {}
        for key, words in self.index.items():
            if len(words) > 10000:
                # print(f"{key}: {len(words)}")
                results[key] = len(words)
        log.debug(f"found {len(results)} stop words")
        sorted_results = sorted(results.items(), key=lambda item: int(item[1]))
        for entry in sorted_results:
            print(f"{entry[0]}: {entry[1]}")
        return list(results.keys())

    def print_sizes(self) -> None:
        size_index = calculate_size(self.index)
        size_documents = calculate_size(self.documents)
        log.debug(f"size of index    : {size_index}")
        log.debug(f"size of documents: {size_documents}")
