/**
 * @file Implements a typeahead search functionality using Bloodhound and Twitter Typeahead
 * @description This module provides an autocomplete search interface that queries an API endpoint
 * and displays search results with debouncing for performance.
 */

import { RequestNetworkEvent } from "./network/events";
import Bloodhound from "corejs-typeahead/dist/bloodhound.js";
import "corejs-typeahead/dist/typeahead.jquery.js";
import "./css/typeahead-bootstrap.css";
import jQuery from "jquery";
import { TYPEAHEAD, TIMING } from "./constants";

interface JQuery<T = HTMLElement> {
    typeahead(options: TypeaheadOptions, dataset: DatasetOptions): JQuery<T>;
    typeahead(method: string, value?: string): JQuery<T>;
}

interface SearchResult {
    name: string;
    key: string;
}

interface BloodhoundTokenizers {
    whitespace: (str: string) => string[];
}

interface BloodhoundStatic {
    tokenizers: BloodhoundTokenizers;
    new <T>(options: BloodhoundOptions): BloodhoundEngine<T>;
}

interface BloodhoundEngine<T> {
    initialize(): Promise<void>;
    add(data: T[]): void;
    get(query: string): T[];
    search(query: string): Promise<T[]>;
    clear(): void;
}

interface BloodhoundOptions {
    datumTokenizer: (datum: string) => string[];
    queryTokenizer: (query: string) => string[];
    remote: {
        url: string;
        wildcard: string;
        filter: (response: { results: SearchResult[] }) => SearchResult[];
        rateLimitBy: "debounce";
        rateLimitWait: number;
    };
}

interface TypeaheadOptions {
    hint: boolean;
    highlight: boolean;
    minLength: number;
}

interface DatasetOptions {
    name: string;
    display: string;
    limit: number;
    source: BloodhoundEngine<SearchResult>;
    templates: {
        suggestion: (data: SearchResult) => string;
        pending: (query: string) => string;
    };
}

const $ = jQuery;

/**
 * Initializes the typeahead search functionality
 * @function initTypeahead
 * @description Sets up a typeahead search box with the following features:
 * - Remote API search with debouncing
 * - Result highlighting
 * - Keyboard navigation support
 * - Loading state indication
 * - Clear button functionality
 */
export const initTypeahead = (): void => {
    // Initialize Bloodhound suggestion engine with remote data source
    const bloodhoundConstructor = Bloodhound as unknown as BloodhoundStatic;
    const typeaheadBloodhound = new bloodhoundConstructor<SearchResult>({
        datumTokenizer: bloodhoundConstructor.tokenizers.whitespace,
        queryTokenizer: bloodhoundConstructor.tokenizers.whitespace,
        remote: {
            url: TYPEAHEAD.API_ENDPOINT,
            wildcard: TYPEAHEAD.QUERY_WILDCARD,
            // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
            filter: (response) => response.results,
            rateLimitBy: "debounce",
            rateLimitWait: TIMING.TYPEAHEAD_DEBOUNCE,
        },
    });

    const inputElement = document.getElementById(TYPEAHEAD.ELEMENT_ID);

    if (!inputElement) {
        console.log("Error - Typeahead missing input element");
        return;
    }

    // Configure and initialize typeahead
    ($(inputElement) as unknown as JQuery).typeahead(
        {
            hint: false,
            highlight: true,
            minLength: TYPEAHEAD.MIN_QUERY_LENGTH,
        },
        {
            name: "results",
            display: "name",
            limit: TYPEAHEAD.MAX_RESULTS,
            source: typeaheadBloodhound,
            templates: {
                suggestion: (data: SearchResult) => `
          <div>
            <span>${data.name}</span>
            <em>(${data.key.split("-")[0]})</em>
          </div>
        `,
                pending: () => `<div>Loading...</div>`,
            },
        },
    );

    // Handle keyboard events
    $(inputElement).on("keydown", (event: JQuery.KeyboardEventBase) => {
        console.log("Typeahead keydown");

        if (event.key === "Enter") {
            event.preventDefault();
            navigateTypeahead();
        } else if (event.key === "Escape") {
            ($(inputElement) as unknown as JQuery).typeahead("close");
        }
    });

    // Handle selection state
    $(inputElement).on(
        "typeahead:autocomplete",
        (_: JQuery.EventBase, datum: SearchResult) => {
            $(inputElement).data("selectedKey", datum.key);
        },
    );

    $(inputElement).on(
        "typeahead:render",
        (_: JQuery.EventBase, suggestion: SearchResult | undefined) => {
            if (suggestion !== undefined) {
                $(inputElement).data("selectedKey", suggestion.key);
            } else {
                $(inputElement).data("selectedKey", null);
            }
        },
    );

    $(inputElement).on(
        "typeahead:selected",
        (_: JQuery.EventBase, datum: SearchResult) => {
            console.log("typeahead:selected: ", datum);
            console.log("typeahead:selected: ", $(inputElement));
            $(inputElement).data("selectedKey", datum.key);
            navigateTypeahead();
        },
    );

    // Initialize clear button functionality
    const clearButton = document.querySelector(TYPEAHEAD.CLEAR_BUTTON_SELECTOR);
    if (clearButton) {
        clearButton.addEventListener("click", () => {
            ($(inputElement) as unknown as JQuery).typeahead("val", "");
        });
    }
};

/**
 * Handles navigation when a typeahead suggestion is selected
 * @function navigateTypeahead
 * @description Triggers a custom event to request network data for the selected entity
 * and updates the browser history.
 */
const navigateTypeahead = (): void => {
    console.log("navigateTypeahead");

    const inputElement = document.getElementById(TYPEAHEAD.ELEMENT_ID);
    if (!inputElement) return;

    const datum = $(inputElement).data("selectedKey") as string | null;
    console.log("navigateTypeahead: ", datum);

    if (datum) {
        ($(inputElement) as unknown as JQuery).typeahead("close");
        inputElement.blur();

        // Create and dispatch custom event
        const pushHistory = true;
        console.log("dispatching RequestNetworkEvent");
        window.dispatchEvent(new RequestNetworkEvent(datum, pushHistory));
    }
};
