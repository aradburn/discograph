/**
 * @file Implements a typeahead search functionality using Bloodhound and Twitter Typeahead
 * @description This module provides an autocomplete search interface that queries an API endpoint
 * and displays search results with debouncing for performance.
 */

import { RequestNetworkEvent } from "./network/events";

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
export const initTypeahead = () => {
    // Initialize Bloodhound suggestion engine with remote data source
    // @ts-ignore
    const typeaheadBloodhound = new Bloodhound({
        // @ts-ignore
        datumTokenizer: Bloodhound.tokenizers.whitespace,
        // @ts-ignore
        queryTokenizer: Bloodhound.tokenizers.whitespace,
        remote: {
            url: "/api/search/%QUERY",
            wildcard: "%QUERY",
            filter: (response) => response.results,
            rateLimitBy: 'debounce',
            rateLimitWait: 1000, // Debounce API calls by 1 second
        },
    });

    const inputElement = document.getElementById("typeahead");
    console.log("inputElement: ", inputElement);
//    const loadingElement = document.querySelector("#search .loading");
//    console.log("loadingElement: ", loadingElement);

    if (!inputElement) {
        console.log("Error - Typeahead missing input or loading element");
        return;
    }

    // Configure and initialize typeahead
    // Note: We still need to use jQuery for typeahead plugin functionality
    // @ts-ignore
    $(inputElement).typeahead(
        {
            hint: false,
            highlight: true,
            minLength: 4, // Minimum characters before search begins
        }, 
        {
            name: "results",
            display: "name",
            limit: 1000,
            source: typeaheadBloodhound,
            templates: {
                suggestion: (data) => `
                    <div>
                        <span>${data.name}</span>
                        <em>(${data.key.split('-')[0]})</em>
                    </div>
                `,
            },
        }
    );

    // Handle keyboard events
    inputElement.addEventListener('keydown', (event) => {
        console.log("Typeahead keydownassdf");

        if (event.key === 'Enter') {
            event.preventDefault();
            navigateTypeahead();
        } else if (event.key === 'Escape') {
            // @ts-ignore
            $(inputElement).typeahead("close");
        }
    });

    // Handle loading state
    ['typeahead:asynccancel', 'typeahead:asyncreceive'].forEach(eventName => {
        inputElement.addEventListener(eventName, () => {
            loadingElement.classList.add("invisible");
        });
    });

    inputElement.addEventListener('typeahead:asyncrequest', () => {
        loadingElement.classList.remove("invisible");
    });

    // Handle selection state
    inputElement.addEventListener('typeahead:autocomplete', (event) => {
        // @ts-ignore
        elementData.set(inputElement, event.detail.datum.key);
    });

    inputElement.addEventListener('typeahead:render', (event) => {
        // @ts-ignore
        const suggestion = event.detail.suggestion;
        elementData.set(inputElement, suggestion?.key || null);
    });

    inputElement.addEventListener('typeahead:selected', (event) => {
        // @ts-ignore
        elementData.set(inputElement, event.detail.datum.key);
        navigateTypeahead();
    });

    // Initialize clear button functionality
    const clearButton = document.querySelector('#search .clear');
    if (clearButton) {
        clearButton.addEventListener("click", () => {
            // @ts-ignore
            $(inputElement).typeahead('val', '');
        });
    }
};

/**
 * Handles navigation when a typeahead suggestion is selected
 * @function navigateTypeahead
 * @description Triggers a custom event to request network data for the selected entity
 * and updates the browser history.
 */
const navigateTypeahead = () => {
    const inputElement = document.getElementById("typeahead");
    if (!inputElement) return;

    const datum = elementData.get(inputElement);
    if (datum) {
        // @ts-ignore
        $(inputElement).typeahead("close");
        inputElement.blur();
        
        // Create and dispatch custom event
        const pushHistory = true;
        window.dispatchEvent(new RequestNetworkEvent(datum, pushHistory));
    }
};

// Initialize WeakMap for storing element data
const elementData = new WeakMap();
