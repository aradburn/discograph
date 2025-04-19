import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Define interfaces for Bloodhound
interface BloodhoundTokenizers {
    whitespace: ReturnType<typeof vi.fn>;
}

interface BloodhoundStatic {
    tokenizers: BloodhoundTokenizers;
    new <T>(options: any): ReturnType<typeof vi.fn>;
}

// Define interface for SearchResult
interface SearchResult {
    key: string;
    name: string;
}

// Define interface for BloodhoundEngine
interface BloodhoundEngine<T> {
    initialize: ReturnType<typeof vi.fn>;
    add: ReturnType<typeof vi.fn>;
    get: ReturnType<typeof vi.fn>;
    search: ReturnType<typeof vi.fn>;
    clear: ReturnType<typeof vi.fn>;
}

// Define interface for event handlers
interface KeyboardEventLike {
    key: string;
    preventDefault?: () => void;
}

// Define mock object interfaces
interface MockedJQueryFunctions {
    typeahead: ReturnType<typeof vi.fn>;
    on: ReturnType<typeof vi.fn>;
    data: ReturnType<typeof vi.fn>;
    blur: ReturnType<typeof vi.fn>;
}

// Mock jQuery before importing typeahead
vi.mock("jquery", () => {
    // Create mock functions within the factory scope to avoid hoisting issues
    const typeaheadFn = vi.fn().mockReturnThis();
    const onFn = vi.fn().mockReturnThis();
    const dataFn = vi.fn();
    const blurFn = vi.fn();

    return {
        default: vi.fn(
            () =>
                ({
                    typeahead: typeaheadFn,
                    on: onFn,
                    data: dataFn,
                    blur: blurFn,
                }) as MockedJQueryFunctions,
        ),
    };
});

// Mock Bloodhound before importing
vi.mock("corejs-typeahead/dist/bloodhound.js", () => {
    const mockTokenizers = {
        whitespace: vi.fn((str: string) => str.split(/\s+/)),
    };

    const MockBloodhound = vi.fn().mockImplementation(() => ({
        initialize: vi.fn().mockResolvedValue(undefined),
        add: vi.fn(),
        get: vi.fn(),
        search: vi.fn(),
        clear: vi.fn(),
    }));

    // Add tokenizers property to the constructor using the proper type
    (MockBloodhound as unknown as BloodhoundStatic).tokenizers = mockTokenizers;

    return {
        default: MockBloodhound,
    };
});

// Mock RequestNetworkEvent for testing navigateTypeahead
vi.mock("../network/events", () => {
    return {
        RequestNetworkEvent: vi
            .fn()
            .mockImplementation((key: string, pushHistory: boolean) => ({
                key,
                pushHistory,
            })),
    };
});

// Import after mocks
import { initTypeahead } from "../typeahead";
import jQuery from "jquery";
import { RequestNetworkEvent } from "../network/events";

// Define types for typechecking the mock results
interface MockJQueryResult {
    typeahead: ReturnType<typeof vi.fn>;
    on: ReturnType<typeof vi.fn>;
    data: ReturnType<typeof vi.fn>;
    blur: ReturnType<typeof vi.fn>;
}

// Define type for callback function array
type CallbackArray = [string, (...args: any[]) => void];

describe("Typeahead", () => {
    let mockConsoleLog: ReturnType<typeof vi.fn>;
    let dispatchEventSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
        // Mock console.log
        mockConsoleLog = vi.fn();
        vi.spyOn(console, "log").mockImplementation(mockConsoleLog);

        // Mock window.dispatchEvent
        dispatchEventSpy = vi
            .spyOn(window, "dispatchEvent")
            .mockImplementation(() => true);

        // Reset all mocks before each test
        vi.clearAllMocks();

        // Setup DOM elements with the expected structure
        document.body.innerHTML = `
            <div id="search">
                <input type="text" id="typeahead" class="typeahead" />
                <div class="clear"></div>
            </div>
        `;

        // Mock document.getElementById
        vi.spyOn(document, "getElementById").mockImplementation((id) => {
            if (id === "typeahead") {
                return document.querySelector("#typeahead");
            }
            return null;
        });
    });

    afterEach(() => {
        document.body.innerHTML = "";
        vi.restoreAllMocks();
    });

    it("should initialize typeahead with correct configuration", () => {
        initTypeahead();

        // Verify that getElementById was called with the right ID
        expect(document.getElementById).toHaveBeenCalledWith("typeahead");

        // Get the mocked jQuery function imported from the mocked module
        const mockedJQuery = jQuery as unknown as ReturnType<typeof vi.fn>;

        // Verify that jQuery was called (but don't check arguments since they're DOM elements)
        expect(mockedJQuery).toHaveBeenCalled();

        // Get the jQuery result
        const mockJQueryResult = mockedJQuery.mock.results[0]
            .value as MockJQueryResult;

        // Verify typeahead was called with correct config
        expect(mockJQueryResult.typeahead).toHaveBeenCalledWith(
            expect.objectContaining({
                hint: false,
                highlight: true,
                minLength: 4,
            }),
            expect.any(Object),
        );
    });

    it("should setup event listeners", () => {
        // Spy on the addEventListener method
        const addEventListenerSpy = vi.spyOn(
            Element.prototype,
            "addEventListener",
        );

        initTypeahead();

        // Check if addEventListener was called with 'click' (for the clear button)
        expect(addEventListenerSpy).toHaveBeenCalledWith(
            "click",
            expect.any(Function),
        );
    });

    it("should handle keyboard events", () => {
        // Setup a mock for document.querySelector to return our input element
        const mockDispatchEvent = vi.fn();
        const mockAddEventListener = vi.fn();

        // Mock the querySelector to return a mock element
        vi.spyOn(document, "querySelector").mockImplementation((selector) => {
            if (selector === "#typeahead") {
                return {
                    dispatchEvent: mockDispatchEvent,
                    addEventListener: mockAddEventListener,
                    blur: vi.fn(),
                } as unknown as Element;
            }
            return null;
        });

        initTypeahead();

        // Verify that the clear button event listener was set up
        const clearButton = document.querySelector("#search .clear");
        expect(document.querySelector).toHaveBeenCalledWith("#search .clear");
    });

    it("should log error if typeahead element is not found", () => {
        // Mock getElementById to return null
        vi.spyOn(document, "getElementById").mockReturnValue(null);

        initTypeahead();

        // Verify console.log was called with error message
        expect(mockConsoleLog).toHaveBeenCalledWith(
            "Error - Typeahead missing input element",
        );
    });

    it("should handle keyboard event with Enter key", () => {
        initTypeahead();

        // Get the mocked jQuery function
        const mockedJQuery = jQuery as unknown as ReturnType<typeof vi.fn>;
        const mockJQueryResult = mockedJQuery.mock.results[0]
            .value as MockJQueryResult;

        // Simulate a keydown event with Enter key
        const enterKeyEvent: KeyboardEventLike = {
            key: "Enter",
            preventDefault: vi.fn(),
        };

        // Get the callback function that was registered for the keydown event
        const calls = mockJQueryResult.on.mock.calls as CallbackArray[];
        const keydownCall = calls.find((call) => call[0] === "keydown");

        if (!keydownCall) {
            throw new Error("Keydown event handler not found");
        }

        const keydownCallback = keydownCall[1];

        // Call the keydown callback with the Enter key event
        keydownCallback(enterKeyEvent);

        // Enter key should prevent default and trigger navigation
        expect(enterKeyEvent.preventDefault).toHaveBeenCalled();
    });

    it("should handle keyboard event with Escape key", () => {
        initTypeahead();

        // Get the mocked jQuery function
        const mockedJQuery = jQuery as unknown as ReturnType<typeof vi.fn>;
        const mockJQueryResult = mockedJQuery.mock.results[0]
            .value as MockJQueryResult;

        // Simulate a keydown event with Escape key
        const escapeKeyEvent: KeyboardEventLike = {
            key: "Escape",
        };

        // Get the callback function that was registered for the keydown event
        const calls = mockJQueryResult.on.mock.calls as CallbackArray[];
        const keydownCall = calls.find((call) => call[0] === "keydown");

        if (!keydownCall) {
            throw new Error("Keydown event handler not found");
        }

        const keydownCallback = keydownCall[1];

        // Call the keydown callback with the Escape key event
        keydownCallback(escapeKeyEvent);

        // Escape key should close the typeahead
        expect(mockJQueryResult.typeahead).toHaveBeenCalledWith("close");
    });

    it("should handle typeahead:autocomplete event", () => {
        initTypeahead();

        // Get the mocked jQuery function
        const mockedJQuery = jQuery as unknown as ReturnType<typeof vi.fn>;
        const mockJQueryResult = mockedJQuery.mock.results[0]
            .value as MockJQueryResult;

        // Simulate a typeahead:autocomplete event
        const eventBase = {};
        const datum: SearchResult = { key: "test-key", name: "Test Entity" };

        // Get the callback function that was registered for the typeahead:autocomplete event
        const calls = mockJQueryResult.on.mock.calls as CallbackArray[];
        const autocompleteCall = calls.find(
            (call) => call[0] === "typeahead:autocomplete",
        );

        if (!autocompleteCall) {
            throw new Error("Autocomplete event handler not found");
        }

        const autocompleteCallback = autocompleteCall[1];

        // Call the autocomplete callback
        autocompleteCallback(eventBase, datum);

        // Should set the selectedKey data
        expect(mockJQueryResult.data).toHaveBeenCalledWith(
            "selectedKey",
            "test-key",
        );
    });

    it("should handle typeahead:render event with suggestion", () => {
        initTypeahead();

        // Get the mocked jQuery function
        const mockedJQuery = jQuery as unknown as ReturnType<typeof vi.fn>;
        const mockJQueryResult = mockedJQuery.mock.results[0]
            .value as MockJQueryResult;

        // Simulate a typeahead:render event with suggestion
        const eventBase = {};
        const suggestion: SearchResult = {
            key: "test-key",
            name: "Test Entity",
        };

        // Get the callback function that was registered for the typeahead:render event
        const calls = mockJQueryResult.on.mock.calls as CallbackArray[];
        const renderCall = calls.find((call) => call[0] === "typeahead:render");

        if (!renderCall) {
            throw new Error("Render event handler not found");
        }

        const renderCallback = renderCall[1];

        // Call the render callback with suggestion
        renderCallback(eventBase, suggestion);

        // Should set the selectedKey data
        expect(mockJQueryResult.data).toHaveBeenCalledWith(
            "selectedKey",
            "test-key",
        );
    });

    it("should handle typeahead:render event without suggestion", () => {
        initTypeahead();

        // Get the mocked jQuery function
        const mockedJQuery = jQuery as unknown as ReturnType<typeof vi.fn>;
        const mockJQueryResult = mockedJQuery.mock.results[0]
            .value as MockJQueryResult;

        // Simulate a typeahead:render event without suggestion
        const eventBase = {};
        const suggestion = undefined;

        // Get the callback function that was registered for the typeahead:render event
        const calls = mockJQueryResult.on.mock.calls as CallbackArray[];
        const renderCall = calls.find((call) => call[0] === "typeahead:render");

        if (!renderCall) {
            throw new Error("Render event handler not found");
        }

        const renderCallback = renderCall[1];

        // Call the render callback without suggestion
        renderCallback(eventBase, suggestion);

        // Should set the selectedKey data to null
        expect(mockJQueryResult.data).toHaveBeenCalledWith("selectedKey", null);
    });

    it("should handle typeahead:selected event", () => {
        // Mock the DOM element's blur method
        const mockBlurFn = vi.fn();
        const typeaheadElement = document.querySelector(
            "#typeahead",
        ) as unknown as HTMLInputElement;
        if (typeaheadElement) {
            typeaheadElement.blur = mockBlurFn;
        }

        initTypeahead();

        // Get the mocked jQuery function
        const mockedJQuery = jQuery as unknown as ReturnType<typeof vi.fn>;
        const mockJQueryResult = mockedJQuery.mock.results[0]
            .value as MockJQueryResult;

        // Set up data mock to return a key when accessed
        mockJQueryResult.data.mockImplementation((key) => {
            if (key === "selectedKey") return "test-key";
            return null;
        });

        // Simulate a typeahead:selected event
        const eventBase = {};
        const datum: SearchResult = { key: "test-key", name: "Test Entity" };

        // Get the callback function that was registered for the typeahead:selected event
        const calls = mockJQueryResult.on.mock.calls as CallbackArray[];
        const selectedCall = calls.find(
            (call) => call[0] === "typeahead:selected",
        );

        if (!selectedCall) {
            throw new Error("Selected event handler not found");
        }

        const selectedCallback = selectedCall[1];

        // Call the selected callback
        selectedCallback(eventBase, datum);

        // Should set the selectedKey data
        expect(mockJQueryResult.data).toHaveBeenCalledWith(
            "selectedKey",
            "test-key",
        );

        // Should close the typeahead
        expect(mockJQueryResult.typeahead).toHaveBeenCalledWith("close");

        // Should blur the input element (DOM element, not jQuery)
        expect(mockBlurFn).toHaveBeenCalled();

        // Should dispatch a RequestNetworkEvent
        expect(RequestNetworkEvent).toHaveBeenCalledWith("test-key", true);
        expect(dispatchEventSpy).toHaveBeenCalled();
    });

    it("should clear the input when clear button is clicked", () => {
        // Create a spy for the addEventListener
        const addEventListenerSpy = vi.fn();

        // Mock the querySelector to return a mock element with addEventListener spy
        const originalQuerySelector = document.querySelector;
        vi.spyOn(document, "querySelector").mockImplementation((selector) => {
            if (selector === "#search .clear") {
                return {
                    addEventListener: addEventListenerSpy,
                } as unknown as Element;
            }
            return originalQuerySelector.call(
                document,
                selector,
            ) as Element | null;
        });

        initTypeahead();

        // Get the mocked jQuery function
        const mockedJQuery = jQuery as unknown as ReturnType<typeof vi.fn>;
        const mockJQueryResult = mockedJQuery.mock.results[0]
            .value as MockJQueryResult;

        // Verify addEventListener was called
        expect(addEventListenerSpy).toHaveBeenCalledWith(
            "click",
            expect.any(Function),
        );

        // Get the click handler that was registered
        const clickHandler = addEventListenerSpy.mock.calls[0][1] as (
            event: MouseEvent,
        ) => void;

        // Simulate a click on the clear button
        clickHandler({} as MouseEvent);

        // Should call typeahead with val and empty string
        expect(mockJQueryResult.typeahead).toHaveBeenCalledWith("val", "");
    });
});
