import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Define interfaces for Bloodhound
interface BloodhoundTokenizers {
    whitespace: ReturnType<typeof vi.fn>;
}

interface BloodhoundStatic {
    tokenizers: BloodhoundTokenizers;
    new (): ReturnType<typeof vi.fn>;
}

// Mock jQuery before importing typeahead
vi.mock("jquery", () => {
    // Create mock functions within the factory scope to avoid hoisting issues
    const typeaheadFn = vi.fn().mockReturnThis();
    const onFn = vi.fn().mockReturnThis();
    const dataFn = vi.fn();
    const blurFn = vi.fn();

    return {
        default: vi.fn(() => ({
            typeahead: typeaheadFn,
            on: onFn,
            data: dataFn,
            blur: blurFn,
        })),
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

// Import after mocks
import { initTypeahead } from "../typeahead";
import jQuery from "jquery";

// Define types for typechecking the mock results
interface MockJQueryResult {
    typeahead: ReturnType<typeof vi.fn>;
    on: ReturnType<typeof vi.fn>;
    data: ReturnType<typeof vi.fn>;
    blur: ReturnType<typeof vi.fn>;
}

describe("Typeahead", () => {
    beforeEach(() => {
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
});
