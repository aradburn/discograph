import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import jQuery from "jquery";

// Mock CSS imports
vi.mock("~bootstrap/dist/css/bootstrap.min.css", () => ({}));
vi.mock("../css/discograph.scss", () => ({}));

// Mock jQuery
vi.mock("jquery", () => ({
    default: vi.fn(),
}));

// Mock bootstrap
vi.mock("bootstrap", () => ({
    default: vi.fn(),
}));

// Mock init - must come before importing initApp
vi.mock("../init", () => ({
    initApp: vi.fn(),
}));

// Import initApp after mocking
import { initApp } from "../init";

// Declare jQuery globals for tests
declare global {
    interface Window {
        $: typeof jQuery;
        jQuery: typeof jQuery;
    }
}

describe("index.ts", () => {
    // Save original methods
    const originalAddEventListener = document.addEventListener;

    beforeEach(() => {
        // Clear all mocks
        vi.clearAllMocks();

        // Reset the window object
        delete window.$;
        delete window.jQuery;

        // Mock document.addEventListener before importing the module
        vi.spyOn(document, "addEventListener").mockImplementation(vi.fn());
    });

    afterEach(() => {
        // Clean up mocks and restore originals
        vi.restoreAllMocks();
        // Reset the module registry to clear cached modules
        vi.resetModules();
    });

    it("should inject jQuery into global scope", async () => {
        // Import the index module
        await import("../index");

        // Check that jQuery was added to window
        expect(window.$).toBe(jQuery);
        expect(window.jQuery).toBe(jQuery);
    });

    it("should add DOMContentLoaded event listener", async () => {
        // Import the index module
        await import("../index");

        // Verify document.addEventListener was called
        expect(document.addEventListener).toHaveBeenCalledWith(
            "DOMContentLoaded",
            initApp,
        );
    });

    it("should initialize app when DOM content is loaded", async () => {
        // Import the index module
        await import("../index");

        // Get the callback from the addEventListener mock
        const domContentLoadedCallback = vi
            .mocked(document.addEventListener)
            .mock.calls.find((call) => call[0] === "DOMContentLoaded")?.[1];

        if (domContentLoadedCallback) {
            // Create a mock event
            const mockEvent = new Event("DOMContentLoaded");

            // Call the handler
            if (typeof domContentLoadedCallback === "function") {
                domContentLoadedCallback(mockEvent);
            } else if (
                domContentLoadedCallback &&
                "handleEvent" in domContentLoadedCallback
            ) {
                domContentLoadedCallback.handleEvent(mockEvent);
            }

            // Verify initApp was called
            expect(initApp).toHaveBeenCalled();
        } else {
            // This assertion will fail if the event listener was not set up
            expect(
                vi.mocked(document.addEventListener).mock.calls.length,
            ).toBeGreaterThan(0);
        }
    });
});
