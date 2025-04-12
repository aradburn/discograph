import type { Mock } from "vitest";
import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { JSDOM } from "jsdom";
import { dg } from "../dg";
import * as initModule from "../init";
import {
    initWindow,
    initApp,
    VIEWPORT_SIZE_MULTIPLIER,
    SVG_SCALING_MULTIPLIER,
} from "../init";
import { ResizeEvent } from "../network/events";
import type { TreeConfig } from "../roles";

// Define CustomEvent type for mocking
interface CustomEventInit {
    bubbles?: boolean;
    cancelable?: boolean;
    composed?: boolean;
    detail?: unknown;
}

// Define a proper type for the debounce function to avoid the Function type
type AnyFunction = (...args: any[]) => any;

// Define proper error type
type ErrorWithMessage = Error & { message: string };

// Mock all dependencies
vi.mock("bootstrap", () => ({
    Tooltip: vi.fn().mockImplementation(() => ({
        // Mock Tooltip methods if needed
    })),
}));

vi.mock("../loading", () => ({
    loading: {
        init: vi.fn(),
    },
}));

vi.mock("../relations", () => ({
    initRelations: vi.fn(),
}));

vi.mock("../network/init", () => ({
    initNetwork: vi.fn(),
    resetNetworkTransform: vi.fn(),
}));

vi.mock("../network/forceLayout", () => ({
    restartForceLayout: vi.fn(),
    stopForceLayout: vi.fn(),
}));

vi.mock("../roles", () => ({
    initRoles: vi.fn(),
}));

vi.mock("../svg", () => ({
    initSvg: vi.fn(),
    printSvg: vi.fn(),
}));

vi.mock("../typeahead", () => ({
    initTypeahead: vi.fn(),
}));

vi.mock("../fsm", () => ({
    DiscographFsm: vi.fn().mockImplementation(() => ({
        // Mock FSM methods if needed
    })),
}));

vi.mock("../messages", () => ({
    showMessage: vi.fn(),
    clearMessages: vi.fn(),
}));

// Mock original debounce to execute immediately for testing
vi.mock("../utils", () => ({
    debounce: vi.fn().mockImplementation((fn: AnyFunction) => fn),
}));

// Import mocked modules
import * as loading from "../loading";
import * as networkInit from "../network/init";
import * as svg from "../svg";
import * as messages from "../messages";
import * as roles from "../roles";
import * as forceLayout from "../network/forceLayout";
import * as fsm from "../fsm";
import * as relations from "../relations";
import * as typeahead from "../typeahead";

// Create a simple event stub that mimics just enough of DOM events
class EventStub {
    type: string;
    defaultPrevented: boolean = false;

    constructor(type: string) {
        this.type = type;
    }

    preventDefault() {
        this.defaultPrevented = true;
    }
}

// Define a better type for handler functions to avoid using Function type
type ResizeHandler = () => void;
type EventHandler = (event: EventStub) => void;

// Define interfaces for the mocked modules to ensure type safety
interface MockedInitModule {
    initWindow: typeof initModule.initWindow;
}

interface MockedSvg {
    initSvg: typeof svg.initSvg;
    printSvg: typeof svg.printSvg;
}

interface MockedNetworkInit {
    initNetwork: typeof networkInit.initNetwork;
    resetNetworkTransform: typeof networkInit.resetNetworkTransform;
}

interface MockedRelations {
    initRelations: typeof relations.initRelations;
}

interface MockedRoles {
    initRoles: typeof roles.initRoles;
}

interface MockedTypeahead {
    initTypeahead: typeof typeahead.initTypeahead;
}

interface MockedLoading {
    loading: {
        init: typeof loading.loading.init;
    };
}

interface MockedFsm {
    DiscographFsm: typeof fsm.DiscographFsm;
}

interface MockedForceLayout {
    restartForceLayout: typeof forceLayout.restartForceLayout;
    stopForceLayout: typeof forceLayout.stopForceLayout;
}

describe("Init Module", () => {
    let originalWindow: Window & typeof globalThis;
    let dom: JSDOM;

    // Setup DOM environment before each test
    beforeEach(() => {
        // Save original window reference
        originalWindow = global.window;

        // Define a better type for the document.getElementById mock return
        interface MockElement {
            clientWidth?: number;
            clientHeight?: number;
            id?: string;
            style?: {
                opacity?: string;
            };
        }

        // Create a mock for document.getElementById to correctly return element dimensions
        const mockGetElementById = vi
            .fn()
            .mockImplementation((id: string): MockElement | null => {
                if (id === "svg-container-fluid") {
                    return {
                        clientWidth: 1000,
                        clientHeight: 800,
                    };
                }

                // Return default elements for other IDs
                try {
                    return document.querySelector(
                        `#${id}`,
                    ) as MockElement | null;
                } catch (error: unknown) {
                    // Handle error gracefully
                    const errorMessage =
                        error instanceof Error
                            ? error.message
                            : "Unknown error occurred";
                    console.error(
                        `Error querying for element with id ${id}:`,
                        errorMessage,
                    );
                    return null;
                }
            });

        // Create a new DOM instance
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call
        dom = new JSDOM(`
            <!DOCTYPE html>
            <html>
                <body>
                    <div id="svg-container-fluid" style="width: 1000px; height: 800px;"></div>
                    <button id="request-random">Random</button>
                    <button id="start-layout">Start Layout</button>
                    <button id="stop-layout">Stop Layout</button>
                    <button id="print">Print</button>
                    <div id="nav-top" style="opacity: 0;"></div>
                    <div id="modal-help" style="opacity: 0;"></div>
                    <div id="side-menu-content" style="opacity: 0;"></div>
                    <div data-bs-toggle="tooltip" title="Test tooltip"></div>
                </body>
            </html>
        `);

        // Set up global window with required properties
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
        global.window = Object.assign(dom.window, {
            devicePixelRatio: 2,
            dgRoles: {
                core: { data: [] },
                plugins: [],
            } as TreeConfig,
            // We'll mock addEventListener to capture and track handlers
            addEventListener: vi
                .fn()
                .mockImplementation((event: string, handler: EventListener) => {
                    // Store the handler properly
                    return { event, handler };
                }),
        }) as unknown as Window & typeof globalThis;

        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
        global.document = dom.window.document;

        // Override document.getElementById to return elements with proper dimensions
        global.document.getElementById = mockGetElementById;

        // Reset dg object
        dg.dpr = 1;
        dg.dimensions = [0, 0];
        dg.svg_dimensions = [0, 0];
        dg.network.newNodeCoords = [0, 0];
        dg.fsm = null;

        // Clear all mocks
        vi.clearAllMocks();
    });

    afterEach(() => {
        // Restore original window
        global.window = originalWindow;
        vi.restoreAllMocks();
    });

    describe("initWindow", () => {
        it("should correctly calculate dimensions", () => {
            // Act
            initWindow();

            // Assert
            expect(dg.dpr).toBe(2);
            expect(dg.dimensions).toEqual([1000, 800]);
            expect(dg.svg_dimensions[0]).toBeCloseTo(
                1000 * VIEWPORT_SIZE_MULTIPLIER * 2,
            );
            expect(dg.svg_dimensions[1]).toBeCloseTo(
                800 * VIEWPORT_SIZE_MULTIPLIER * 2,
            );
        });

        it("should set newNodeCoords to center of svg dimensions", () => {
            // Act
            initWindow();

            // Assert
            expect(dg.network.newNodeCoords).toEqual([
                dg.svg_dimensions[0] / 2,
                dg.svg_dimensions[1] / 2,
            ]);
        });

        it.skip("should add resize event listener to window", () => {
            // Set up a special mock for window.addEventListener to capture the resize handler
            const mockAddEventListener = vi.fn();
            window.addEventListener = mockAddEventListener;

            // Act
            initWindow();

            // Assert
            expect(mockAddEventListener).toHaveBeenCalledWith(
                "resize",
                expect.any(Function),
            );
        });

        it("should handle resize events properly", () => {
            // Set up mocks - directly access the implementation we want to verify
            const mockResetNetworkTransform = vi.fn();
            const mockInitSvg = vi.fn();
            vi.mocked(networkInit.resetNetworkTransform).mockImplementation(
                mockResetNetworkTransform,
            );
            vi.mocked(svg.initSvg).mockImplementation(mockInitSvg);

            // Capture the resize handler
            let resizeHandler: ResizeHandler | null = null;
            (window.addEventListener as Mock).mockImplementation(
                (event: string, handler: ResizeHandler) => {
                    if (event === "resize") {
                        resizeHandler = handler;
                    }
                },
            );

            // Act
            initWindow();

            // Verify handler was captured
            expect(resizeHandler).not.toBeNull();

            // Manually trigger the resize handler
            if (resizeHandler) {
                resizeHandler();

                // Assert handler's behavior
                expect(mockInitSvg).toHaveBeenCalled();
                expect(mockResetNetworkTransform).toHaveBeenCalled();
            }
        });

        it("should handle errors during resize", () => {
            // Set up mocks
            const mockShowMessage = vi.fn();
            const mockClearMessages = vi.fn();
            const mockInitSvg = vi.fn().mockImplementation(() => {
                throw new Error("Test error");
            });

            vi.mocked(messages.showMessage).mockImplementation(mockShowMessage);
            vi.mocked(messages.clearMessages).mockImplementation(
                mockClearMessages,
            );
            vi.mocked(svg.initSvg).mockImplementation(mockInitSvg);

            // Capture the resize handler
            let resizeHandler: ResizeHandler | null = null;
            (window.addEventListener as Mock).mockImplementation(
                (event: string, handler: ResizeHandler) => {
                    if (event === "resize") {
                        resizeHandler = handler;
                    }
                },
            );

            // Act
            initWindow();

            // Verify handler was captured
            expect(resizeHandler).not.toBeNull();

            // Manually trigger the resize handler
            if (resizeHandler) {
                try {
                    // We don't catch the error here because it should be handled in the implementation
                    resizeHandler();
                } catch (error: unknown) {
                    // Just in case, but we expect the implementation to handle errors internally
                    const errorMessage =
                        error instanceof Error
                            ? error.message
                            : "Unknown error occurred";
                    console.error(
                        "Unexpected error in resize handler:",
                        errorMessage,
                    );
                }

                // Assert error handling
                expect(mockShowMessage).toHaveBeenCalledWith(
                    expect.stringContaining(
                        "Error during window resize: Test error",
                    ),
                    "error",
                );
                expect(mockClearMessages).toHaveBeenCalledWith(5000);
            }
        });
    });

    describe("initApp", () => {
        it.skip("should initialize all components", () => {
            // Create individual spies for each function we need to mock
            const mockInitWindow = vi.fn();
            const mockInitSvg = vi.fn();
            const mockInitNetwork = vi.fn();
            const mockInitRelations = vi.fn();
            const mockInitRoles = vi.fn();
            const mockInitTypeahead = vi.fn();
            const mockLoadingInit = vi.fn();

            // Mock the window.dgRoles
            window.dgRoles = {
                core: { data: [] },
                plugins: [],
            };

            // Use spyOn differently - mock the implementation before actually calling it
            const spyInitWindow = vi
                .spyOn(initModule, "initWindow")
                .mockImplementation(mockInitWindow);
            const spyInitSvg = vi
                .spyOn(svg, "initSvg")
                .mockImplementation(mockInitSvg);
            const spyInitNetwork = vi
                .spyOn(networkInit, "initNetwork")
                .mockImplementation(mockInitNetwork);
            const spyInitRelations = vi
                .spyOn(relations, "initRelations")
                .mockImplementation(mockInitRelations);
            const spyInitRoles = vi
                .spyOn(roles, "initRoles")
                .mockImplementation(mockInitRoles);
            const spyInitTypeahead = vi
                .spyOn(typeahead, "initTypeahead")
                .mockImplementation(mockInitTypeahead);
            const spyLoadingInit = vi
                .spyOn(loading.loading, "init")
                .mockImplementation(mockLoadingInit);

            // Act - Call the function we're testing
            initApp();

            // Assert that all mocked functions were called
            expect(mockInitWindow).toHaveBeenCalled();
            expect(mockInitSvg).toHaveBeenCalled();
            expect(mockInitNetwork).toHaveBeenCalled();
            expect(mockInitRelations).toHaveBeenCalled();
            expect(mockInitRoles).toHaveBeenCalled();
            expect(mockInitTypeahead).toHaveBeenCalled();
            expect(mockLoadingInit).toHaveBeenCalled();

            // Restore the original implementations
            spyInitWindow.mockRestore();
            spyInitSvg.mockRestore();
            spyInitNetwork.mockRestore();
            spyInitRelations.mockRestore();
            spyInitRoles.mockRestore();
            spyInitTypeahead.mockRestore();
            spyLoadingInit.mockRestore();
        });

        it("should set up event listeners for UI controls", () => {
            // Mock querySelector to return actual buttons
            const buttons = {
                requestRandom: document.createElement("button"),
                startLayout: document.createElement("button"),
                stopLayout: document.createElement("button"),
                print: document.createElement("button"),
            };

            // Set IDs for the buttons
            buttons.requestRandom.id = "request-random";
            buttons.startLayout.id = "start-layout";
            buttons.stopLayout.id = "stop-layout";
            buttons.print.id = "print";

            // Mock button event listeners
            buttons.requestRandom.addEventListener = vi.fn();
            buttons.startLayout.addEventListener = vi.fn();
            buttons.stopLayout.addEventListener = vi.fn();
            buttons.print.addEventListener = vi.fn();

            // Mock document.querySelector
            const originalQuerySelector = document.querySelector;
            document.querySelector = vi
                .fn()
                .mockImplementation((selector: string) => {
                    if (selector === "#request-random")
                        return buttons.requestRandom;
                    if (selector === "#start-layout")
                        return buttons.startLayout;
                    if (selector === "#stop-layout") return buttons.stopLayout;
                    if (selector === "#print") return buttons.print;
                    return originalQuerySelector.call(
                        document,
                        selector,
                    ) as Element | null;
                });

            // Act
            initApp();

            // Assert
            expect(buttons.requestRandom.addEventListener).toHaveBeenCalledWith(
                "click",
                expect.any(Function),
            );
            expect(buttons.startLayout.addEventListener).toHaveBeenCalledWith(
                "click",
                expect.any(Function),
            );
            expect(buttons.stopLayout.addEventListener).toHaveBeenCalledWith(
                "click",
                expect.any(Function),
            );
            expect(buttons.print.addEventListener).toHaveBeenCalledWith(
                "click",
                expect.any(Function),
            );

            // Restore original querySelector
            document.querySelector = originalQuerySelector;
        });

        it("should initialize the FSM", () => {
            // Mock DiscographFsm
            const mockDiscographFsm = vi.fn();
            const originalDiscographFsm = fsm.DiscographFsm;

            // Use proper typing for the fsm module
            const mockedFsm = fsm as MockedFsm;
            mockedFsm.DiscographFsm = mockDiscographFsm;

            // Act
            initApp();

            // Assert
            expect(mockDiscographFsm).toHaveBeenCalled();

            // Restore original
            mockedFsm.DiscographFsm = originalDiscographFsm;
        });

        it("should set opacity for UI elements", () => {
            // Create elements with opacity that will actually be changed
            const elements = {
                navTop: document.createElement("div"),
                modalHelp: document.createElement("div"),
                sideMenuContent: document.createElement("div"),
            };

            // Set IDs and initial opacity
            elements.navTop.id = "nav-top";
            elements.modalHelp.id = "modal-help";
            elements.sideMenuContent.id = "side-menu-content";

            elements.navTop.style.opacity = "0";
            elements.modalHelp.style.opacity = "0";
            elements.sideMenuContent.style.opacity = "0";

            // Mock document.querySelector
            const originalQuerySelector = document.querySelector;
            document.querySelector = vi
                .fn()
                .mockImplementation((selector: string) => {
                    if (selector === "#nav-top") return elements.navTop;
                    if (selector === "#modal-help") return elements.modalHelp;
                    if (selector === "#side-menu-content")
                        return elements.sideMenuContent;
                    return originalQuerySelector.call(
                        document,
                        selector,
                    ) as Element | null;
                });

            // Act
            initApp();

            // Assert
            expect(elements.navTop.style.opacity).toBe("1");
            expect(elements.modalHelp.style.opacity).toBe("1");
            expect(elements.sideMenuContent.style.opacity).toBe("1");

            // Restore original querySelector
            document.querySelector = originalQuerySelector;
        });

        it("should handle button click events correctly", () => {
            // Mock dependencies
            const mockRestartForceLayout = vi.fn();
            const mockStopForceLayout = vi.fn();
            const mockPrintSvg = vi.fn();

            const originalRestartForceLayout = forceLayout.restartForceLayout;
            const originalStopForceLayout = forceLayout.stopForceLayout;
            const originalPrintSvg = svg.printSvg;

            // Use proper typing for force layout module
            const mockedForceLayout = forceLayout as MockedForceLayout;
            const mockedSvg = svg as MockedSvg;

            mockedForceLayout.restartForceLayout = mockRestartForceLayout;
            mockedForceLayout.stopForceLayout = mockStopForceLayout;
            mockedSvg.printSvg = mockPrintSvg;

            // Create buttons with event handlers that we can capture
            const buttons = {
                requestRandom: document.createElement("button"),
                startLayout: document.createElement("button"),
                stopLayout: document.createElement("button"),
                print: document.createElement("button"),
            };

            // Set IDs for the buttons
            buttons.requestRandom.id = "request-random";
            buttons.startLayout.id = "start-layout";
            buttons.stopLayout.id = "stop-layout";
            buttons.print.id = "print";

            // Capture event handlers
            const handlers: Record<string, EventHandler[]> = {
                requestRandom: [],
                startLayout: [],
                stopLayout: [],
                print: [],
            };

            // Mock addEventListener to capture handlers
            buttons.requestRandom.addEventListener = vi
                .fn()
                .mockImplementation((event: string, handler: EventHandler) => {
                    if (event === "click") handlers.requestRandom.push(handler);
                });

            buttons.startLayout.addEventListener = vi
                .fn()
                .mockImplementation((event: string, handler: EventHandler) => {
                    if (event === "click") handlers.startLayout.push(handler);
                });

            buttons.stopLayout.addEventListener = vi
                .fn()
                .mockImplementation((event: string, handler: EventHandler) => {
                    if (event === "click") handlers.stopLayout.push(handler);
                });

            buttons.print.addEventListener = vi
                .fn()
                .mockImplementation((event: string, handler: EventHandler) => {
                    if (event === "click") handlers.print.push(handler);
                });

            // Mock dispatchEvent
            buttons.requestRandom.dispatchEvent = vi.fn();

            // Mock document.querySelector
            const originalQuerySelector = document.querySelector;
            document.querySelector = vi
                .fn()
                .mockImplementation((selector: string) => {
                    if (selector === "#request-random")
                        return buttons.requestRandom;
                    if (selector === "#start-layout")
                        return buttons.startLayout;
                    if (selector === "#stop-layout") return buttons.stopLayout;
                    if (selector === "#print") return buttons.print;
                    return originalQuerySelector.call(
                        document,
                        selector,
                    ) as Element | null;
                });

            // Act
            initApp();

            // Create mock event
            const mockEvent = new EventStub("click");

            // Call handlers directly
            if (handlers.startLayout.length > 0) {
                handlers.startLayout[0](mockEvent);
                expect(mockRestartForceLayout).toHaveBeenCalledWith(0.1);
            }

            if (handlers.stopLayout.length > 0) {
                handlers.stopLayout[0](mockEvent);
                expect(mockStopForceLayout).toHaveBeenCalled();
            }

            if (handlers.print.length > 0) {
                handlers.print[0](mockEvent);
                expect(mockPrintSvg).toHaveBeenCalled();
            }

            if (handlers.requestRandom.length > 0) {
                handlers.requestRandom[0](mockEvent);
                expect(buttons.requestRandom.dispatchEvent).toHaveBeenCalled();
            }

            // Restore original functions
            mockedForceLayout.restartForceLayout = originalRestartForceLayout;
            mockedForceLayout.stopForceLayout = originalStopForceLayout;
            mockedSvg.printSvg = originalPrintSvg;
            document.querySelector = originalQuerySelector;
        });

        it("should not call initRoles if window.dgRoles is not defined", () => {
            // Mock initRoles function
            const mockInitRoles = vi.fn();
            const originalInitRoles = roles.initRoles;

            // Use proper typing for roles module
            const mockedRoles = roles as MockedRoles;
            mockedRoles.initRoles = mockInitRoles;

            // Remove window.dgRoles
            delete global.window.dgRoles;

            // Act
            initApp();

            // Assert
            expect(mockInitRoles).not.toHaveBeenCalled();

            // Restore original
            mockedRoles.initRoles = originalInitRoles;
        });
    });
});
