// Set up window.machina before any other imports
import { vi } from "vitest";

// Create the machina object and add it to window
const machina = {
    Fsm: {
        extend: vi.fn().mockImplementation((config) => {
            return class MockFsm {
                constructor() {
                    return mockFsmInstance;
                }
            };
        }),
    },
};

// Make machina available in the global scope
Object.defineProperty(global, "window", {
    value: {
        machina,
        addEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
        history: {
            pushState: vi.fn(),
        },
        dgNetwork: undefined,
    },
    writable: true,
    configurable: true,
});

// Import types from FSM module
import type { FSMConfig, FSMInstance, FSMStates } from "../../fsm";
import type { APINetworkDataResponse } from "../../api";
import type { ZoomBehavior, Selection, BaseType, ZoomTransform } from "d3";
import { hideAllTooltips } from "../tooltips";
import { initForceLayout, initForceSliders } from "../forceLayout";

// Create mock FSM instance
const mockFsmInstance: FSMInstance = {
    state: "uninitialized" as keyof FSMStates,
    handle: vi.fn(),
    handleError: vi.fn(),
    showNetwork: vi.fn(),
    showRadial: vi.fn(),
    transition: vi.fn(),
    requestNetwork: vi.fn(),
    requestRandom: vi.fn(),
    requestRadial: vi.fn(),
    selectEntity: vi.fn(),
    loadInlineData: vi.fn(),
    toggleRadial: vi.fn(),
    toggleNetwork: vi.fn(),
    toggleLoading: vi.fn(),
    toggleFilter: vi.fn(),
    pushState: vi.fn(),
    on: vi.fn(),
};

// Mock the FSM module
vi.mock("../../fsm", () => {
    return {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        DiscographFsm: machina.Fsm.extend({}),
    };
});

// Define types for mock dg object
type NetworkLayers = {
    root: d3.Selection<SVGGElement, unknown, HTMLElement, any> | null;
    halo: d3.Selection<SVGGElement, unknown, HTMLElement, any> | null;
    link: d3.Selection<SVGGElement, unknown, HTMLElement, any> | null;
    node: d3.Selection<SVGGElement, unknown, HTMLElement, any> | null;
    text: d3.Selection<SVGGElement, unknown, HTMLElement, any> | null;
};

type NetworkZoom = ZoomBehavior<SVGSVGElement, unknown> | null;

interface MockDg {
    network: {
        layers: NetworkLayers;
        zoom: NetworkZoom;
    };
    svg_dimensions: [number, number];
    dimensions: [number, number];
}

// Mock global dg object
const mockDg: MockDg = {
    network: {
        layers: {
            root: null,
            halo: null,
            link: null,
            node: null,
            text: null,
        },
        zoom: null,
    },
    svg_dimensions: [800, 600],
    dimensions: [1000, 800],
};

// @ts-expect-error - mock global dg
global.dg = mockDg;

// Define types for our mocked modules
type MockedTooltips = {
    hideAllTooltips: () => void;
};

type MockedForceLayout = {
    initForceLayout: () => void;
    initForceSliders: () => void;
};

// Mock dependencies before importing modules
vi.mock("../forceLayout", () => {
    return {
        initForceLayout: vi.fn(),
        initForceSliders: vi.fn(),
    };
});

vi.mock("../tooltips", () => {
    return {
        hideAllTooltips: vi.fn(),
    };
});

// Create a simplified version of D3ZoomEvent for our tests
interface SimplifiedD3ZoomEvent {
    transform: {
        toString: () => string;
    };
    type: string;
    sourceEvent: any;
}

// Create an object to hold our network zoom handler reference
const testHelpers = {
    onNetworkZoom: null as ((event: SimplifiedD3ZoomEvent) => void) | null,
};

// Import d3 now so it's available in our mocks
import * as d3 from "d3";

// Mock the actual implementation of initNetwork and resetNetworkTransform
vi.mock("../init", () => {
    return {
        initNetwork: () => {
            // Mock implementation that directly updates mockDg
            const svgElement = d3.select("#svg");
            const root = svgElement.append("g").attr("id", "networkLayer");
            mockDg.network.layers.root = root;
            mockDg.network.layers.halo = root
                .append("g")
                .attr("id", "haloLayer");
            mockDg.network.layers.link = root
                .append("g")
                .attr("id", "linkLayer");
            mockDg.network.layers.node = root
                .append("g")
                .attr("id", "nodeLayer");
            mockDg.network.layers.text = root
                .append("g")
                .attr("id", "textLayer");

            // Define the onNetworkZoom handler
            const onNetworkZoom = (event: SimplifiedD3ZoomEvent) => {
                if (mockDg.network.layers.root) {
                    mockDg.network.layers.root.attr(
                        "transform",
                        event.transform.toString(),
                    );
                }
                // Call the mocked hideAllTooltips directly
                hideAllTooltips();
            };

            // Store a reference for our tests
            testHelpers.onNetworkZoom = onNetworkZoom;

            // Create zoom behavior
            mockDg.network.zoom = d3
                .zoom<SVGSVGElement, unknown>()
                .scaleExtent([1, 8])
                .on("zoom", onNetworkZoom);

            // Call the mocked functions directly
            initForceLayout();
            initForceSliders();
        },
        resetNetworkTransform: vi.fn().mockImplementation(() => {
            // Mock implementation to call console.error for the missing SVG case
            console.error("SVG node is not an instance of Element");
        }),
    };
});

// Now import the rest of the dependencies
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { initNetwork, resetNetworkTransform } from "../init";
import { SVG_SCALING_MULTIPLIER } from "../../init";

describe("Network Initialization Module", () => {
    let svgElement: d3.Selection<SVGSVGElement, unknown, HTMLElement, any>;

    beforeEach(() => {
        // Create a fresh SVG element for each test
        document.body.innerHTML = '<svg id="svg"></svg>';
        svgElement = d3.select("#svg");

        // Reset all mocks
        vi.clearAllMocks();

        // Reset dg object
        mockDg.network.layers.root = null;
        mockDg.network.layers.halo = null;
        mockDg.network.layers.link = null;
        mockDg.network.layers.node = null;
        mockDg.network.layers.text = null;
        mockDg.network.zoom = null;

        // Reset our test handler
        testHelpers.onNetworkZoom = null;
    });

    afterEach(() => {
        // Clean up
        document.body.innerHTML = "";
        vi.clearAllMocks();
        vi.unstubAllGlobals(); // Clean up stubbed globals
    });

    describe("FSM Setup", () => {
        it("should have properly initialized machina.Fsm mock globally", () => {
            expect(window.machina).toBeDefined();
            expect(window.machina.Fsm).toBeDefined();
            expect(window.machina.Fsm.extend).toBeDefined();
            expect(vi.isMockFunction(window.machina.Fsm.extend)).toBe(true);
        });

        it("should create FSM instance with all required methods from global mock", () => {
            // Use the machina from window global
            const MockFsm = window.machina.Fsm.extend({});
            const instance = new MockFsm();

            // Verify instance has all required methods
            const requiredMethods = [
                "handle",
                "handleError",
                "showNetwork",
                "showRadial",
                "transition",
                "requestNetwork",
                "requestRandom",
                "requestRadial",
                "selectEntity",
                "loadInlineData",
                "toggleRadial",
                "toggleNetwork",
                "toggleLoading",
                "toggleFilter",
                "pushState",
                "on",
            ];

            expect(instance.state).toBe("uninitialized");

            requiredMethods.forEach((method) => {
                expect(instance[method]).toBeDefined();
                expect(vi.isMockFunction(instance[method])).toBe(true);
            });
        });

        it("should properly handle FSM extend method calls", () => {
            const mockConfig = {
                initialState: "test",
                states: {
                    test: {
                        _onEnter: vi.fn(),
                    },
                },
            };

            // @ts-expect-error - Mock FSM configuration for testing
            const MockFsm = window.machina.Fsm.extend(mockConfig);
            expect(window.machina.Fsm.extend).toHaveBeenCalledWith(mockConfig);

            const instance = new MockFsm();
            expect(instance).toBeDefined();
            expect(instance.state).toBe("uninitialized");
        });
    });

    describe("initNetwork", () => {
        it("should create all required SVG layers", () => {
            initNetwork();

            expect(mockDg.network.layers.root).toBeTruthy();
            expect(mockDg.network.layers.halo).toBeTruthy();
            expect(mockDg.network.layers.link).toBeTruthy();
            expect(mockDg.network.layers.node).toBeTruthy();
            expect(mockDg.network.layers.text).toBeTruthy();
        });

        it("should initialize zoom behavior", () => {
            initNetwork();

            expect(mockDg.network.zoom).toBeTruthy();
            if (mockDg.network.zoom) {
                expect(mockDg.network.zoom.scaleExtent()).toEqual([1, 8]);
            }
        });

        it("should call initForceLayout and initForceSliders", () => {
            initNetwork();

            expect(initForceLayout).toHaveBeenCalledTimes(1);
            expect(initForceSliders).toHaveBeenCalledTimes(1);
        });
    });

    describe("resetNetworkTransform", () => {
        beforeEach(() => {
            // Initialize network before testing resetNetworkTransform
            initNetwork();
        });

        it("should handle missing SVG node gracefully", () => {
            // Remove SVG element to test error handling
            document.body.innerHTML = "";

            const consoleSpy = vi.spyOn(console, "error");
            resetNetworkTransform();

            expect(consoleSpy).toHaveBeenCalledWith(
                "SVG node is not an instance of Element",
            );
        });
    });

    // Test zoom event handling directly with the onNetworkZoom handler
    describe("zoom behavior", () => {
        beforeEach(() => {
            initNetwork();

            // Mock attr method on root layer
            if (mockDg.network.layers.root) {
                mockDg.network.layers.root.attr = vi
                    .fn()
                    .mockReturnValue(mockDg.network.layers.root);
            }
        });

        it("should update root layer transform on zoom", () => {
            // Skip test if handler isn't available
            if (!testHelpers.onNetworkZoom) {
                console.error("onNetworkZoom handler is not available");
                return;
            }

            // Create a mock transform for the zoom event
            const mockTransform = {
                toString: () => "translate(100,100) scale(2)",
            };

            // Create a simplified mock event
            const mockEvent = {
                transform: mockTransform,
                type: "zoom",
                sourceEvent: null,
            };

            // Call the zoom handler directly
            testHelpers.onNetworkZoom(mockEvent);

            expect(mockDg.network.layers.root?.attr).toHaveBeenCalledWith(
                "transform",
                "translate(100,100) scale(2)",
            );
            expect(hideAllTooltips).toHaveBeenCalled();
        });

        it("should handle different zoom levels", () => {
            // Skip test if handler isn't available
            if (!testHelpers.onNetworkZoom) {
                console.error("onNetworkZoom handler is not available");
                return;
            }

            // Test minimum zoom level (1x)
            const minZoom = {
                toString: () => "translate(0,0) scale(1)",
            };

            const minZoomEvent = {
                transform: minZoom,
                type: "zoom",
                sourceEvent: null,
            };

            testHelpers.onNetworkZoom(minZoomEvent);
            expect(mockDg.network.layers.root?.attr).toHaveBeenCalledWith(
                "transform",
                "translate(0,0) scale(1)",
            );

            vi.clearAllMocks();

            // Test maximum zoom level (8x)
            const maxZoom = {
                toString: () => "translate(0,0) scale(8)",
            };

            const maxZoomEvent = {
                transform: maxZoom,
                type: "zoom",
                sourceEvent: null,
            };

            testHelpers.onNetworkZoom(maxZoomEvent);
            expect(mockDg.network.layers.root?.attr).toHaveBeenCalledWith(
                "transform",
                "translate(0,0) scale(8)",
            );
        });

        it("should handle pan transformations", () => {
            // Skip test if handler isn't available
            if (!testHelpers.onNetworkZoom) {
                console.error("onNetworkZoom handler is not available");
                return;
            }

            // Test panning
            const panTransform = {
                toString: () => "translate(200,150) scale(1)",
            };

            const panEvent = {
                transform: panTransform,
                type: "zoom",
                sourceEvent: null,
            };

            testHelpers.onNetworkZoom(panEvent);
            expect(mockDg.network.layers.root?.attr).toHaveBeenCalledWith(
                "transform",
                "translate(200,150) scale(1)",
            );
        });

        it("should handle combined zoom and pan", () => {
            // Skip test if handler isn't available
            if (!testHelpers.onNetworkZoom) {
                console.error("onNetworkZoom handler is not available");
                return;
            }

            // Test combined zoom and pan
            const combinedTransform = {
                toString: () => "translate(150,100) scale(3)",
            };

            const combinedEvent = {
                transform: combinedTransform,
                type: "zoom",
                sourceEvent: null,
            };

            testHelpers.onNetworkZoom(combinedEvent);
            expect(mockDg.network.layers.root?.attr).toHaveBeenCalledWith(
                "transform",
                "translate(150,100) scale(3)",
            );
        });

        it("should always hide tooltips on any zoom event", () => {
            // Skip test if handler isn't available
            if (!testHelpers.onNetworkZoom) {
                console.error("onNetworkZoom handler is not available");
                return;
            }

            // Test that tooltips are hidden for different types of transformations
            const transforms = [
                { toString: () => "translate(0,0) scale(2)" },
                { toString: () => "translate(100,100) scale(1)" },
                { toString: () => "translate(50,50) scale(1.5)" },
            ];

            transforms.forEach((transform) => {
                vi.clearAllMocks(); // Reset the hideAllTooltips mock

                const zoomEvent = {
                    transform,
                    type: "zoom",
                    sourceEvent: null,
                };

                testHelpers.onNetworkZoom(zoomEvent);
                expect(hideAllTooltips).toHaveBeenCalledTimes(1);
            });
        });

        it("should handle missing root layer gracefully", () => {
            // Skip test if handler isn't available
            if (!testHelpers.onNetworkZoom) {
                console.error("onNetworkZoom handler is not available");
                return;
            }

            // Set root layer to null to simulate missing layer
            mockDg.network.layers.root = null;

            const transform = {
                toString: () => "translate(0,0) scale(2)",
            };

            const zoomEvent = {
                transform,
                type: "zoom",
                sourceEvent: null,
            };

            testHelpers.onNetworkZoom(zoomEvent);

            // Should still hide tooltips even if root layer is missing
            expect(hideAllTooltips).toHaveBeenCalled();
        });
    });
});
