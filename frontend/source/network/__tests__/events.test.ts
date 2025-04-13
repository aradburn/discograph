/**
 * @vitest-environment jsdom
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import type { SimNode, SimLink } from "../data";
import type * as d3 from "d3";
import { NodeType } from "../types";

// Import modules directly - we'll mock their functions with spyOn
import { nodeTooltip } from "../tooltips";
import * as forceLayout from "../forceLayout";
import * as tick from "../tick";
import { dg } from "../../dg";

// Now import the functions under test
import {
    onDragStart,
    onDrag,
    onDragEnd,
    onNetworkStart,
    onNetworkEnd,
    RequestNetworkEvent,
    SelectEntityEvent,
    ResizeEvent,
} from "../events";

type D3DragEventWithSource = d3.D3DragEvent<SVGGElement, SimNode, SimNode> & {
    sourceEvent: MouseEvent | TouchEvent;
};

// Create a minimal mock MouseEvent
const createMockMouseEvent = (type: string): MouseEvent => {
    return {
        type,
        preventDefault: vi.fn(),
        stopPropagation: vi.fn(),
    } as unknown as MouseEvent;
};

describe("Network Graph Event Handlers", () => {
    let mockNode: SimNode;
    let mockEvent: D3DragEventWithSource;

    // Setup spies before tests
    const hideTooltipSpy = vi
        .spyOn(nodeTooltip, "hide")
        .mockImplementation(() => {});
    const restartForceLayoutSpy = vi
        .spyOn(forceLayout, "restartForceLayout")
        .mockImplementation(() => {});
    const stopForceLayoutSpy = vi
        .spyOn(forceLayout, "stopForceLayout")
        .mockImplementation(() => {});
    const onTickSpy = vi.spyOn(tick, "onTick").mockImplementation(() => {});

    // Mock network properties
    const originalIsRunningLayout = dg.network.isRunningLayout;
    const originalTick = dg.network.tick;

    // Define types for mock return values
    type D3Selection = {
        classed: (className: string, value: boolean) => void;
    };

    // Setup mock selectAll functions for network layers
    const mockNodeSelectAll = vi
        .fn()
        .mockReturnValue({ classed: vi.fn() } as D3Selection);
    const mockLinkSelectAll = vi
        .fn()
        .mockReturnValue({ classed: vi.fn() } as D3Selection);

    beforeEach(() => {
        // Reset all mocks
        vi.clearAllMocks();

        // Mock the network properties
        Object.defineProperty(dg.network, "isRunningLayout", {
            get: vi.fn(() => false),
            set: vi.fn(),
            configurable: true,
        });

        Object.defineProperty(dg.network, "tick", {
            get: vi.fn(() => 0),
            set: vi.fn(),
            configurable: true,
        });

        // Mock the selectAll methods
        vi.spyOn(dg.network.layers.node, "selectAll").mockImplementation(
            mockNodeSelectAll,
        );
        vi.spyOn(dg.network.layers.link, "selectAll").mockImplementation(
            mockLinkSelectAll,
        );

        // Setup test data
        mockNode = {
            x: 100,
            y: 100,
            fx: null,
            fy: null,
            dragx: 0,
            dragy: 0,
        } as SimNode;

        mockEvent = {
            subject: mockNode,
            x: 150,
            y: 150,
            active: false,
            sourceEvent: createMockMouseEvent("mousedown"),
            target: document.createElementNS("http://www.w3.org/2000/svg", "g"),
            type: "drag",
            identifier: 1,
            dx: 0,
            dy: 0,
        } as unknown as D3DragEventWithSource;
    });

    afterEach(() => {
        // Restore network properties
        Object.defineProperty(dg.network, "isRunningLayout", {
            value: originalIsRunningLayout,
            writable: true,
            configurable: true,
        });

        Object.defineProperty(dg.network, "tick", {
            value: originalTick,
            writable: true,
            configurable: true,
        });
    });

    describe("onDragStart", () => {
        it("should fix node position and set drag coordinates", () => {
            onDragStart(mockEvent);

            expect(mockNode.fx).toBe(mockNode.x);
            expect(mockNode.fy).toBe(mockNode.y);
            expect(mockNode.dragx).toBe(mockNode.x);
            expect(mockNode.dragy).toBe(mockNode.y);
        });

        it("should hide tooltip on mousedown", () => {
            mockEvent.sourceEvent = createMockMouseEvent("mousedown");
            onDragStart(mockEvent);
            expect(hideTooltipSpy).toHaveBeenCalled();
        });

        it("should not hide tooltip for non-mousedown events", () => {
            mockEvent.sourceEvent = createMockMouseEvent("touchstart");
            onDragStart(mockEvent);
            expect(hideTooltipSpy).not.toHaveBeenCalled();
        });
    });

    describe("onDrag", () => {
        it("should update node fixed position", () => {
            onDrag(mockEvent);

            expect(mockNode.fx).toBe(mockEvent.x);
            expect(mockNode.fy).toBe(mockEvent.y);
        });

        it("should restart force layout if node position changed", () => {
            mockNode.dragx = 0;
            mockNode.dragy = 0;
            onDrag(mockEvent);
            expect(restartForceLayoutSpy).toHaveBeenCalledWith(0.3);
        });

        it("should not restart force layout if node position unchanged", () => {
            mockNode.dragx = mockNode.x;
            mockNode.dragy = mockNode.y;
            onDrag(mockEvent);
            expect(restartForceLayoutSpy).not.toHaveBeenCalled();
        });
    });

    describe("onDragEnd", () => {
        it("should release fixed position if node was dragged", () => {
            mockNode.dragx = 150;
            mockNode.dragy = 150;
            mockEvent.sourceEvent = createMockMouseEvent("mouseup");
            onDragEnd(mockEvent);

            expect(mockNode.fx).toBeNull();
            expect(mockNode.fy).toBeNull();
            expect(stopForceLayoutSpy).toHaveBeenCalled();
        });

        it("should not stop force layout if node was not dragged", () => {
            mockNode.dragx = mockNode.x;
            mockNode.dragy = mockNode.y;
            onDragEnd(mockEvent);
            expect(stopForceLayoutSpy).not.toHaveBeenCalled();
        });

        it("should hide tooltip on mouseup", () => {
            mockEvent.sourceEvent = createMockMouseEvent("mouseup");
            onDragEnd(mockEvent);
            expect(hideTooltipSpy).toHaveBeenCalled();
        });
    });

    describe("onNetworkStart", () => {
        it("should initialize network state", () => {
            const isRunningLayoutSetter = vi.fn();
            const tickSetter = vi.fn();

            // Mock the setters specifically for this test
            Object.defineProperty(dg.network, "isRunningLayout", {
                get: vi.fn(() => false),
                set: isRunningLayoutSetter,
                configurable: true,
            });

            Object.defineProperty(dg.network, "tick", {
                get: vi.fn(() => 0),
                set: tickSetter,
                configurable: true,
            });

            onNetworkStart();

            expect(isRunningLayoutSetter).toHaveBeenCalledWith(true);
            expect(tickSetter).toHaveBeenCalledWith(0);
        });

        it("should make nodes and links interactive", () => {
            onNetworkStart();

            expect(mockLinkSelectAll).toHaveBeenCalledWith(".link");
            expect(mockNodeSelectAll).toHaveBeenCalledWith(".node");
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            expect(mockLinkSelectAll().classed).toHaveBeenCalledWith(
                "noninteractive",
                false,
            );
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            expect(mockNodeSelectAll().classed).toHaveBeenCalledWith(
                "noninteractive",
                false,
            );
        });
    });

    describe("onNetworkEnd", () => {
        it("should update network state and maintain interactivity", () => {
            const mockSimulation = {} as d3.Simulation<SimNode, undefined>;
            const isRunningLayoutSetter = vi.fn();

            // Mock the setter specifically for this test
            Object.defineProperty(dg.network, "isRunningLayout", {
                get: vi.fn(() => true),
                set: isRunningLayoutSetter,
                configurable: true,
            });

            onNetworkEnd(mockSimulation);

            expect(isRunningLayoutSetter).toHaveBeenCalledWith(false);
            expect(mockLinkSelectAll).toHaveBeenCalledWith(".link");
            expect(mockNodeSelectAll).toHaveBeenCalledWith(".node");
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            expect(mockLinkSelectAll().classed).toHaveBeenCalledWith(
                "noninteractive",
                false,
            );
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            expect(mockNodeSelectAll().classed).toHaveBeenCalledWith(
                "noninteractive",
                false,
            );
            expect(onTickSpy).toHaveBeenCalledWith(mockSimulation);
        });
    });
});

describe("Custom Events", () => {
    describe("RequestNetworkEvent", () => {
        it("should create event with correct properties", () => {
            const event = new RequestNetworkEvent("testKey", true);

            expect(event.type).toBe("discograph:request-network");
            expect(event.detail).toEqual({
                entityKey: "testKey",
                pushHistory: true,
            });
            expect(event.bubbles).toBe(true);
        });
    });

    describe("SelectEntityEvent", () => {
        it("should create event with correct properties", () => {
            const event = new SelectEntityEvent("testKey", true);

            expect(event.type).toBe("discograph:select-entity");
            expect(event.detail).toEqual({
                entityKey: "testKey",
                fixed: true,
            });
            expect(event.bubbles).toBe(true);
        });
    });

    describe("ResizeEvent", () => {
        it("should create event with correct properties", () => {
            const event = new ResizeEvent();

            expect(event.type).toBe("discograph:resize");
            expect(event.bubbles).toBe(true);
        });
    });
});
