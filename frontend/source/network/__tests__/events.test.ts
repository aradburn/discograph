import { describe, it, expect, vi, beforeEach } from "vitest";
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
import { nodeTooltip } from "../tooltips";
import { restartForceLayout, stopForceLayout } from "../forceLayout";
import type { SimNode, SimData, SimLink } from "../data";
import type * as d3 from "d3";
import type { DiscographCore, Network } from "../../dg";

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

// Mock the dg object
vi.mock("../../dg", () => {
    const createMockSelection = () => {
        const mockClassed = vi.fn().mockReturnThis();
        const mockSelectAll = vi.fn().mockReturnValue({
            classed: mockClassed,
            each: vi.fn().mockReturnThis(),
            attr: vi.fn().mockReturnThis(),
            data: vi.fn().mockReturnThis(),
            enter: vi.fn().mockReturnThis(),
            exit: vi.fn().mockReturnThis(),
            append: vi.fn().mockReturnThis(),
            remove: vi.fn().mockReturnThis(),
            merge: vi.fn().mockReturnThis(),
            call: vi.fn().mockReturnThis(),
            on: vi.fn().mockReturnThis(),
            style: vi.fn().mockReturnThis(),
            text: vi.fn().mockReturnThis(),
            html: vi.fn().mockReturnThis(),
            select: vi.fn().mockReturnThis(),
            selectAll: vi.fn().mockReturnThis(),
            filter: vi.fn().mockReturnThis(),
            selectChild: vi.fn().mockReturnThis(),
            selectChildren: vi.fn().mockReturnThis(),
            selection: vi.fn().mockReturnThis(),
        });

        const selection = {
            select: vi.fn().mockReturnThis(),
            selectAll: mockSelectAll,
            filter: vi.fn().mockReturnThis(),
            merge: vi.fn().mockReturnThis(),
            selectChild: vi.fn().mockReturnThis(),
            selectChildren: vi.fn().mockReturnThis(),
            selection: vi.fn().mockReturnThis(),
            classed: mockClassed,
            each: vi.fn().mockReturnThis(),
            attr: vi.fn().mockReturnThis(),
            data: vi.fn().mockReturnThis(),
            enter: vi.fn().mockReturnThis(),
            exit: vi.fn().mockReturnThis(),
            append: vi.fn().mockReturnThis(),
            remove: vi.fn().mockReturnThis(),
            call: vi.fn().mockReturnThis(),
            on: vi.fn().mockReturnThis(),
            style: vi.fn().mockReturnThis(),
            text: vi.fn().mockReturnThis(),
            html: vi.fn().mockReturnThis(),
        } as unknown as d3.Selection<
            SVGGElement,
            unknown,
            HTMLElement,
            unknown
        >;

        return selection;
    };

    const createMockNetworkLayers = () => ({
        root: createMockSelection(),
        halo: createMockSelection(),
        text: createMockSelection(),
        node: createMockSelection(),
        link: createMockSelection(),
    });

    const createMockSimData = (): SimData => ({
        center: {
            x: 0,
            y: 0,
            type: "artist",
            key: "",
            name: "",
            size: 0,
            missing: 0,
            hasMissing: false,
            distance: 0,
            radius: 0,
            lastClickTime: 0,
            lastTouchTime: 0,
            links: [],
            cluster: 0,
            fixed: false,
            isIntermediate: false,
        },
        nodeMap: new Map<string, SimNode>(),
        linkMap: new Map<string, SimLink>(),
        maxDistance: 0,
    });

    const createMockDg = () =>
        ({
            network: {
                isRunningLayout: false,
                tick: 0,
                layers: createMockNetworkLayers(),
                dimensions: [0, 0],
                forceLayout: {} as d3.Simulation<SimNode, undefined>,
                isUpdating: false,
                newNodeCoords: [0, 0],
                zoom: null,
                data: createMockSimData(),
            } as Network,
        }) as DiscographCore;

    return {
        dg: createMockDg(),
    };
});

// Mock tooltips
vi.mock("../tooltips", () => ({
    nodeTooltip: {
        hide: vi.fn(),
    },
}));

// Mock force layout functions
vi.mock("../forceLayout", () => ({
    restartForceLayout: vi.fn(),
    stopForceLayout: vi.fn(),
}));

// Mock tick function
vi.mock("../tick", () => ({
    onTick: vi.fn(),
}));

describe("Network Graph Event Handlers", () => {
    let mockNode: SimNode;
    let mockEvent: D3DragEventWithSource;
    let dg: DiscographCore;

    beforeEach(async () => {
        // Import dg in beforeEach to ensure it's available for each test
        const module = await import("../../dg");
        dg = module.dg;

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

        // Reset mock dg state
        dg.network.isRunningLayout = false;
        dg.network.tick = 0;

        // Clear all mocks before each test
        vi.clearAllMocks();
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
            expect(nodeTooltip.hide).toHaveBeenCalled();
        });

        it("should not hide tooltip for non-mousedown events", () => {
            mockEvent.sourceEvent = createMockMouseEvent("touchstart");
            onDragStart(mockEvent);
            expect(nodeTooltip.hide).not.toHaveBeenCalled();
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

            expect(restartForceLayout).toHaveBeenCalledWith(0.3);
        });

        it("should not restart force layout if node position unchanged", () => {
            mockNode.dragx = mockNode.x;
            mockNode.dragy = mockNode.y;
            onDrag(mockEvent);

            expect(restartForceLayout).not.toHaveBeenCalled();
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
            expect(stopForceLayout).toHaveBeenCalled();
        });

        it("should not stop force layout if node was not dragged", () => {
            mockNode.dragx = mockNode.x;
            mockNode.dragy = mockNode.y;
            onDragEnd(mockEvent);

            expect(stopForceLayout).not.toHaveBeenCalled();
        });

        it("should hide tooltip on mouseup", () => {
            mockEvent.sourceEvent = createMockMouseEvent("mouseup");
            onDragEnd(mockEvent);
            expect(nodeTooltip.hide).toHaveBeenCalled();
        });
    });

    describe("onNetworkStart", () => {
        it("should initialize network state", () => {
            onNetworkStart();

            expect(dg.network.isRunningLayout).toBe(true);
            expect(dg.network.tick).toBe(0);
        });

        it("should make nodes and links interactive", () => {
            onNetworkStart();

            expect(dg.network.layers.link.selectAll).toHaveBeenCalledWith(
                ".link",
            );
            expect(dg.network.layers.node.selectAll).toHaveBeenCalledWith(
                ".node",
            );
            expect(
                dg.network.layers.link.selectAll().classed,
            ).toHaveBeenCalledWith("noninteractive", false);
            expect(
                dg.network.layers.node.selectAll().classed,
            ).toHaveBeenCalledWith("noninteractive", false);
        });
    });

    describe("onNetworkEnd", () => {
        it("should update network state and maintain interactivity", () => {
            const mockSimulation = {} as d3.Simulation<SimNode, undefined>;
            onNetworkEnd(mockSimulation);

            expect(dg.network.isRunningLayout).toBe(false);
            expect(dg.network.layers.link.selectAll).toHaveBeenCalledWith(
                ".link",
            );
            expect(dg.network.layers.node.selectAll).toHaveBeenCalledWith(
                ".node",
            );
            expect(
                dg.network.layers.link.selectAll().classed,
            ).toHaveBeenCalledWith("noninteractive", false);
            expect(
                dg.network.layers.node.selectAll().classed,
            ).toHaveBeenCalledWith("noninteractive", false);
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
