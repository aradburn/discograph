import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import * as d3 from "d3";
import {
    initForceLayout,
    displayForceLayout,
    setForceLayoutNodes,
    restartForceLayout,
    stopForceLayout,
    setNetworkForces,
    resetNetworkForces,
} from "../forceLayout";
import type { SimNode, SimLink } from "../data";
import { NodeType } from "../data";
import { discographManager, networkManager } from "../../core";
import { FORCE } from "../../constants";

// Mock d3
vi.mock("d3", () => ({
    forceSimulation: vi.fn(() => ({
        force: vi.fn().mockReturnThis(),
        on: vi.fn().mockReturnThis(),
        stop: vi.fn().mockReturnThis(),
        nodes: vi.fn().mockReturnThis(),
        alpha: vi.fn().mockReturnThis(),
        restart: vi.fn().mockReturnThis(),
    })),
    forceCollide: vi.fn(() => ({
        radius: vi.fn().mockReturnThis(),
        iterations: vi.fn(),
    })),
    forceManyBody: vi.fn(() => ({
        strength: vi.fn().mockReturnThis(),
        distanceMax: vi.fn().mockReturnThis(),
        theta: vi.fn(),
    })),
    forceLink: vi.fn(() => ({
        id: vi.fn().mockReturnThis(),
        links: vi.fn().mockReturnThis(),
        distance: vi.fn().mockReturnThis(),
        iterations: vi.fn(),
    })),
    forceX: vi.fn(() => ({
        strength: vi.fn(),
    })),
    forceY: vi.fn(() => ({
        strength: vi.fn(),
    })),
    forceRadial: vi.fn(() => ({
        strength: vi.fn().mockReturnThis(),
    })),
    group: vi.fn(() => new Map()),
    arc: vi.fn(() => ({
        innerRadius: vi.fn().mockReturnThis(),
        outerRadius: vi.fn().mockReturnThis(),
        startAngle: vi.fn().mockReturnThis(),
        endAngle: vi.fn().mockReturnThis(),
    })),
    InternMap: vi.fn(function () {
        return new Map();
    }),
}));

// Mock core module with networkManager
vi.mock("../../core", () => {
    // Create a mock selectAll function inside the mock callback
    const innerMockSelectAll = vi.fn().mockReturnValue({
        data: vi.fn().mockReturnValue({
            join: vi.fn(),
        }),
        classed: vi.fn().mockReturnThis(),
    });

    return {
        discographManager: {
            svgDimensions: [800, 600],
        },
        networkManager: {
            data: {
                nodeMap: new Map(),
                linkMap: new Map(),
            },
            layers: {
                root: { selectAll: innerMockSelectAll },
                halo: { selectAll: innerMockSelectAll },
                node: { selectAll: innerMockSelectAll },
                text: { selectAll: innerMockSelectAll },
                link: { selectAll: innerMockSelectAll },
            },
            forceLayout: null,
            isRunningLayout: false,
            tick: 0,
        },
    };
});

// Mock window event dispatching
const dispatchEventSpy = vi
    .spyOn(window, "dispatchEvent")
    .mockImplementation(() => true);

// Create mock nodes and links for testing
const createMockNode = (
    key: string,
    props: Partial<SimNode> = {},
): SimNode => ({
    key,
    name: `Test Node ${key}`,
    type: NodeType.Artist,
    size: 10,
    x: 0,
    y: 0,
    missing: 0,
    hasMissing: false,
    lastClickTime: 0,
    lastTouchTime: 0,
    distance: 0,
    radius: 5,
    links: [],
    cluster: 1,
    fixed: false,
    isIntermediate: false,
    dragx: 0,
    dragy: 0,
    fx: null,
    fy: null,
    vx: 0,
    vy: 0,
    index: 0,
    highlighted: false,
    selected: false,
    ...props,
});

const createMockLink = (
    source: SimNode,
    target: SimNode,
    props: Partial<SimLink> = {},
): SimLink => ({
    key: `${source.key}-${target.key}`,
    source,
    target,
    role: "default",
    distance: 1,
    isSpline: false,
    intermediate: createMockNode("intermediate"),
    highlighted: false,
    selected: false,
    ...props,
});

// Mock DOM elements
beforeEach(() => {
    // Reset forceLayout
    networkManager.forceLayout = null;
});

afterEach(() => {
    vi.clearAllMocks();
});

describe("Force Layout Initialization", () => {
    it("should initialize force layout with correct configuration", () => {
        initForceLayout();
        expect(d3.forceSimulation).toHaveBeenCalled();
        expect(networkManager.forceLayout).toBeDefined();
    });

    it("should set up all required forces", () => {
        initForceLayout();
        expect(d3.forceCollide).toHaveBeenCalled();
        expect(networkManager.forceLayout.force).toHaveBeenCalledWith(
            "bbox",
            expect.any(Function),
        );
    });
});

describe("Force Layout Display and Control", () => {
    it("should display force layout correctly", () => {
        displayForceLayout();
        expect(networkManager.layers.halo.selectAll).toHaveBeenCalled();
        expect(networkManager.layers.node.selectAll).toHaveBeenCalled();
        expect(networkManager.layers.text.selectAll).toHaveBeenCalled();
        expect(networkManager.layers.link.selectAll).toHaveBeenCalled();
    });

    it("should start force layout with provided nodes", () => {
        const mockNodes: SimNode[] = [
            createMockNode("1", { x: 0, y: 0 }),
            createMockNode("2", { x: 100, y: 100 }),
        ];
        // Initialize force layout first
        networkManager.forceLayout = d3.forceSimulation();
        setForceLayoutNodes(mockNodes);
        expect(networkManager.forceLayout.nodes).toHaveBeenCalledWith(
            mockNodes,
        );
    });

    it("should restart force layout with new alpha value", () => {
        networkManager.forceLayout = d3.forceSimulation();
        restartForceLayout(FORCE.SIMULATION.ALPHA);
        expect(networkManager.forceLayout.alpha).toHaveBeenCalledWith(
            FORCE.SIMULATION.ALPHA,
        );
        expect(networkManager.forceLayout.restart).toHaveBeenCalled();
    });

    it("should stop force layout", () => {
        networkManager.forceLayout = d3.forceSimulation();
        stopForceLayout();
        expect(networkManager.forceLayout.stop).toHaveBeenCalled();
    });

    it("should handle force layout when not initialized", () => {
        networkManager.forceLayout = null;
        const consoleSpy = vi.spyOn(console, "error");
        restartForceLayout(FORCE.SIMULATION.ALPHA);
        expect(consoleSpy).toHaveBeenCalledWith(
            "Force layout is not initialized",
        );
    });
});

describe("Node and Link Processing", () => {
    it("should filter intermediate nodes from display", () => {
        const mockNodes = [
            createMockNode("1"),
            createMockNode("2", { isIntermediate: true }),
            createMockNode("3"),
        ];
        networkManager.data.nodeMap = new Map(
            mockNodes.map((node) => [node.key, node]),
        );

        displayForceLayout();

        // Verify that the layers were updated
        expect(networkManager.layers.node.selectAll).toHaveBeenCalledWith(
            ".node",
        );
        expect(networkManager.layers.halo.selectAll).toHaveBeenCalledWith(
            ".node",
        );
        expect(networkManager.layers.text.selectAll).toHaveBeenCalledWith(
            ".node",
        );
    });

    it("should filter spline links from display", () => {
        const node1 = createMockNode("1");
        const node2 = createMockNode("2");
        const mockLinks = [
            createMockLink(node1, node2),
            createMockLink(node1, node2, { isSpline: true }),
        ];
        networkManager.data.linkMap = new Map(
            mockLinks.map((link) => [link.key, link]),
        );

        displayForceLayout();

        // Verify that the link layer was updated
        expect(networkManager.layers.link.selectAll).toHaveBeenCalledWith(
            ".link",
        );
    });
});

describe("Error Handling", () => {
    it("should handle force layout operations when not initialized", () => {
        networkManager.forceLayout = null;
        const consoleSpy = vi.spyOn(console, "error");

        stopForceLayout();
        expect(consoleSpy).not.toHaveBeenCalled(); // stopForceLayout should handle null case silently

        restartForceLayout(FORCE.SIMULATION.ALPHA);
        expect(consoleSpy).toHaveBeenCalledWith(
            "Force layout is not initialized",
        );
    });
});

describe("Network Forces Control", () => {
    it("should dispatch set forces event", () => {
        setNetworkForces();
        expect(dispatchEventSpy).toHaveBeenCalled();
        const lastCall = dispatchEventSpy.mock.calls[0][0];
        expect(lastCall.type).toBe("discograph:set-forces");
    });

    it("should dispatch reset forces event", () => {
        resetNetworkForces();
        expect(dispatchEventSpy).toHaveBeenCalled();
        const lastCall = dispatchEventSpy.mock.calls[0][0];
        expect(lastCall.type).toBe("discograph:reset-forces");
    });
});

describe("Hull Processing", () => {
    it("should process hull data for clustered nodes", () => {
        const mockNodes = [
            createMockNode("1", { cluster: 1 }),
            createMockNode("2", { cluster: 1 }),
            createMockNode("3", { cluster: 2 }),
        ];
        networkManager.data.nodeMap = new Map(
            mockNodes.map((node) => [node.key, node]),
        );

        displayForceLayout();

        // Verify that the hull layer was updated
        expect(networkManager.layers.halo.selectAll).toHaveBeenCalledWith(
            ".hull",
        );
    });
});
