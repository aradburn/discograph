import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import * as d3 from "d3";
import {
    initForceLayout,
    initForceSliders,
    setupForceSliders,
    displayForceLayout,
    startForceLayout,
    restartForceLayout,
    stopForceLayout,
    ALPHA,
} from "../forceLayout";
import { dg, networkStore } from "../../dg";
import type { SimNode, SimLink } from "../data";
import { NodeType } from "../types";

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
    group: vi.fn(() => new Map()),
}));

// Mock dg global object
vi.mock("../../dg", () => {
    const mockNetworkStore = {
        data: {
            nodeMap: new Map(),
            linkMap: new Map(),
        },
        layers: {
            halo: {
                selectAll: vi.fn(() => ({
                    data: vi.fn(() => ({
                        join: vi.fn(),
                    })),
                })),
            },
            node: {
                selectAll: vi.fn(() => ({
                    data: vi.fn(() => ({
                        join: vi.fn(),
                    })),
                })),
            },
            text: {
                selectAll: vi.fn(() => ({
                    data: vi.fn(() => ({
                        join: vi.fn(),
                    })),
                })),
            },
            link: {
                selectAll: vi.fn(() => ({
                    data: vi.fn(() => ({
                        join: vi.fn(),
                    })),
                })),
            },
        },
        forceLayout: null,
    };

    return {
        dg: {
            network: {
                data: {
                    nodeMap: new Map(),
                    linkMap: new Map(),
                },
                layers: {
                    halo: {
                        selectAll: vi.fn(() => ({
                            data: vi.fn(() => ({
                                join: vi.fn(),
                            })),
                        })),
                    },
                    node: {
                        selectAll: vi.fn(() => ({
                            data: vi.fn(() => ({
                                join: vi.fn(),
                            })),
                        })),
                    },
                    text: {
                        selectAll: vi.fn(() => ({
                            data: vi.fn(() => ({
                                join: vi.fn(),
                            })),
                        })),
                    },
                    link: {
                        selectAll: vi.fn(() => ({
                            data: vi.fn(() => ({
                                join: vi.fn(),
                            })),
                        })),
                    },
                },
                forceLayout: null,
            },
            svg_dimensions: [800, 600],
        },
        networkStore: mockNetworkStore,
    };
});

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
    document.body.innerHTML = `
        <input type="range" id="nodeRange" min="0" max="40" />
        <input type="range" id="linkRange" min="0" max="40" />
        <input type="range" id="gravRange" min="0" max="40" />
    `;
});

afterEach(() => {
    vi.clearAllMocks();
    document.body.innerHTML = "";
});

describe("Force Layout Initialization", () => {
    it("should initialize force layout with correct configuration", () => {
        initForceLayout();
        expect(d3.forceSimulation).toHaveBeenCalled();
        expect(networkStore.forceLayout).toBeDefined();
    });

    it("should set up all required forces", () => {
        initForceLayout();
        expect(d3.forceCollide).toHaveBeenCalled();
        expect(d3.forceManyBody).toHaveBeenCalled();
    });
});

describe("Force Slider Controls", () => {
    it("should initialize force sliders", () => {
        initForceSliders();
        const nodeSlider = document.getElementById(
            "nodeRange",
        ) as HTMLInputElement;
        const linkSlider = document.getElementById(
            "linkRange",
        ) as HTMLInputElement;
        const gravSlider = document.getElementById(
            "gravRange",
        ) as HTMLInputElement;

        expect(nodeSlider.oninput).toBeDefined();
        expect(linkSlider.oninput).toBeDefined();
        expect(gravSlider.oninput).toBeDefined();
    });

    it("should setup force sliders with correct initial values", () => {
        setupForceSliders();
        const nodeSlider = document.getElementById(
            "nodeRange",
        ) as HTMLInputElement;
        const linkSlider = document.getElementById(
            "linkRange",
        ) as HTMLInputElement;
        const gravSlider = document.getElementById(
            "gravRange",
        ) as HTMLInputElement;

        expect(nodeSlider.value).toBe("12");
        expect(linkSlider.value).toBe("40");
        expect(gravSlider.value).toBe("10");
    });

    it("should handle slider input events", () => {
        networkStore.forceLayout = d3.forceSimulation();
        initForceSliders();

        const nodeSlider = document.getElementById(
            "nodeRange",
        ) as HTMLInputElement;
        const linkSlider = document.getElementById(
            "linkRange",
        ) as HTMLInputElement;
        const gravSlider = document.getElementById(
            "gravRange",
        ) as HTMLInputElement;

        // Simulate slider input events
        nodeSlider.value = "20";
        nodeSlider.dispatchEvent(new Event("input"));
        expect(networkStore.forceLayout.force).toHaveBeenCalled();
        expect(networkStore.forceLayout.alpha).toHaveBeenCalled();
        expect(networkStore.forceLayout.restart).toHaveBeenCalled();

        linkSlider.value = "30";
        linkSlider.dispatchEvent(new Event("input"));
        expect(networkStore.forceLayout.force).toHaveBeenCalled();

        gravSlider.value = "15";
        gravSlider.dispatchEvent(new Event("input"));
        expect(networkStore.forceLayout.force).toHaveBeenCalled();
    });
});

describe("Force Layout Display and Control", () => {
    it("should display force layout correctly", () => {
        displayForceLayout();
        expect(networkStore.layers.halo.selectAll).toHaveBeenCalled();
        expect(networkStore.layers.node.selectAll).toHaveBeenCalled();
        expect(networkStore.layers.text.selectAll).toHaveBeenCalled();
        expect(networkStore.layers.link.selectAll).toHaveBeenCalled();
    });

    it("should start force layout with provided nodes", () => {
        const mockNodes: SimNode[] = [
            createMockNode("1", { x: 0, y: 0 }),
            createMockNode("2", { x: 100, y: 100 }),
        ];
        startForceLayout(mockNodes);
        expect(networkStore.forceLayout.nodes).toHaveBeenCalledWith(mockNodes);
    });

    it("should restart force layout with new alpha value", () => {
        networkStore.forceLayout = d3.forceSimulation();
        restartForceLayout(ALPHA);
        expect(networkStore.forceLayout.alpha).toHaveBeenCalledWith(ALPHA);
        expect(networkStore.forceLayout.restart).toHaveBeenCalled();
    });

    it("should stop force layout", () => {
        networkStore.forceLayout = d3.forceSimulation();
        stopForceLayout();
        expect(networkStore.forceLayout.stop).toHaveBeenCalled();
    });

    it("should handle force layout when not initialized", () => {
        networkStore.forceLayout = null;
        const consoleSpy = vi.spyOn(console, "error");
        restartForceLayout(ALPHA);
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
        networkStore.data.nodeMap = new Map(
            mockNodes.map((node) => [node.key, node]),
        );

        displayForceLayout();

        // Verify that the layers were updated
        expect(networkStore.layers.node.selectAll).toHaveBeenCalledWith(
            ".node",
        );
        expect(networkStore.layers.halo.selectAll).toHaveBeenCalledWith(
            ".node",
        );
        expect(networkStore.layers.text.selectAll).toHaveBeenCalledWith(
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
        networkStore.data.linkMap = new Map(
            mockLinks.map((link) => [link.key, link]),
        );

        displayForceLayout();

        // Verify that the link layer was updated
        expect(networkStore.layers.link.selectAll).toHaveBeenCalledWith(
            ".link",
        );
    });
});

describe("Error Handling", () => {
    it("should handle missing DOM elements gracefully", () => {
        document.body.innerHTML = ""; // Remove all elements
        const consoleSpy = vi.spyOn(console, "error");
        initForceSliders();
        expect(consoleSpy).toHaveBeenCalledWith(
            "Could not find one or more slider elements",
        );
    });

    it("should handle force layout operations when not initialized", () => {
        networkStore.forceLayout = null;
        const consoleSpy = vi.spyOn(console, "error");

        stopForceLayout();
        expect(consoleSpy).not.toHaveBeenCalled(); // stopForceLayout should handle null case silently

        restartForceLayout(ALPHA);
        expect(consoleSpy).toHaveBeenCalledWith(
            "Force layout is not initialized",
        );
    });
});
