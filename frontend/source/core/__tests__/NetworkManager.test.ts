import { describe, it, expect, vi, beforeEach, afterAll } from "vitest";
import { NetworkManager } from "../NetworkManager";

// Mock the necessary dependencies to avoid circular references
vi.mock("../../network/data", () => {
    return {
        convertNetworkDataToSimData: vi
            .fn()
            .mockImplementation((data: unknown) => data),
    };
});

// Mock d3
vi.mock("d3", () => {
    const mockSimulation = {
        nodes: vi.fn().mockReturnThis(),
        force: vi.fn().mockReturnThis(),
        alphaTarget: vi.fn().mockReturnThis(),
        restart: vi.fn().mockReturnThis(),
        stop: vi.fn().mockReturnThis(),
        tick: vi.fn().mockReturnThis(),
        on: vi.fn().mockReturnThis(),
    };

    return {
        forceSimulation: vi.fn().mockReturnValue(mockSimulation),
        select: vi.fn().mockReturnValue({
            append: vi.fn().mockReturnThis(),
            attr: vi.fn().mockReturnThis(),
            call: vi.fn().mockReturnThis(),
        }),
    };
});

// Mock the RequestNetworkEvent
vi.mock("../../network/events", () => {
    return {
        RequestNetworkEvent: {
            EVENT_NAME: "discograph:request-network",
        },
    };
});

describe("NetworkManager Basic Tests", () => {
    // Mock window event listeners
    const originalAddEventListener = window.addEventListener;
    const originalRemoveEventListener = window.removeEventListener;

    // Setup before each test
    beforeEach(() => {
        vi.clearAllMocks();

        // Mock window event listeners
        window.addEventListener = vi.fn();
        window.removeEventListener = vi.fn();
    });

    // Restore original window functions after all tests
    afterAll(() => {
        window.addEventListener = originalAddEventListener;
        window.removeEventListener = originalRemoveEventListener;
    });

    describe("constructor", () => {
        it("should initialize with default values when no config is provided", () => {
            const manager = new NetworkManager({
                skipEventSetup: true, // Skip event setup to avoid DOM issues
            });

            expect(manager.isUpdating).toBe(false);
            expect(manager.isRunningLayout).toBe(false);
            expect(manager.tick).toBe(0);
            expect(manager.newNodeCoords).toEqual([0, 0]);
            expect(manager.zoom).toBeNull();
            expect(manager.data).toBeDefined();
            expect(manager.layers).toBeDefined();
            expect(manager.selectedNodeKey).toBeUndefined();
        });

        it("should skip event setup when configured to do so", () => {
            const manager = new NetworkManager({ skipEventSetup: true });

            expect(window.addEventListener).not.toHaveBeenCalled();
        });

        it("should have properly initialized network layers", () => {
            const manager = new NetworkManager({ skipEventSetup: true });

            expect(manager.layers).toBeDefined();
            expect(manager.layers.root).toBeNull();
            expect(manager.layers.halo).toBeNull();
            expect(manager.layers.text).toBeNull();
            expect(manager.layers.node).toBeNull();
            expect(manager.layers.link).toBeNull();
        });

        it("should have properly initialized network data", () => {
            const manager = new NetworkManager({ skipEventSetup: true });

            expect(manager.data).toBeDefined();
            expect(manager.data.center).toBeDefined();
            expect(manager.data.nodeMap).toBeInstanceOf(Map);
            expect(manager.data.linkMap).toBeInstanceOf(Map);
            expect(manager.data.maxDistance).toBe(0);
        });

        it("should have properly initialized center node", () => {
            const manager = new NetworkManager({ skipEventSetup: true });
            const center = manager.data.center;

            expect(center).toBeDefined();
            expect(center.x).toBe(0);
            expect(center.y).toBe(0);
            expect(center.type).toBe("artist");
            expect(center.key).toBe("");
            expect(center.name).toBe("");
            expect(center.size).toBe(0);
            expect(center.missing).toBe(0);
            expect(center.hasMissing).toBe(false);
            expect(center.distance).toBe(0);
            expect(center.radius).toBe(0);
            expect(center.lastClickTime).toBe(0);
            expect(center.lastTouchTime).toBe(0);
            expect(center.links).toEqual([]);
            expect(center.cluster).toBe(0);
            expect(center.fixed).toBe(false);
            expect(center.isIntermediate).toBe(false);
        });
    });

    describe("getters and setters", () => {
        let manager: NetworkManager;

        beforeEach(() => {
            manager = new NetworkManager({ skipEventSetup: true });
        });

        it("should get and set isUpdating", () => {
            expect(manager.isUpdating).toBe(false);
            manager.isUpdating = true;
            expect(manager.isUpdating).toBe(true);
        });

        it("should get and set isRunningLayout", () => {
            expect(manager.isRunningLayout).toBe(false);
            manager.isRunningLayout = true;
            expect(manager.isRunningLayout).toBe(true);
        });

        it("should get and set tick", () => {
            expect(manager.tick).toBe(0);
            manager.tick = 10;
            expect(manager.tick).toBe(10);
        });

        it("should get and set newNodeCoords", () => {
            expect(manager.newNodeCoords).toEqual([0, 0]);
            manager.newNodeCoords = [100, 200];
            expect(manager.newNodeCoords).toEqual([100, 200]);
        });

        it("should get and set selectedNodeKey", () => {
            expect(manager.selectedNodeKey).toBeUndefined();
            manager.selectedNodeKey = "node1";
            expect(manager.selectedNodeKey).toBe("node1");
        });

        it("should have null force layout initially", () => {
            expect(manager.forceLayout).toBeNull();
        });

        it("should have null zoom behavior initially", () => {
            expect(manager.zoom).toBeNull();
        });
    });
});
