import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import * as d3 from "d3";
import {
    calculateSplineInner,
    generateSpline,
    getHullVertices,
    onTick,
    unlabeledRoles,
} from "../tick";
import { hideAllTooltips } from "../tooltips";
import type { SimNode, SimLink } from "../data";
import { NodeType } from "../types";

// Mock dependencies
vi.mock("../tooltips", () => ({
    hideAllTooltips: vi.fn(),
}));

// Mock dg module
vi.mock("../../dg", () => {
    const mockNetworkStore = {
        tick: 0,
        data: {
            center: null,
            nodeMap: new Map(),
            linkMap: new Map(),
            maxDistance: 0,
        },
        layers: {
            root: null,
            link: null,
            halo: null,
            node: null,
            text: null,
        },
        dimensions: [800, 600],
        forceLayout: null,
        isUpdating: false,
        isRunningLayout: false,
        newNodeCoords: [0, 0],
        zoom: null,
    };

    return {
        dg: {
            svg_dimensions: [800, 600],
        },
        networkStore: mockNetworkStore,
    };
});

// Import after mocking
import { dg, networkStore } from "../../dg";

// Mock d3 functions we need
vi.mock("d3", async (importOriginal) => {
    const originalModule = await importOriginal();
    return {
        ...(originalModule as object),
        select: vi.fn().mockReturnValue({
            selectAll: vi.fn().mockReturnValue({
                attr: vi.fn().mockReturnValue({}),
                each: vi.fn().mockReturnValue({}),
                select: vi.fn().mockReturnValue({
                    attr: vi.fn().mockReturnValue({}),
                }),
            }),
        }) as unknown as typeof d3.select,
        polygonHull: vi
            .fn()
            .mockImplementation((points: Array<[number, number]>) => {
                // Simple mock implementation that returns the first and last points to form a hull
                if (!points || points.length < 2) return null;
                return [points[0], points[points.length - 1]] as [
                    number,
                    number,
                ][];
            }),
    };
});

describe("Network Visualization Functions", () => {
    describe("unlabeledRoles", () => {
        it("should contain the correct roles", () => {
            expect(unlabeledRoles).toEqual([
                "Alias",
                "Member Of",
                "Sublabel Of",
            ]);
        });
    });

    describe("calculateSplineInner", () => {
        it("should calculate correct intersection points when target is to the right and above", () => {
            const [newSX, newSY] = calculateSplineInner(0, 0, 10, 100, -100);
            expect(newSX).toBeCloseTo(7.071); // cos(45°) * 10
            expect(newSY).toBeCloseTo(-7.071); // sin(45°) * 10
        });

        it("should calculate correct intersection points when target is to the left and below", () => {
            const [newSX, newSY] = calculateSplineInner(0, 0, 10, -100, 100);
            expect(newSX).toBeCloseTo(-7.071);
            expect(newSY).toBeCloseTo(7.071);
        });
    });

    describe("generateSpline", () => {
        it("should generate straight line path when no intermediate point exists", () => {
            const link: SimLink = {
                source: { x: 0, y: 0, radius: 5 },
                target: { x: 100, y: 100, radius: 5 },
            } as SimLink;

            const path = generateSpline(link);
            expect(path).toBe("M 0,0 L 100,100");
        });

        it("should generate curved path when intermediate point exists", () => {
            const link: SimLink = {
                source: { x: 0, y: 0, radius: 5 },
                target: { x: 100, y: 100, radius: 5 },
                intermediate: { x: 50, y: 0 },
            } as SimLink;

            const path = generateSpline(link);
            expect(path).toMatch(/M .+,.+ S 50,0 .+,.+/);
        });
    });

    describe("getHullVertices", () => {
        it("should generate correct vertices for a single node", () => {
            const nodes: SimNode[] = [
                {
                    x: 100,
                    y: 100,
                    radius: 30,
                } as SimNode,
            ];

            const vertices = getHullVertices(nodes);
            expect(vertices).toHaveLength(4);
            expect(vertices).toContainEqual([110, 110]);
            expect(vertices).toContainEqual([110, 90]);
            expect(vertices).toContainEqual([90, 110]);
            expect(vertices).toContainEqual([90, 90]);
        });

        it("should generate correct number of vertices for multiple nodes", () => {
            const nodes: SimNode[] = [
                { x: 100, y: 100, radius: 30 },
                { x: 200, y: 200, radius: 30 },
            ] as SimNode[];

            const vertices = getHullVertices(nodes);
            expect(vertices).toHaveLength(8); // 4 vertices per node
        });
    });

    describe("onTick", () => {
        const mockSimulation = {} as d3.Simulation<SimNode, undefined>;

        beforeEach(() => {
            // Reset the mock values in each test
            vi.mocked(networkStore.data.nodeMap).clear();
            networkStore.tick = 0;

            // Mock d3.select to return an object with chainable methods
            (d3.select as ReturnType<typeof vi.fn>).mockReturnValue({
                selectAll: () => ({
                    attr: () => ({}),
                    each: () => ({}),
                    select: () => ({
                        attr: () => ({}),
                    }),
                }),
            });
        });

        afterEach(() => {
            vi.clearAllMocks();
        });

        it("should increment the network tick counter", () => {
            onTick(mockSimulation);
            expect(networkStore.tick).toBe(1);
        });

        it("should call hideAllTooltips", () => {
            onTick(mockSimulation);
            expect(hideAllTooltips).toHaveBeenCalled();
        });

        it("should center the main node if it exists and is not fixed", () => {
            const centerNode: SimNode = {
                key: "center",
                name: "Center Node",
                type: NodeType.Artist,
                size: 10,
                x: 0,
                y: 0,
                missing: 0,
                hasMissing: false,
                lastClickTime: 0,
                lastTouchTime: 0,
                distance: 0,
                radius: 10,
                links: [],
                cluster: 0,
                fixed: false,
                isIntermediate: false,
                // Additional SimulationProps
                dragx: 0,
                dragy: 0,
                fx: null,
                fy: null,
                vx: 0,
                vy: 0,
                index: 0,
                highlighted: false,
                selected: false,
            };

            networkStore.data.center = {
                key: "center",
                name: "Center Node",
                type: NodeType.Artist,
                size: 10,
                x: 0,
                y: 0,
                missing: 0,
                hasMissing: false,
                lastClickTime: 0,
                lastTouchTime: 0,
                distance: 0,
                radius: 10,
                links: [],
                cluster: 0,
                fixed: false,
                isIntermediate: false,
            };
            networkStore.data.nodeMap.set("center", centerNode);
            dg.svg_dimensions = [800, 600];

            onTick(mockSimulation);

            // Center node should be moved towards the center of the SVG
            expect(centerNode.x).toBeGreaterThan(0);
            expect(centerNode.y).toBeGreaterThan(0);
        });

        it("should not center the main node if it is fixed", () => {
            const centerNode: SimNode = {
                key: "center",
                name: "Center Node",
                type: NodeType.Artist,
                size: 10,
                x: 0,
                y: 0,
                missing: 0,
                hasMissing: false,
                lastClickTime: 0,
                lastTouchTime: 0,
                distance: 0,
                radius: 10,
                links: [],
                cluster: 0,
                fixed: true,
                isIntermediate: false,
                // Additional SimulationProps
                dragx: 0,
                dragy: 0,
                fx: null,
                fy: null,
                vx: 0,
                vy: 0,
                index: 0,
                highlighted: false,
                selected: false,
            };

            networkStore.data.center = {
                key: "center",
                name: "Center Node",
                type: NodeType.Artist,
                size: 10,
                x: 0,
                y: 0,
                missing: 0,
                hasMissing: false,
                lastClickTime: 0,
                lastTouchTime: 0,
                distance: 0,
                radius: 10,
                links: [],
                cluster: 0,
                fixed: true,
                isIntermediate: false,
            };
            networkStore.data.nodeMap.set("center", centerNode);
            dg.svg_dimensions = [800, 600];

            onTick(mockSimulation);

            // Fixed node should not move
            expect(centerNode.x).toBe(0);
            expect(centerNode.y).toBe(0);
        });
    });
});
