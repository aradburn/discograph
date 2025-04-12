import { describe, it, expect, vi, beforeEach } from "vitest";
import { dg } from "../dg";
import * as d3 from "d3";

// Mock d3
vi.mock("d3", () => ({
    arc: vi.fn(() => ({
        innerRadius: vi.fn().mockReturnThis(),
        outerRadius: vi.fn().mockReturnThis(),
        startAngle: vi.fn().mockReturnThis(),
        endAngle: vi.fn().mockReturnThis(),
    })),
    InternMap: vi.fn(() => new Map()),
}));

describe("Discograph Core (dg)", () => {
    beforeEach(() => {
        // Reset window.devicePixelRatio for consistent tests
        Object.defineProperty(window, "devicePixelRatio", {
            value: 1,
            writable: true,
        });
    });

    describe("Basic Properties", () => {
        it("should have the correct version", () => {
            expect(dg.version).toBe("2.1.0");
        });

        it("should have debug mode disabled by default", () => {
            expect(dg.debug).toBe(false);
        });

        it("should have device pixel ratio set", () => {
            expect(dg.dpr).toBe(1);
        });

        it("should have initial dimensions set to [0, 0]", () => {
            expect(dg.dimensions).toEqual([0, 0]);
            expect(dg.svg_dimensions).toEqual([0, 0]);
        });
    });

    describe("Network Configuration", () => {
        it("should have properly initialized network object", () => {
            expect(dg.network).toBeDefined();
            expect(dg.network.dimensions).toEqual([0, 0]);
            expect(dg.network.isUpdating).toBe(false);
            expect(dg.network.isRunningLayout).toBe(false);
            expect(dg.network.tick).toBe(0);
            expect(dg.network.newNodeCoords).toEqual([0, 0]);
        });

        it("should have properly initialized network layers", () => {
            expect(dg.network.layers).toBeDefined();
            expect(dg.network.layers.root).toBeNull();
            expect(dg.network.layers.halo).toBeNull();
            expect(dg.network.layers.text).toBeNull();
            expect(dg.network.layers.node).toBeNull();
            expect(dg.network.layers.link).toBeNull();
        });

        it("should have properly initialized network data", () => {
            expect(dg.network.data).toBeDefined();
            expect(dg.network.data.center).toBeDefined();
            expect(dg.network.data.nodeMap).toBeInstanceOf(Map);
            expect(dg.network.data.linkMap).toBeInstanceOf(Map);
            expect(dg.network.data.maxDistance).toBe(0);
        });

        it("should have properly initialized center node", () => {
            const center = dg.network.data.center;
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

    describe("Relations Configuration", () => {
        it("should have properly initialized relations object", () => {
            expect(dg.relations).toBeDefined();
            expect(dg.relations.data).toBeDefined();
            expect(dg.relations.data.results).toEqual([]);
            expect(dg.relations.byYear).toBeInstanceOf(Map);
            expect(dg.relations.byRole).toBeInstanceOf(Map);
            expect(dg.relations.layers.root).toBeNull();
        });
    });

    describe("State Management", () => {
        it("should have null selected node key initially", () => {
            expect(dg.selectedNodeKey).toBeNull();
        });

        it("should have null FSM initially", () => {
            expect(dg.fsm).toBeNull();
        });
    });

    describe("D3 Integration", () => {
        it("should have d3 arc generator initialized", () => {
            expect(dg.arc).toBeDefined();
            // Verify it's a function since we mocked d3.arc()
            expect(typeof dg.arc).toBe("object");
        });

        it("should have null zoom behavior initially", () => {
            expect(dg.network.zoom).toBeNull();
        });

        it("should have null force layout initially", () => {
            expect(dg.network.forceLayout).toBeNull();
        });
    });
});
