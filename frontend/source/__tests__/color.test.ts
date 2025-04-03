import { describe, it, expect } from "vitest";
import { clamp, getNodeColorClass, getLinkColorClass } from "../color";
import type { SimNode, SimLink } from "../network/data";

// Minimal mock types for testing purposes
type MockSimNode = Pick<SimNode, "type" | "distance">;
type MockSimLink = {
    source: MockSimNode;
    target: MockSimNode;
};

describe("Color utility functions", () => {
    describe("clamp", () => {
        it("should return the number if it's within the range", () => {
            expect(clamp(5, 0, 10)).toBe(5);
        });

        it("should clamp the number to the minimum value", () => {
            expect(clamp(-5, 0, 10)).toBe(0);
        });

        it("should clamp the number to the maximum value", () => {
            expect(clamp(15, 0, 10)).toBe(10);
        });

        it("should work correctly when min and max are the same", () => {
            expect(clamp(5, 3, 3)).toBe(3);
            expect(clamp(2, 3, 3)).toBe(3);
        });

        it("should handle negative ranges", () => {
            expect(clamp(-5, -10, -1)).toBe(-5);
            expect(clamp(-15, -10, -1)).toBe(-10);
            expect(clamp(0, -10, -1)).toBe(-1);
        });
    });

    describe("getNodeColorClass", () => {
        it("should return correct class for artist node within range", () => {
            const node: MockSimNode = { type: "artist", distance: 3 };
            expect(getNodeColorClass(node as SimNode)).toBe("color-4");
        });

        it("should return correct class for artist node at min distance", () => {
            const node: MockSimNode = { type: "artist", distance: -2 }; // Clamps to 0, index = 0 + 1 = 1
            expect(getNodeColorClass(node as SimNode)).toBe("color-1");
        });

        it("should return correct class for artist node at max distance", () => {
            const node: MockSimNode = { type: "artist", distance: 10 }; // Clamps to 8, index = 8
            expect(getNodeColorClass(node as SimNode)).toBe("color-8");
        });

        it("should return correct class for label node within range", () => {
            const node: MockSimNode = { type: "label", distance: 3 }; // index = 3 + 2 = 5
            expect(getNodeColorClass(node as SimNode)).toBe("color-5");
        });

        it("should return correct class for label node at min distance", () => {
            const node: MockSimNode = { type: "label", distance: -5 }; // Clamps to 0, index = 0 + 2 = 2
            expect(getNodeColorClass(node as SimNode)).toBe("color-2");
        });

        it("should return correct class for label node at max distance", () => {
            const node: MockSimNode = { type: "label", distance: 7 }; // Clamps to 8, index = 8
            expect(getNodeColorClass(node as SimNode)).toBe("color-8");
        });

        it("should handle distance 0 for artist", () => {
            const node: MockSimNode = { type: "artist", distance: 0 }; // index = 0 + 1 = 1
            expect(getNodeColorClass(node as SimNode)).toBe("color-1");
        });

        it("should handle distance 0 for label", () => {
            const node: MockSimNode = { type: "label", distance: 0 }; // index = 0 + 2 = 2
            expect(getNodeColorClass(node as SimNode)).toBe("color-2");
        });
    });

    describe("getLinkColorClass", () => {
        it("should return color-2 if min distance is 0", () => {
            const link: MockSimLink = {
                source: { type: "artist", distance: 0 },
                target: { type: "label", distance: 3 },
            };
            // min distance = 0 -> effective distance = 2 -> index = 2
            expect(getLinkColorClass(link as SimLink)).toBe("color-2");
        });

        it("should return color-5 if min distance is greater than 0", () => {
            const link: MockSimLink = {
                source: { type: "artist", distance: 1 },
                target: { type: "label", distance: 3 },
            };
            // min distance = 1 -> effective distance = 5 -> index = 5
            expect(getLinkColorClass(link as SimLink)).toBe("color-5");
        });

        it("should return color-5 even if min distance is large", () => {
            const link: MockSimLink = {
                source: { type: "artist", distance: 6 },
                target: { type: "label", distance: 8 },
            };
            // min distance = 6 -> effective distance = 5 -> index = 5
            expect(getLinkColorClass(link as SimLink)).toBe("color-5");
        });

        it("should handle negative distances correctly", () => {
            const link: MockSimLink = {
                source: { type: "artist", distance: -2 },
                target: { type: "label", distance: 1 },
            };
            // min distance = -2 -> effective distance = 5 -> index = 5
            expect(getLinkColorClass(link as SimLink)).toBe("color-5");
        });

        it("should handle one node having distance 0", () => {
            const link: MockSimLink = {
                source: { type: "artist", distance: 5 },
                target: { type: "label", distance: 0 },
            };
            // min distance = 0 -> effective distance = 2 -> index = 2
            expect(getLinkColorClass(link as SimLink)).toBe("color-2");
        });
    });
});
