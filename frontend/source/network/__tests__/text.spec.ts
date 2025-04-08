import { describe, it, expect, beforeEach, vi } from "vitest";
import type * as d3 from "d3";
import type { Mock } from "vitest";
import {
    getNodeText,
    getNodeDebug,
    onTextEnter,
    onTextExit,
    onTextUpdate,
    LABEL_OFFSET_Y,
} from "../text";
import { dg } from "../../dg";
import type { SimNode } from "../data";

type D3AttrFunction = (d: SimNode) => string;

type MockD3Chainable = {
    append: Mock;
    attr: Mock;
    text: Mock;
    select: Mock;
    remove: Mock;
};

// Mock node data
const createMockNode = (overrides = {}): SimNode => ({
    key: "test-123",
    name: "Test Node",
    type: "artist",
    size: 1,
    x: 0,
    y: 0,
    vx: 0,
    vy: 0,
    fx: null,
    fy: null,
    distance: 1,
    radius: 10,
    links: [],
    missing: 0,
    cluster: undefined,
    hasMissing: false,
    lastClickTime: 0,
    lastTouchTime: 0,
    fixed: false,
    isIntermediate: false,
    index: 0,
    dragx: 0,
    dragy: 0,
    highlighted: false,
    selected: false,
    ...overrides,
});

describe("Network Node Text Module", () => {
    // Mock D3 selections
    let mockAppendFn: Mock;
    let mockAttrFn: Mock;
    let mockTextFn: Mock;
    let mockSelectFn: Mock;
    let mockRemoveFn: Mock;
    let mockChainableSelection: MockD3Chainable;

    beforeEach(() => {
        // Reset debug mode and mocks before each test
        dg.debug = false;
        vi.clearAllMocks();

        // Create mock D3 selection chain functions
        mockAppendFn = vi.fn();
        mockAttrFn = vi.fn();
        mockTextFn = vi.fn();
        mockSelectFn = vi.fn();
        mockRemoveFn = vi.fn();

        // Create a chainable mock selection
        mockChainableSelection = {
            append: mockAppendFn,
            attr: mockAttrFn,
            text: mockTextFn,
            select: mockSelectFn,
            remove: mockRemoveFn,
        };

        // Set up all functions to return the chainable selection
        mockAppendFn.mockReturnValue(mockChainableSelection);
        mockAttrFn.mockReturnValue(mockChainableSelection);
        mockTextFn.mockReturnValue(mockChainableSelection);
        mockSelectFn.mockReturnValue(mockChainableSelection);
    });

    describe("getNodeText", () => {
        it("should return node name when length is less than 50", () => {
            const node = createMockNode({ name: "Short Name" });
            expect(getNodeText(node)).toBe("Short Name");
        });

        it("should truncate node name when length is greater than 50", () => {
            const longName = "A".repeat(60);
            const node = createMockNode({ name: longName });
            expect(getNodeText(node)).toBe(`${"A".repeat(50)}...`);
        });

        it("should append debug info when debug mode is enabled", () => {
            const node = createMockNode();
            dg.debug = true;
            expect(getNodeText(node)).toContain("Test Node");
            expect(getNodeText(node)).toContain("dist: 1");
        });
    });

    describe("getNodeDebug", () => {
        it("should return formatted debug string with all node properties", () => {
            const node = createMockNode({
                distance: 2,
                radius: 15,
                links: ["link1", "link2"],
                missing: 1,
                cluster: "cluster1",
            });

            const debugInfo = getNodeDebug(node);
            expect(debugInfo).toContain("dist: 2");
            expect(debugInfo).toContain("radi: 15");
            expect(debugInfo).toContain("link: 2");
            expect(debugInfo).toContain("miss: 1");
            expect(debugInfo).toContain("clus: cluster1");
        });

        it("should handle undefined cluster", () => {
            const node = createMockNode();
            const debugInfo = getNodeDebug(node);
            expect(debugInfo).toContain("clus: undefined");
        });

        it("should handle undefined links", () => {
            const node = createMockNode({ links: undefined });
            const debugInfo = getNodeDebug(node);
            expect(debugInfo).toContain("link: 0");
        });
    });

    describe("Text Selection Handlers", () => {
        describe("onTextEnter", () => {
            let textEnterSelection: d3.Selection<
                d3.EnterElement,
                SimNode,
                SVGGElement,
                unknown
            >;

            beforeEach(() => {
                textEnterSelection = {
                    append: mockAppendFn,
                } as unknown as d3.Selection<
                    d3.EnterElement,
                    SimNode,
                    SVGGElement,
                    unknown
                >;
            });

            it("should create text group with correct attributes", () => {
                // Act
                onTextEnter(textEnterSelection);

                // Assert
                // Verify group creation
                expect(mockAppendFn).toHaveBeenCalledWith("g");
                expect(mockAttrFn).toHaveBeenCalledWith(
                    "id",
                    expect.any(Function),
                );

                // Test the id function
                const idFn = mockAttrFn.mock.calls.find(
                    (call) => call[0] === "id",
                )?.[1] as D3AttrFunction;

                if (!idFn) {
                    throw new Error("id function not found in mock calls");
                }

                const mockNode = createMockNode();
                expect(idFn(mockNode)).toBe("test-123");

                // Verify text element creation
                expect(mockAppendFn).toHaveBeenCalledWith("text");
            });

            it("should add cluster class when cluster is defined", () => {
                // Act
                onTextEnter(textEnterSelection);

                // Assert
                // Find the class function from attr calls
                const classFn = mockAttrFn.mock.calls.find(
                    (call) => call[0] === "class",
                )?.[1] as D3AttrFunction;

                if (!classFn) {
                    throw new Error("class function not found in mock calls");
                }

                // Test with a node that has a cluster
                const mockNodeWithCluster = createMockNode({
                    cluster: "test-cluster",
                });
                const classesWithCluster = classFn(mockNodeWithCluster);
                expect(classesWithCluster).toContain("cluster");

                // Test with a node that has no cluster
                const mockNodeWithoutCluster = createMockNode({
                    cluster: undefined,
                });
                const classesWithoutCluster = classFn(mockNodeWithoutCluster);
                expect(classesWithoutCluster).not.toContain("cluster");
            });
        });

        describe("onTextExit", () => {
            it("should remove text elements", () => {
                const textExitSelection = {
                    remove: mockRemoveFn,
                } as unknown as d3.Selection<
                    SVGGElement,
                    SimNode,
                    SVGGElement,
                    unknown
                >;

                // Act
                onTextExit(textExitSelection);

                // Assert
                expect(mockRemoveFn).toHaveBeenCalledTimes(1);
            });
        });

        describe("onTextUpdate", () => {
            it("should update text content for both outer and inner elements", () => {
                const textUpdateSelection = {
                    select: mockSelectFn,
                } as unknown as d3.Selection<
                    SVGGElement,
                    SimNode,
                    SVGGElement,
                    unknown
                >;

                // Act
                onTextUpdate(textUpdateSelection);

                // Assert
                expect(mockSelectFn).toHaveBeenCalledWith(".outer");
                expect(mockSelectFn).toHaveBeenCalledWith(".inner");
                expect(mockTextFn).toHaveBeenCalled();
            });
        });
    });

    describe("Constants", () => {
        it("should export LABEL_OFFSET_Y constant", () => {
            expect(LABEL_OFFSET_Y).toBe(9);
        });
    });
});
