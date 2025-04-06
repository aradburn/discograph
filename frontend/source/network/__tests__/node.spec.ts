// Mock the dg module before imports
vi.mock("../../dg", () => {
    // Create a mock selection
    const mockSelection = {
        selectAll: vi.fn(),
    } as unknown as d3.Selection<SVGGElement, unknown, HTMLElement, unknown>;

    // Mock the dg object with proper types
    const mockDg = {
        network: {
            layers: {
                root: mockSelection,
                halo: mockSelection,
                text: mockSelection,
                node: mockSelection,
                link: mockSelection,
            },
        },
    };

    return {
        dg: mockDg,
    };
});

import type * as d3 from "d3";
import {
    vi,
    describe,
    it,
    expect,
    beforeEach,
    afterEach,
    type Mock,
} from "vitest";
import {
    onNodeEnter,
    onNodeExit,
    onNodeUpdate,
    onNodeMouseOver,
    onNodeMouseDown,
    onNodeMouseDoubleClick,
    onNodeTouchStart,
    getRadius,
    getOuterRadius,
    getInnerRadius,
    NODE_INNER_RADIUS,
    NODE_OUTER_RADIUS,
} from "../node";
import type { SimNode } from "../data";
import { dg } from "../../dg";

// Mock d3 drag behavior
vi.mock("d3", async () => {
    const actual = await vi.importActual("d3");
    return {
        ...actual,
        drag: () => ({
            on: vi.fn().mockReturnThis(),
        }),
    };
});

// Mock tooltips
vi.mock("../tooltips", () => ({
    nodeTooltip: {
        show: vi.fn(),
        hide: vi.fn(),
    },
    hideAllTooltips: vi.fn(),
}));

// Mock dg global object
vi.mock("../dg", () => ({
    dg: {
        network: {
            layers: {
                node: {
                    selectAll: vi.fn().mockReturnValue({
                        filter: vi.fn().mockReturnValue({
                            raise: vi.fn(),
                        }),
                    }),
                },
                text: {
                    selectAll: vi.fn().mockReturnValue({
                        filter: vi.fn().mockReturnValue({
                            raise: vi.fn(),
                        }),
                    }),
                },
            },
        },
    },
}));

describe("Network Node Functions", () => {
    // Mock types
    type MockD3Element = {
        attr: Mock;
        append: Mock;
        on: Mock;
        style: Mock;
        selectAll: Mock;
        select: Mock;
        call: Mock;
        filter: Mock;
        raise: Mock;
        remove: Mock;
    };

    type NodeEnterSelection = d3.Selection<
        d3.EnterElement,
        SimNode,
        d3.BaseType,
        unknown
    >;
    type NodeSelection = d3.Selection<
        SVGGElement,
        SimNode,
        d3.BaseType,
        unknown
    >;

    // Mock variables
    let mockCircle: MockD3Element;
    let mockRect: MockD3Element;
    let mockPath: MockD3Element;
    let mockAppendedGroup: MockD3Element;
    let mockNodeEnterSelection: NodeEnterSelection;
    let mockNodeSelection: NodeSelection;
    let boundAppend: Mock;
    let boundRemove: Mock;
    let boundSelectAll: Mock;

    // Mock node data
    const mockArtistNode: SimNode = {
        key: "artist-test",
        name: "Test Artist",
        type: "artist",
        size: 10,
        distance: 1,
        x: 0,
        y: 0,
        fx: null,
        fy: null,
        index: 0,
        vx: 0,
        vy: 0,
        dragx: 0,
        dragy: 0,
        selected: false,
        highlighted: false,
        missing: 0,
        hasMissing: false,
        lastClickTime: 0,
        lastTouchTime: 0,
        cluster: 0,
        fixed: false,
        isIntermediate: false,
        radius: 0,
        links: [],
    };

    const mockLabelNode: SimNode = {
        ...mockArtistNode,
        key: "label-test",
        name: "Test Label",
        type: "label",
    };

    beforeEach(() => {
        // Reset all mocks
        vi.clearAllMocks();

        // Create mock elements with chainable methods
        mockCircle = {
            attr: vi.fn().mockReturnThis(),
            append: vi.fn().mockReturnThis(),
            on: vi.fn().mockReturnThis(),
            style: vi.fn().mockReturnThis(),
            selectAll: vi.fn().mockReturnThis(),
            select: vi.fn().mockReturnThis(),
            call: vi.fn().mockReturnThis(),
            filter: vi.fn().mockReturnThis(),
            raise: vi.fn().mockReturnThis(),
            remove: vi.fn().mockReturnThis(),
        };

        mockRect = {
            attr: vi.fn().mockReturnThis(),
            append: vi.fn().mockReturnThis(),
            on: vi.fn().mockReturnThis(),
            style: vi.fn().mockReturnThis(),
            selectAll: vi.fn().mockReturnThis(),
            select: vi.fn().mockReturnThis(),
            call: vi.fn().mockReturnThis(),
            filter: vi.fn().mockReturnThis(),
            raise: vi.fn().mockReturnThis(),
            remove: vi.fn().mockReturnThis(),
        };

        mockPath = {
            attr: vi.fn().mockReturnThis(),
            append: vi.fn().mockReturnThis(),
            on: vi.fn().mockReturnThis(),
            style: vi.fn().mockReturnThis(),
            selectAll: vi.fn().mockReturnThis(),
            select: vi.fn().mockReturnThis(),
            call: vi.fn().mockReturnThis(),
            filter: vi.fn().mockReturnThis(),
            raise: vi.fn().mockReturnThis(),
            remove: vi.fn().mockReturnThis(),
        };

        mockAppendedGroup = {
            attr: vi.fn().mockReturnThis(),
            append: vi
                .fn()
                .mockImplementation((type: string): MockD3Element => {
                    if (type === "circle") return mockCircle;
                    if (type === "rect") return mockRect;
                    if (type === "path") return mockPath;
                    return mockAppendedGroup;
                }),
            on: vi.fn().mockReturnThis(),
            style: vi.fn().mockReturnThis(),
            selectAll: vi.fn().mockReturnThis(),
            select: vi.fn().mockImplementation((selector: string) => {
                if (
                    selector ===
                    "function (d) { return d.type === 'artist' ? this : null; }"
                ) {
                    return mockAppendedGroup;
                }
                if (
                    selector ===
                    "function (d) { return d.type === 'label' ? this : null; }"
                ) {
                    return mockAppendedGroup;
                }
                return mockAppendedGroup;
            }),
            call: vi.fn().mockReturnThis(),
            filter: vi.fn().mockReturnThis(),
            raise: vi.fn().mockReturnThis(),
            remove: vi.fn().mockReturnThis(),
        };

        // Create mock selections with bound methods
        boundAppend = vi.fn().mockReturnValue(mockAppendedGroup);
        boundRemove = vi.fn();
        boundSelectAll = vi.fn().mockReturnValue({
            attr: vi.fn().mockReturnThis(),
            style: vi.fn().mockReturnThis(),
        });

        mockNodeEnterSelection = {
            append: boundAppend,
        } as unknown as NodeEnterSelection;

        mockNodeSelection = {
            remove: boundRemove,
            selectAll: boundSelectAll,
        } as unknown as NodeSelection;

        vi.useFakeTimers();
    });

    afterEach(() => {
        vi.clearAllMocks();
        vi.useRealTimers();
    });

    describe("Radius Calculations", () => {
        it("should calculate base radius correctly", () => {
            // Base radius = (sqrt(size) * 2 + boost1 + boost2) / alias
            // boost1: distance=0 -> 10, distance=1 -> 5, else 0
            // boost2: numLinks>=20 -> 10, numLinks>=10 -> 5, else 0
            // alias: cluster defined -> 2, else 1
            expect(getRadius(10, 0, 5, undefined)).toBe(16); // Center node (sqrt(10)*2 + 10 + 0)/1
            expect(getRadius(10, 1, 5, undefined)).toBe(11); // Distance 1 node (sqrt(10)*2 + 5 + 0)/1
            expect(getRadius(10, 2, 21, undefined)).toBe(16); // Many links (sqrt(10)*2 + 0 + 10)/1
            expect(getRadius(10, 2, 5, 1)).toBe(3); // Clustered node (sqrt(10)*2 + 0 + 0)/2
        });

        it("should calculate outer radius correctly", () => {
            // Outer radius = NODE_OUTER_RADIUS + base radius
            expect(getOuterRadius(mockArtistNode)).toBe(NODE_OUTER_RADIUS + 6); // 11 + 6 = 17
        });

        it("should calculate inner radius correctly", () => {
            // Inner radius = NODE_INNER_RADIUS + base radius
            expect(getInnerRadius(mockArtistNode)).toBe(NODE_INNER_RADIUS + 6); // 8 + 6 = 14
        });
    });

    describe("onNodeEnter", () => {
        it("should create node group with correct attributes", () => {
            onNodeEnter(mockNodeEnterSelection);

            // Verify group creation
            expect(boundAppend).toHaveBeenCalledWith("g");

            // Verify attribute setting
            expect(mockAppendedGroup.attr).toHaveBeenCalledWith(
                "id",
                expect.any(Function),
            );
            expect(mockAppendedGroup.attr).toHaveBeenCalledWith(
                "class",
                expect.any(Function),
            );

            // Test id attribute
            const idCalls = mockAppendedGroup.attr.mock.calls;
            const idCall = idCalls.find((call) => call[0] === "id");
            expect(idCall).toBeTruthy();
            const idFunc = idCall?.[1] as (d: SimNode) => string;
            expect(idFunc(mockArtistNode)).toBe("artist-test");

            // Test class attribute
            const classCall = idCalls.find((call) => call[0] === "class");
            expect(classCall).toBeTruthy();
            const classFunc = classCall?.[1] as (d: SimNode) => string;
            expect(classFunc(mockArtistNode)).toBe("node artist Palette3");
            expect(classFunc(mockLabelNode)).toBe("node label Palette4");
        });

        it("should create artist node elements with correct attributes", () => {
            onNodeEnter(mockNodeEnterSelection);

            // Verify circle creation for artist nodes
            expect(mockAppendedGroup.append).toHaveBeenCalledWith("circle");

            // Verify shadow circle attributes
            expect(mockCircle.attr).toHaveBeenCalledWith("class", "shadow");
            expect(mockCircle.attr).toHaveBeenCalledWith(
                "cx",
                expect.any(Function),
            );
            expect(mockCircle.attr).toHaveBeenCalledWith(
                "cy",
                expect.any(Function),
            );
            expect(mockCircle.attr).toHaveBeenCalledWith(
                "r",
                expect.any(Function),
            );

            // Verify outer circle attributes
            const outerClassCalls = mockCircle.attr.mock.calls.filter(
                (call) => call[0] === "class" && typeof call[1] === "function",
            );
            const outerClassFunc = outerClassCalls[0]?.[1] as (
                d: SimNode,
            ) => string;
            expect(outerClassFunc(mockArtistNode)).toContain("outer");
        });

        it("should create label node elements with correct attributes", () => {
            onNodeEnter(mockNodeEnterSelection);

            // Verify rect creation for label nodes
            expect(mockAppendedGroup.append).toHaveBeenCalledWith("rect");

            // Verify rect attributes
            expect(mockRect.attr).toHaveBeenCalledWith(
                "class",
                expect.any(Function),
            );
            expect(mockRect.attr).toHaveBeenCalledWith(
                "height",
                expect.any(Function),
            );
            expect(mockRect.attr).toHaveBeenCalledWith(
                "width",
                expect.any(Function),
            );
            expect(mockRect.attr).toHaveBeenCalledWith(
                "x",
                expect.any(Function),
            );
            expect(mockRect.attr).toHaveBeenCalledWith(
                "y",
                expect.any(Function),
            );
        });

        it("should create more indicator with correct attributes", () => {
            onNodeEnter(mockNodeEnterSelection);

            // Verify path creation for more indicator
            expect(mockAppendedGroup.append).toHaveBeenCalledWith("path");
            expect(mockPath.attr).toHaveBeenCalledWith("class", "more");
            expect(mockPath.attr).toHaveBeenCalledWith(
                "d",
                expect.any(Function),
            );
            expect(mockPath.style).toHaveBeenCalledWith(
                "opacity",
                expect.any(Function),
            );
        });

        it("should bind mouse and touch events", () => {
            onNodeEnter(mockNodeEnterSelection);

            // Verify that all event handlers are bound
            expect(mockAppendedGroup.on).toHaveBeenCalledWith(
                "mouseover",
                expect.any(Function),
            );
            expect(mockAppendedGroup.on).toHaveBeenCalledWith(
                "mouseenter",
                expect.any(Function),
            );
            expect(mockAppendedGroup.on).toHaveBeenCalledWith(
                "mouseleave",
                expect.any(Function),
            );
            expect(mockAppendedGroup.on).toHaveBeenCalledWith(
                "mousedown",
                expect.any(Function),
            );
            expect(mockAppendedGroup.on).toHaveBeenCalledWith(
                "dblclick",
                expect.any(Function),
            );
            expect(mockAppendedGroup.on).toHaveBeenCalledWith(
                "touchstart",
                expect.any(Function),
            );

            // Test mouseover handler
            const mouseoverHandler = mockAppendedGroup.on.mock.calls.find(
                (call) => call[0] === "mouseover",
            )?.[1] as ((event: MouseEvent, d: SimNode) => void) | undefined;
            expect(mouseoverHandler).toBeDefined();

            // Test mouseenter handler
            const mouseenterHandler = mockAppendedGroup.on.mock.calls.find(
                (call) => call[0] === "mouseenter",
            )?.[1] as ((event: MouseEvent, d: SimNode) => void) | undefined;
            expect(mouseenterHandler).toBeDefined();

            // Test mouseleave handler
            const mouseleaveHandler = mockAppendedGroup.on.mock.calls.find(
                (call) => call[0] === "mouseleave",
            )?.[1] as ((event: MouseEvent, d: SimNode) => void) | undefined;
            expect(mouseleaveHandler).toBeDefined();
        });
    });

    describe("onNodeExit", () => {
        it("should remove exiting nodes", () => {
            onNodeExit(mockNodeSelection);
            expect(boundRemove).toHaveBeenCalled();
        });
    });

    describe("onNodeUpdate", () => {
        it("should update node classes and more indicator", () => {
            onNodeUpdate(mockNodeSelection);

            expect(boundSelectAll).toHaveBeenCalledWith(".outer");
            expect(boundSelectAll).toHaveBeenCalledWith(".inner");
            expect(boundSelectAll).toHaveBeenCalledWith(".more");
        });
    });

    describe("Mouse Event Handlers", () => {
        beforeEach(function (this: void) {
            // Reset all mocks
            vi.clearAllMocks();

            // Mock the filter and raise methods
            const mockFilterRaise = {
                filter: vi.fn().mockReturnValue({
                    raise: vi.fn(),
                }),
            };
            const mockSelectAll = vi.fn().mockReturnValue(mockFilterRaise);

            // Set up the mock layers
            (dg.network.layers as unknown) = {
                root: mockNodeSelection,
                halo: mockNodeSelection,
                text: {
                    ...mockNodeSelection,
                    selectAll: mockSelectAll,
                },
                node: {
                    ...mockNodeSelection,
                    selectAll: mockSelectAll,
                },
                link: mockNodeSelection,
            };
        });

        it("should handle mouseover events", () => {
            const event = new MouseEvent("mouseover");

            onNodeMouseOver(event, mockArtistNode);
            vi.advanceTimersByTime(250);

            // eslint-disable-next-line @typescript-eslint/unbound-method
            expect(dg.network.layers.node?.selectAll).toHaveBeenCalledWith(
                ".node",
            );
            // eslint-disable-next-line @typescript-eslint/unbound-method
            expect(dg.network.layers.text?.selectAll).toHaveBeenCalledWith(
                ".node",
            );
        });

        it("should handle mousedown events", () => {
            const event = new MouseEvent("mousedown");
            const dispatchEventSpy = vi.spyOn(window, "dispatchEvent");

            onNodeMouseDown(event, mockArtistNode);

            expect(dispatchEventSpy).toHaveBeenCalled();
        });

        it("should handle double click events", () => {
            const event = new MouseEvent("dblclick");
            const stopPropagationSpy = vi.fn();
            event.stopPropagation = stopPropagationSpy;
            const dispatchEventSpy = vi.spyOn(window, "dispatchEvent");

            onNodeMouseDoubleClick(event, mockArtistNode);

            expect(stopPropagationSpy).toHaveBeenCalled();
            expect(dispatchEventSpy).toHaveBeenCalled();
        });

        it("should handle touch events", () => {
            const event = new TouchEvent("touchstart");
            const stopPropagationSpy = vi.fn();
            event.stopPropagation = stopPropagationSpy;
            const dispatchEventSpy = vi.spyOn(window, "dispatchEvent");

            onNodeTouchStart(event, mockArtistNode);

            expect(stopPropagationSpy).toHaveBeenCalled();
            expect(dispatchEventSpy).toHaveBeenCalled();
        });
    });
});
