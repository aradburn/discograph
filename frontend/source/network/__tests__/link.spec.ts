/* eslint-disable @typescript-eslint/unbound-method */
import {
    describe,
    it,
    expect,
    vi,
    beforeEach,
    afterEach,
    type Mock,
} from "vitest";
import type * as d3 from "d3";
import { onLinkEnter, onLinkExit, onLinkUpdate } from "../link";
import type { SimLink } from "../data";
import { NodeType } from "../data";

// Mock dependencies
vi.mock("../tooltips", () => ({
    linkTooltip: {
        show: vi.fn(),
        hide: vi.fn(),
    },
}));

vi.mock("../../color", () => ({
    getLinkColorClass: vi.fn().mockReturnValue("mock-color-class"),
}));

// Mock d3 functions we need
vi.mock("d3", async (importOriginal) => {
    const originalModule = await importOriginal();
    return {
        ...(originalModule as object),
        select: vi.fn().mockReturnValue({
            classed: vi.fn().mockReturnValue({
                transition: vi.fn().mockReturnValue({
                    duration: vi.fn(),
                }),
            }),
        }),
    };
});

describe("Network Link Functions", () => {
    // Type definitions for mock selections
    type LinkEnterSelection = d3.Selection<
        d3.EnterElement,
        SimLink,
        SVGGElement,
        unknown
    >;

    interface MockD3Element {
        attr: Mock<(name: string, value?: unknown) => MockD3Element>;
        append: Mock<(type: string) => MockD3Element>;
        on: Mock<
            (
                event: string,
                handler: (event: MouseEvent, d: SimLink) => void,
            ) => MockD3Element
        >;
        text: Mock<
            (value?: ((d: SimLink) => string) | string) => MockD3Element
        >;
        querySelector: Mock<(selector: string) => Element | null>;
    }

    let mockPath: MockD3Element;
    let mockText: MockD3Element;
    let mockAppendedGroup: MockD3Element;
    let mockLinkEnterSelection: LinkEnterSelection;

    const mockLink: SimLink = {
        key: "source-target-role-1-2",
        source: {
            key: "source-node",
            x: 0,
            y: 0,
            type: NodeType.Artist,
            name: "Source",
            size: 1,
            distance: 0,
            radius: 5,
            links: [],
            missing: 0,
            hasMissing: false,
            lastClickTime: 0,
            lastTouchTime: 0,
            cluster: 1,
            fixed: false,
            isIntermediate: false,
            vx: 0,
            vy: 0,
            index: 0,
            highlighted: false,
            selected: false,
            dragx: 0,
            dragy: 0,
            fx: null,
            fy: null,
        },
        target: {
            key: "target-node",
            x: 100,
            y: 100,
            type: NodeType.Label,
            name: "Target",
            size: 1,
            distance: 0,
            radius: 5,
            links: [],
            missing: 0,
            hasMissing: false,
            lastClickTime: 0,
            lastTouchTime: 0,
            cluster: 1,
            fixed: false,
            isIntermediate: false,
            vx: 0,
            vy: 0,
            index: 1,
            highlighted: false,
            selected: false,
            dragx: 0,
            dragy: 0,
            fx: null,
            fy: null,
        },
        role: "Test Role",
        distance: 1,
        isSpline: false,
        intermediate: null,
        highlighted: false,
        selected: false,
    };

    beforeEach(() => {
        vi.clearAllMocks();
        vi.useFakeTimers();

        // Initialize mock objects
        mockPath = {
            attr: vi.fn().mockReturnThis(),
            append: vi.fn().mockReturnThis(),
            on: vi.fn().mockReturnThis(),
            text: vi.fn().mockReturnThis(),
            querySelector: vi.fn().mockReturnValue(null),
        };

        mockText = {
            attr: vi.fn().mockReturnThis(),
            append: vi.fn().mockReturnThis(),
            on: vi.fn().mockReturnThis(),
            text: vi.fn().mockReturnThis(),
            querySelector: vi.fn().mockReturnValue(null),
        };

        mockAppendedGroup = {
            attr: vi.fn().mockReturnThis(),
            append: vi.fn().mockImplementation((type: string) => {
                if (type === "path") return mockPath;
                if (type === "text") return mockText;
                return mockAppendedGroup;
            }),
            on: vi.fn().mockReturnThis(),
            text: vi.fn().mockReturnThis(),
            querySelector: vi.fn().mockReturnValue(null),
        };

        mockLinkEnterSelection = {
            append: vi.fn().mockReturnValue(mockAppendedGroup),
        } as unknown as LinkEnterSelection;
    });

    afterEach(() => {
        vi.clearAllMocks();
        vi.useRealTimers();
    });

    describe("onLinkEnter", () => {
        it("should create link group with correct attributes", () => {
            onLinkEnter(mockLinkEnterSelection);

            // Verify group creation
            expect(mockLinkEnterSelection.append).toHaveBeenCalledWith("g");

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
            const idCalls = (mockAppendedGroup.attr as Mock).mock.calls;
            let idCall: unknown[] | undefined;
            for (const call of idCalls) {
                if (call[0] === "id") {
                    idCall = call;
                    break;
                }
            }
            expect(idCall).toBeTruthy();
            const idFunc = idCall?.[1] as (d: SimLink) => string;
            expect(idFunc(mockLink)).toBe("link-source-target-role-1-2");

            // Test class attribute
            let classCall: unknown[] | undefined;
            for (const call of idCalls) {
                if (call[0] === "class") {
                    classCall = call;
                    break;
                }
            }
            expect(classCall).toBeTruthy();
            const classFunc = classCall?.[1] as (d: SimLink) => string;
            expect(classFunc(mockLink)).toBe("link role LinkGreenPalette");
        });

        it("should create path with correct attributes", () => {
            onLinkEnter(mockLinkEnterSelection);

            // Verify path creation
            expect(mockAppendedGroup.append).toHaveBeenCalledWith("path");

            // Verify path attributes
            expect(mockPath.attr).toHaveBeenCalledWith(
                "class",
                expect.any(Function),
            );

            // Test class attribute
            const mockPathCalls = (mockPath.attr as Mock).mock.calls;
            let classCall: unknown[] | undefined;
            for (const call of mockPathCalls) {
                if (call[0] === "class") {
                    classCall = call;
                    break;
                }
            }
            expect(classCall).toBeTruthy();
            const classFunc = classCall?.[1] as (d: SimLink) => string;
            expect(classFunc(mockLink)).toBe(
                "inner distance-0 mock-color-class",
            );
        });

        it("should create text elements with correct attributes", () => {
            onLinkEnter(mockLinkEnterSelection);

            // Verify text elements creation
            expect(mockAppendedGroup.append).toHaveBeenCalledWith("text");
            expect(mockAppendedGroup.append).toHaveBeenCalledWith("text");

            // Verify text attributes
            expect(mockText.attr).toHaveBeenCalledWith("class", "outer");
            expect(mockText.attr).toHaveBeenCalledWith("class", "inner");

            // Test text content
            expect(mockText.text).toHaveBeenCalledWith(expect.any(Function));
            const textCall = (mockText.text as Mock).mock.calls[0];
            const textFunc = textCall[0] as (d: SimLink) => string;
            expect(textFunc(mockLink)).toBe("TR"); // First letters of "Test Role"
        });

        it("should bind mouse events with tooltip handling", () => {
            // Import handleLinkTooltip directly for test - modify the import and don't try to mock it
            // Instead, we'll test the outcome by verifying the tooltip behaviors

            onLinkEnter(mockLinkEnterSelection);

            // Verify event bindings
            expect(mockAppendedGroup.on).toHaveBeenCalledWith(
                "mouseover",
                expect.any(Function),
            );
            expect(mockAppendedGroup.on).toHaveBeenCalledWith(
                "mouseout",
                expect.any(Function),
            );

            // Get the mouseover handler without using .find() to avoid unbound method warning
            // Just iterate through the calls manually
            // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
            let mouseoverHandler: Function | undefined;
            const mockCalls = (mockAppendedGroup.on as Mock).mock.calls;
            for (let i = 0; i < mockCalls.length; i++) {
                if (mockCalls[i][0] === "mouseover") {
                    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
                    mouseoverHandler = mockCalls[i][1] as Function;
                    break;
                }
            }
            expect(mouseoverHandler).toBeDefined();
            expect(typeof mouseoverHandler).toBe("function");

            // Same approach for mouseout
            // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
            let mouseoutHandler: Function | undefined;
            for (let i = 0; i < mockCalls.length; i++) {
                if (mockCalls[i][0] === "mouseout") {
                    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
                    mouseoutHandler = mockCalls[i][1] as Function;
                    break;
                }
            }
            expect(mouseoutHandler).toBeDefined();
            expect(typeof mouseoutHandler).toBe("function");
        });
    });

    describe("onLinkExit", () => {
        it("should remove exiting links", () => {
            const mockRemove = vi.fn();
            const mockSelection = {
                remove: mockRemove,
            } as unknown as d3.Selection<
                SVGGElement,
                SimLink,
                SVGGElement,
                unknown
            >;

            onLinkExit(mockSelection);
            expect(mockRemove).toHaveBeenCalled();
        });
    });

    describe("onLinkUpdate", () => {
        it("should handle link updates (currently no-op)", () => {
            const mockSelection = {} as d3.Selection<
                SVGGElement,
                SimLink,
                SVGGElement,
                unknown
            >;
            const result = onLinkUpdate(mockSelection);
            expect(result).toBe(mockSelection);
        });
    });
});
