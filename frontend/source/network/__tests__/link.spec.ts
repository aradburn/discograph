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
import { linkTooltip } from "../tooltips";
import type { SimLink } from "../data";

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
    // eslint-disable-next-line @typescript-eslint/consistent-type-imports
    const mod = await importOriginal<typeof import("d3")>();
    return {
        ...mod,
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
        d3.BaseType,
        unknown
    >;
    type LinkSelection = d3.Selection<
        SVGGElement,
        SimLink,
        d3.BaseType,
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
    let mockLinkSelection: LinkSelection;

    const mockLink: SimLink = {
        key: "source-target-role-1-2",
        source: {
            key: "source",
            name: "Source",
            type: "artist",
            size: 1,
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
        },
        target: {
            key: "target",
            name: "Target",
            type: "label",
            size: 1,
            distance: 2,
            x: 0,
            y: 0,
            fx: null,
            fy: null,
            index: 1,
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
        },
        role: "Test Role",
        distance: 1,
        isSpline: false,
        intermediate: null,
        highlighted: false,
        selected: false,
    };

    beforeEach(() => {
        // Reset all mocks
        vi.clearAllMocks();

        // Create mock elements with chainable methods
        mockPath = {
            attr: vi.fn().mockReturnThis(),
            append: vi.fn().mockReturnThis(),
            on: vi.fn().mockReturnThis(),
            text: vi.fn().mockReturnThis(),
            querySelector: vi
                .fn()
                .mockReturnValue(document.createElement("text")),
        };

        mockText = {
            attr: vi.fn().mockReturnThis(),
            append: vi.fn().mockReturnThis(),
            on: vi.fn().mockReturnThis(),
            text: vi.fn().mockReturnThis(),
            querySelector: vi
                .fn()
                .mockReturnValue(document.createElement("text")),
        };

        mockAppendedGroup = {
            attr: vi.fn().mockReturnThis(),
            append: vi
                .fn()
                .mockImplementation((type: string): MockD3Element => {
                    if (type === "path") return mockPath;
                    if (type === "text") return mockText;
                    return mockAppendedGroup;
                }),
            on: vi.fn().mockReturnThis(),
            text: vi.fn().mockReturnThis(),
            querySelector: vi
                .fn()
                .mockReturnValue(document.createElement("text")),
        };

        // Create mock selections
        mockLinkEnterSelection = {
            append: vi.fn().mockReturnValue(mockAppendedGroup),
        } as unknown as LinkEnterSelection;

        mockLinkSelection = {
            remove: vi.fn(),
            select: vi.fn(),
            selectAll: vi.fn(),
        } as unknown as LinkSelection;

        vi.useFakeTimers();
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
            const idCall = idCalls.find((call) => call[0] === "id");
            expect(idCall).toBeTruthy();
            const idFunc = idCall?.[1] as (d: SimLink) => string;
            expect(idFunc(mockLink)).toBe("link-source-target-role-1-2");

            // Test class attribute
            const classCall = idCalls.find((call) => call[0] === "class");
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
            const classCall = (mockPath.attr as Mock).mock.calls.find(
                (call) => call[0] === "class",
            );
            expect(classCall).toBeTruthy();
            const classFunc = classCall?.[1] as (d: SimLink) => string;
            expect(classFunc(mockLink)).toBe(
                "inner distance-1 mock-color-class",
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

            // Test mouseover handler
            const mouseoverCall = (
                mockAppendedGroup.on as Mock
            ).mock.calls.find((call) => call[0] === "mouseover");
            expect(mouseoverCall).toBeTruthy();
            const mouseoverHandler = mouseoverCall?.[1] as (
                event: MouseEvent,
                d: SimLink,
            ) => void;

            // Create mock context and execute handler
            const mockContext = {
                querySelector: vi
                    .fn()
                    .mockReturnValue(document.createElement("text")),
            };
            mouseoverHandler.call(
                mockContext,
                new MouseEvent("mouseover"),
                mockLink,
            );
            vi.advanceTimersByTime(250);
            expect(linkTooltip.show).toHaveBeenCalledWith(
                mockLink,
                expect.any(Element),
            );

            // Test mouseout handler
            const mouseoutCall = (mockAppendedGroup.on as Mock).mock.calls.find(
                (call) => call[0] === "mouseout",
            );
            expect(mouseoutCall).toBeTruthy();
            const mouseoutHandler = mouseoutCall?.[1] as (
                event: MouseEvent,
                d: SimLink,
            ) => void;

            // Execute mouseout handler
            mouseoutHandler.call(
                mockContext,
                new MouseEvent("mouseout"),
                mockLink,
            );
            vi.advanceTimersByTime(250);
            expect(linkTooltip.hide).toHaveBeenCalled();
        });
    });

    describe("onLinkExit", () => {
        it("should remove exiting links", () => {
            onLinkExit(mockLinkSelection);
            // @typescript-eslint/unbound-method: These are test mocks, no actual 'this' binding needed
            const removeMethod = mockLinkSelection.remove as unknown as Mock<
                () => void
            >;
            expect(removeMethod).toHaveBeenCalled();
        });
    });

    describe("onLinkUpdate", () => {
        it("should handle link updates (currently no-op)", () => {
            onLinkUpdate(mockLinkSelection);
            // Currently empty implementation, but test exists for future implementation
            expect(true).toBe(true);
        });
    });
});
