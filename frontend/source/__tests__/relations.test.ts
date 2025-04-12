/* eslint-disable @typescript-eslint/no-unsafe-member-access */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import * as d3 from "d3";
import * as relationsModule from "../relations";
import {
    initRelations,
    setRelationsData,
    createRadialChart,
    handleZoom,
    clearRelationsLayer,
    type RelationsData,
    type RelationsArcData,
} from "../relations";
import { dg } from "../dg";

// Define types for d3 mocks
type D3Selection = d3.Selection<SVGElement, unknown, null, undefined>;
interface MockD3Selection {
    attr: ReturnType<typeof vi.fn>;
    append: ReturnType<typeof vi.fn>;
    remove: ReturnType<typeof vi.fn>;
    on: ReturnType<typeof vi.fn>;
    data: ReturnType<typeof vi.fn>;
    enter: ReturnType<typeof vi.fn>;
    selectAll: ReturnType<typeof vi.fn>;
    select: ReturnType<typeof vi.fn>;
    raise: ReturnType<typeof vi.fn>;
    each: ReturnType<typeof vi.fn>;
    transition: ReturnType<typeof vi.fn>;
    ease: ReturnType<typeof vi.fn>;
    duration: ReturnType<typeof vi.fn>;
    delay: ReturnType<typeof vi.fn>;
    attrTween: ReturnType<typeof vi.fn>;
    text: ReturnType<typeof vi.fn>;
}

// Define mock for D3 arc generator
interface MockD3Arc {
    startAngle: ReturnType<typeof vi.fn>;
    endAngle: ReturnType<typeof vi.fn>;
    innerRadius: ReturnType<typeof vi.fn>;
    outerRadius: ReturnType<typeof vi.fn>;
}

// Test data for relations
const sampleRelationsData: RelationsData = {
    results: [
        { year: 2020, category: "artist", role: "Producer" },
        { year: 2020, category: "artist", role: "Engineer" },
        { year: 2021, category: "label", role: "Producer" },
        { year: 2021, category: "label", role: "Artist" },
        { year: 2022, category: "release", role: "Engineer" },
    ],
};

// Create a properly typed mock selection
const createMockSelection = (): MockD3Selection => {
    const mockSelection = {
        attr: vi.fn(),
        append: vi.fn(),
        remove: vi.fn(),
        on: vi.fn(),
        data: vi.fn(),
        enter: vi.fn(),
        selectAll: vi.fn(),
        select: vi.fn(),
        raise: vi.fn(),
        each: vi.fn(),
        transition: vi.fn(),
        ease: vi.fn(),
        duration: vi.fn(),
        delay: vi.fn(),
        attrTween: vi.fn(),
        text: vi.fn(),
    };

    // Setup method chaining
    mockSelection.attr.mockReturnValue(mockSelection);
    mockSelection.append.mockReturnValue(mockSelection);
    mockSelection.on.mockReturnValue(mockSelection);
    mockSelection.data.mockReturnValue(mockSelection);
    mockSelection.enter.mockReturnValue(mockSelection);
    mockSelection.selectAll.mockReturnValue(mockSelection);
    mockSelection.select.mockReturnValue(mockSelection);
    mockSelection.transition.mockReturnValue(mockSelection);
    mockSelection.ease.mockReturnValue(mockSelection);
    mockSelection.duration.mockReturnValue(mockSelection);
    mockSelection.delay.mockReturnValue(mockSelection);
    mockSelection.attrTween.mockReturnValue(mockSelection);
    mockSelection.text.mockReturnValue(mockSelection);

    return mockSelection;
};

// Mock external dependencies
vi.mock("d3", () => {
    // Create a mock arc generator function
    const createMockArcGenerator = () => {
        const mockArc: any = (d: any) => "M0,0L10,10Z"; // Return a simple SVG path

        mockArc.startAngle = vi.fn().mockReturnValue(mockArc);
        mockArc.endAngle = vi.fn().mockReturnValue(mockArc);
        mockArc.innerRadius = vi.fn().mockReturnValue(mockArc);
        mockArc.outerRadius = vi.fn().mockReturnValue(mockArc);
        mockArc.padAngle = vi.fn().mockReturnValue(mockArc);

        // eslint-disable-next-line @typescript-eslint/no-unsafe-return
        return mockArc;
    };

    // Create a mock selection factory that will be used by d3.select
    const mockSelectionFactory = () => {
        const mockSelection = {
            attr: vi.fn().mockReturnThis(),
            append: vi.fn(function () {
                return mockSelection;
            }),
            remove: vi.fn().mockReturnThis(),
            on: vi.fn().mockReturnThis(),
            data: vi.fn().mockReturnThis(),
            enter: vi.fn().mockReturnThis(),
            selectAll: vi.fn().mockReturnThis(),
            select: vi.fn().mockReturnThis(),
            raise: vi.fn().mockReturnThis(),
            each: vi.fn(function (fn) {
                // Execute the callback with a dummy data object
                // eslint-disable-next-line @typescript-eslint/no-unsafe-call
                fn(
                    {
                        role: "Producer",
                        count: 2,
                        startAngle: 0,
                        endAngle: 2,
                        innerRadius: 0,
                        outerRadius: 0,
                    },
                    0,
                );
                return mockSelection;
            }),
            transition: vi.fn().mockReturnThis(),
            ease: vi.fn().mockReturnThis(),
            duration: vi.fn().mockReturnThis(),
            delay: vi.fn().mockReturnThis(),
            attrTween: vi.fn(function (attr, callback) {
                // Call the callback with a dummy data object
                // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call
                const interpolateFn = callback({
                    role: "Producer",
                    count: 2,
                    startAngle: 0,
                    endAngle: 2,
                    innerRadius: 0,
                    outerRadius: 0,
                });
                // Call the returned function with a time value
                // eslint-disable-next-line @typescript-eslint/no-unsafe-call
                interpolateFn(0.5);
                return mockSelection;
            }),
            text: vi.fn().mockReturnThis(),
        };
        return mockSelection;
    };

    return {
        select: vi.fn(() => mockSelectionFactory()),
        group: vi.fn(() => new Map()),
        // eslint-disable-next-line @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-assignment
        sort: vi.fn((arr) => (Array.isArray(arr) ? [...arr] : [])),
        rollup: vi.fn(
            () =>
                new Map([
                    ["Producer", 2],
                    ["Engineer", 2],
                    ["Artist", 1],
                ]),
        ),
        extent: vi.fn(() => [1, 5]),
        InternMap: vi.fn().mockImplementation(() => new Map()),
        scaleSqrt: vi.fn(() => {
            const scale = (input: number) => input * 10;
            scale.domain = vi.fn().mockReturnThis();
            scale.range = vi.fn().mockReturnThis();
            scale.exponent = vi.fn().mockReturnThis();
            return scale;
        }),
        // eslint-disable-next-line @typescript-eslint/no-unsafe-return
        arc: vi.fn(() => createMockArcGenerator()),
        easeElastic: vi.fn(),
        interpolate: vi.fn((a, b) => {
            // eslint-disable-next-line @typescript-eslint/no-unsafe-return
            return (t: number) => a + (b - a) * t;
        }),
    };
});

// Mock global dg object
vi.mock("../dg", () => ({
    dg: {
        dimensions: [800, 600],
        svg_dimensions: [1000, 800],
        relations: {
            data: { results: [] },
            byYear: new Map(),
            byRole: new Map(),
            layers: {
                root: null,
            },
        },
        arc: {
            innerRadius: vi.fn().mockReturnThis(),
            outerRadius: vi.fn().mockReturnThis(),
            startAngle: vi.fn().mockReturnThis(),
            endAngle: vi.fn().mockReturnThis(),
        },
    },
}));

describe("Relations Module", () => {
    let mockSelection: MockD3Selection;
    let consoleSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
        // Setup DOM environment
        document.body.innerHTML = '<svg id="svg"></svg>';

        // Create fresh mock selection for each test
        mockSelection = createMockSelection();

        // Reset mocks to start fresh for each test
        vi.resetAllMocks();

        // We don't need to manually configure d3.select anymore since we've mocked it in vi.mock

        // Reset the dg.relations state
        dg.relations.layers.root = null;
        dg.relations.data = { results: [] };
        dg.relations.byYear = new Map();
        dg.relations.byRole = new Map();

        // Spy on console.log to capture output
        consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    });

    afterEach(() => {
        // Cleanup
        document.body.innerHTML = "";
        vi.restoreAllMocks();
        consoleSpy.mockRestore();
    });

    describe("initRelations", () => {
        it("should initialize the relations layer", () => {
            // Call the function
            initRelations();

            // Verify d3.select was called with the correct selector
            expect(d3.select).toHaveBeenCalledWith("#svg");

            // Verify root layer was set in dg.relations
            expect(dg.relations.layers.root).not.toBeNull();
        });
    });

    describe("setRelationsData", () => {
        it("should set relations data and process it correctly", () => {
            // Setup spies
            const groupSpy = vi.spyOn(d3, "group");
            const sortSpy = vi.spyOn(d3, "sort");
            const rollupSpy = vi.spyOn(d3, "rollup");

            // Call the function with sample data
            setRelationsData(sampleRelationsData);

            // Verify data was set
            expect(dg.relations.data).toBe(sampleRelationsData);

            // Verify d3.group was called with correct arguments
            expect(groupSpy).toHaveBeenCalledWith(
                sampleRelationsData.results,
                expect.any(Function),
                expect.any(Function),
            );

            // Verify d3.sort was called with correct arguments
            expect(sortSpy).toHaveBeenCalledWith(
                sampleRelationsData.results,
                expect.any(Function),
            );

            // Verify d3.rollup was called with correct arguments
            expect(rollupSpy).toHaveBeenCalledWith(
                expect.anything(),
                expect.any(Function),
                expect.any(Function),
            );

            // Verify console.log was called
            expect(consoleSpy).toHaveBeenCalledWith(
                "dg.relations.byRole: ",
                expect.anything(),
            );
        });
    });

    describe("createRadialChart", () => {
        beforeEach(() => {
            // Setup necessary state
            dg.relations.byRole = new Map([
                ["Producer", 2],
                ["Engineer", 2],
                ["Artist", 1],
            ]);

            // Setup root layer for the chart
            initRelations();
        });

        it("should create a radial chart visualization", () => {
            // Setup spies
            const extentSpy = vi.spyOn(d3, "extent");
            const scaleSqrtSpy = vi.spyOn(d3, "scaleSqrt");
            const arcSpy = vi.spyOn(d3, "arc");
            const interpolateSpy = vi.spyOn(d3, "interpolate");

            // Call the function
            createRadialChart();

            // Verify console logs
            expect(consoleSpy).toHaveBeenCalledWith("createRadialChart()");

            // Verify d3.extent was called to get data ranges
            expect(extentSpy).toHaveBeenCalled();

            // Verify d3.scaleSqrt was called to create scale
            expect(scaleSqrtSpy).toHaveBeenCalled();

            // Verify d3.arc was called to create arc generator
            expect(arcSpy).toHaveBeenCalled();

            // Verify d3.interpolate was called for animation
            expect(interpolateSpy).toHaveBeenCalled();

            // Verify d3.select was called
            expect(d3.select).toHaveBeenCalled();
        });
    });

    describe("handleZoom", () => {
        it("should apply zoom transform to the root layer", () => {
            // Setup root layer
            const mockRoot = { attr: vi.fn() };
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
            dg.relations.layers.root = mockRoot as any;

            // Create a mock transform object
            const mockTransform = {
                toString: vi
                    .fn()
                    .mockReturnValue("translate(100,100) scale(2)"),
            };

            // Call the function
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
            handleZoom({ transform: mockTransform as any });

            // Verify transform was applied
            expect(mockRoot.attr).toHaveBeenCalledWith(
                "transform",
                "translate(100,100) scale(2)",
            );
        });
    });

    describe("clearRelationsLayer", () => {
        it("should remove the relations layer from the SVG", () => {
            // Call the function
            clearRelationsLayer();

            // Verify d3.select was called with the correct selector
            expect(d3.select).toHaveBeenCalledWith("#relationsLayer");
        });
    });
});
