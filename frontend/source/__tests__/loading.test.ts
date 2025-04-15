import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import type { Selection, BaseType } from "d3";
import { DOM_IDS } from "../constants";

// Define mock types that match the Loading class's expected types
type D3Selection = Selection<BaseType, unknown, null, undefined>;

type MockD3Selection = {
    attr: (name: string, value: any) => MockD3Selection;
    selectAll: (selector: string) => MockD3Selection;
    data: {
        (): unknown[];
        <T>(data?: T[]): MockD3Selection;
    };
    enter: () => MockD3Selection;
    append: (type: string) => MockD3Selection;
    exit: () => MockD3Selection;
    transition: () => MockD3Selection;
    duration: (ms: number) => MockD3Selection;
    delay: (ms: number | ((d: any, i: number) => number)) => MockD3Selection;
    attrTween: (name: string, tween: any) => MockD3Selection;
    on: (event: string, handler: any) => MockD3Selection;
    size: () => number;
    style: (name: string, value: any) => MockD3Selection;
    remove: () => MockD3Selection;
    each: {
        (fn: (d: any, i: number) => void): MockD3Selection;
        (func: d3.ValueFn<BaseType, unknown, void>): D3Selection;
    };
} & Partial<D3Selection>;

// Mock d3 with proper implementation
vi.mock("d3", () => {
    const createMockSelectionWithData = () => {
        let storedData: any[] = [];

        const selection = {
            attr: vi.fn().mockReturnThis(),
            selectAll: vi.fn().mockReturnThis(),
            data: vi.fn((data?: any[]) => {
                if (data !== undefined) {
                    storedData = data;
                }
                return selection;
            }),
            enter: vi.fn().mockReturnThis(),
            append: vi.fn().mockReturnThis(),
            exit: vi.fn().mockReturnThis(),
            transition: vi.fn().mockReturnThis(),
            duration: vi.fn().mockReturnThis(),
            delay: vi.fn().mockReturnThis(),
            attrTween: vi.fn().mockReturnThis(),
            on: vi.fn().mockReturnThis(),
            size: vi.fn(() => storedData.length),
            style: vi.fn().mockReturnThis(),
            remove: vi.fn().mockReturnThis(),
            each: vi.fn().mockReturnThis(),
        };

        return selection;
    };

    return {
        select: vi.fn(() => {
            const baseSelection = createMockSelectionWithData();

            // Override append to create new mock selections
            baseSelection.append = vi.fn(() => {
                const childSelection = createMockSelectionWithData();
                // Make sure that selectAll returns a selection with data method
                childSelection.selectAll = vi.fn(() =>
                    createMockSelectionWithData(),
                );
                return childSelection;
            });

            return baseSelection;
        }),
        arc: vi.fn(() => {
            const mockArc = (data: any): string => "M0,0L10,10"; // Mock SVG path

            // Store the accessor functions when these methods are called
            mockArc.startAngle = vi.fn().mockReturnValue(mockArc);
            mockArc.endAngle = vi.fn().mockReturnValue(mockArc);
            mockArc.innerRadius = vi.fn().mockReturnValue(mockArc);
            mockArc.outerRadius = vi.fn().mockReturnValue(mockArc);

            return mockArc;
        }),
        scaleLinear: vi.fn(() => ({
            domain: vi.fn().mockReturnThis(),
            range: vi.fn().mockReturnThis(),
        })),
        scaleOrdinal: vi.fn(() => {
            const scale = (value: string): string => `#${value}`;
            scale.domain = vi.fn().mockReturnValue(scale);
            scale.range = vi.fn().mockReturnValue(scale);
            return scale;
        }),
        schemeCategory10: ["#1f77b4", "#ff7f0e", "#2ca02c"],
        extent: vi.fn((values: number[]) => {
            if (!values || values.length === 0) return [0, 0];
            return [Math.min(...values), Math.max(...values)];
        }),
        interval: vi.fn(() => ({
            on: vi.fn(),
            stop: vi.fn(),
        })),
        interpolate: vi.fn(
            (start: number, end: number): ((t: number) => number) =>
                (t: number): number =>
                    start + (end - start) * t,
        ),
    };
});

import * as d3 from "d3";
import { Loading } from "../loading";

describe("Loading", () => {
    let loading: Loading;

    beforeEach(() => {
        // Reset all mocks
        vi.clearAllMocks();

        // Setup DOM mock
        document.body.innerHTML = '<div id="svg"></div>';
        document.body.innerHTML += '<div id="page-loading"></div>';

        loading = new Loading();
    });

    afterEach(() => {
        document.body.innerHTML = "";
        vi.resetAllMocks();
    });

    describe("constructor", () => {
        it("should initialize with default values", () => {
            expect(loading).toBeDefined();
            expect(loading["barHeight"]).toBe(200);
            expect(loading["layer"]).toBeNull();
            expect(loading["selection"]).toBeNull();
        });
    });

    describe("init", () => {
        it("should initialize the SVG layer with correct dimensions", () => {
            const dimensions: [number, number] = [800, 600];
            loading.init(dimensions);

            expect(d3.select).toHaveBeenCalledWith(DOM_IDS.SVG_ID);
            // Check if d3.arc was called
            expect(d3.arc).toHaveBeenCalled();
        });
    });

    describe("makeArray", () => {
        it("should generate correct number of data points", () => {
            const [data, extent] = loading.makeArray();
            expect(data).toHaveLength(10);
            expect(extent).toHaveLength(2);
            expect(extent[0]).toBeLessThanOrEqual(extent[1]);
        });

        it("should generate valid arc data objects", () => {
            const [data] = loading.makeArray();
            const arcData = data[0];

            expect(arcData).toHaveProperty("active", true);
            expect(arcData).toHaveProperty("startAngle");
            expect(arcData).toHaveProperty("endAngle");
            expect(arcData).toHaveProperty("rotationRate");
            expect(arcData).toHaveProperty("targetInnerRadius");
            expect(arcData).toHaveProperty("targetOuterRadius");
            expect(arcData.targetInnerRadius).toBeLessThanOrEqual(
                arcData.targetOuterRadius,
            );
        });
    });

    describe("toggle", () => {
        beforeEach(() => {
            loading.init([800, 600]);
        });

        it("should show loading animation when status is true", () => {
            loading.toggle(true);
            const pageLoading = document.getElementById("page-loading");
            expect(pageLoading?.style.display).toBe("block");
        });

        it("should hide loading animation when status is false", () => {
            loading.toggle(false);
            const pageLoading = document.getElementById("page-loading");
            expect(pageLoading?.style.display).toBe("none");
        });
    });

    describe("update", () => {
        beforeEach(() => {
            loading.init([800, 600]);
        });

        it("should handle empty data array", () => {
            loading.update([], [0, 1]);
            // Check if scaleLinear was called
            expect(d3.scaleLinear).toHaveBeenCalled();
        });

        it("should not update if layer is not initialized", () => {
            const newLoading = new Loading();
            const consoleSpy = vi.spyOn(console, "error");

            newLoading.update([], [0, 1]);
            expect(consoleSpy).toHaveBeenCalledWith(
                "Layer is not initialized.",
            );
        });

        it("should update with valid data", () => {
            const [data, extent] = loading.makeArray();
            loading.update(data, extent);

            // Check if scaleLinear was called
            expect(d3.scaleLinear).toHaveBeenCalled();
        });
    });
});
