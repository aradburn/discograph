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
    // Using any type for mocks in tests is acceptable when we need flexibility
    const createMockSelectionWithData = (): any => {
        let storedData: any[] = [];

        const selection: any = {
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

    describe("private methods coverage", () => {
        beforeEach(() => {
            loading.init([800, 600]);
        });

        // Helper types for D3 mocks
        interface MockEnterSelection {
            append: ReturnType<typeof vi.fn>;
            attr: ReturnType<typeof vi.fn>;
            each: (fn: (d: any, i: number) => void) => void;
        }

        interface MockUpdateSelection {
            transition: ReturnType<typeof vi.fn>;
            duration: ReturnType<typeof vi.fn>;
            delay: ReturnType<typeof vi.fn>;
            attrTween: ReturnType<typeof vi.fn>;
            size: ReturnType<typeof vi.fn>;
        }

        interface MockExitSelection extends MockUpdateSelection {
            on: ReturnType<typeof vi.fn>;
        }

        interface MockRotateSelection {
            each: (fn: (d: any, i: number) => void) => void;
            attr: ReturnType<typeof vi.fn>;
            // Add all required methods for Selection type, but as optional
            select?: ReturnType<typeof vi.fn>;
            selectAll?: ReturnType<typeof vi.fn>;
            filter?: ReturnType<typeof vi.fn>;
            merge?: ReturnType<typeof vi.fn>;
            nodes?: ReturnType<typeof vi.fn>;
            node?: ReturnType<typeof vi.fn>;
            size?: ReturnType<typeof vi.fn>;
            empty?: ReturnType<typeof vi.fn>;
            data?: ReturnType<typeof vi.fn>;
            enter?: ReturnType<typeof vi.fn>;
            exit?: ReturnType<typeof vi.fn>;
            transition?: ReturnType<typeof vi.fn>;
            call?: ReturnType<typeof vi.fn>;
            on?: ReturnType<typeof vi.fn>;
            dispatch?: ReturnType<typeof vi.fn>;
            raise?: ReturnType<typeof vi.fn>;
            lower?: ReturnType<typeof vi.fn>;
            classed?: ReturnType<typeof vi.fn>;
            style?: ReturnType<typeof vi.fn>;
            property?: ReturnType<typeof vi.fn>;
            text?: ReturnType<typeof vi.fn>;
            html?: ReturnType<typeof vi.fn>;
            append?: ReturnType<typeof vi.fn>;
            insert?: ReturnType<typeof vi.fn>;
            remove?: ReturnType<typeof vi.fn>;
            clone?: ReturnType<typeof vi.fn>;
            datum?: ReturnType<typeof vi.fn>;
            sort?: ReturnType<typeof vi.fn>;
            order?: ReturnType<typeof vi.fn>;
        }

        // Helper to call private methods in a type-safe way (for test-only private access)
        // Intentionally unsafe: used only for test coverage of private methods. Linter warning accepted.
        function callPrivate<T>(
            instance: object,
            method: string,
            ...args: any[]
        ): T {
            return (instance as any)[method](...args);
        }

        it("should call transitionEnter and set arc properties", () => {
            const [data] = loading.makeArray();
            const mockEnter: MockEnterSelection = {
                append: vi.fn().mockReturnThis(),
                attr: vi.fn().mockReturnThis(),
                each: (fn) => {
                    data.forEach((d, i) => fn(d, i));
                },
            };
            callPrivate(loading, "transitionEnter", mockEnter);
            expect(mockEnter.append).toHaveBeenCalledWith("path");
            expect(mockEnter.attr).toHaveBeenCalledWith("class", "arc");
            expect(mockEnter.attr).toHaveBeenCalledWith(
                "d",
                expect.any(Function),
            );
            expect(mockEnter.attr).toHaveBeenCalledWith(
                "fill",
                expect.any(Function),
            );
            expect(data[0].innerRadius).toBe(0);
            expect(data[0].outerRadius).toBe(0);
            expect(data[0].hasTimer).toBe(false);
        });

        it("should call transitionUpdate and update arc radii", () => {
            const [data] = loading.makeArray();
            const mockSelection: MockUpdateSelection = {
                transition: vi.fn().mockReturnThis(),
                duration: vi.fn().mockReturnThis(),
                delay: vi.fn().mockReturnThis(),
                attrTween: vi.fn(
                    (name: string, tween: (d: any) => (t: number) => void) => {
                        // Simulate tween call
                        const fn = tween(data[0]);
                        fn(0.5);
                        return mockSelection;
                    },
                ),
                size: vi.fn(() => 1),
            };
            const barScale = (v: number): number => v * 10;
            callPrivate(loading, "transitionUpdate", mockSelection, barScale);
            expect(mockSelection.transition).toHaveBeenCalled();
            expect(mockSelection.duration).toHaveBeenCalled();
            expect(mockSelection.delay).toHaveBeenCalled();
            expect(mockSelection.attrTween).toHaveBeenCalledWith(
                "d",
                expect.any(Function),
            );
            expect(typeof data[0].innerRadius).toBe("number");
            expect(typeof data[0].outerRadius).toBe("number");
        });

        it("should call transitionExit and remove arcs", () => {
            const [data] = loading.makeArray();
            const mockRemove = vi.fn();
            // Minimal Selection mock for d3.select
            const minimalSelection = {
                remove: mockRemove,
                select: vi.fn(),
                selectAll: vi.fn(),
                filter: vi.fn(),
                merge: vi.fn(),
                nodes: vi.fn(),
                node: vi.fn(),
                size: vi.fn(),
                empty: vi.fn(),
                data: vi.fn(),
                enter: vi.fn(),
                exit: vi.fn(),
                transition: vi.fn(),
                call: vi.fn(),
                on: vi.fn(),
                dispatch: vi.fn(),
                raise: vi.fn(),
                lower: vi.fn(),
                classed: vi.fn(),
                style: vi.fn(),
                property: vi.fn(),
                text: vi.fn(),
                html: vi.fn(),
                append: vi.fn(),
                insert: vi.fn(),
                clone: vi.fn(),
                datum: vi.fn(),
                sort: vi.fn(),
                order: vi.fn(),
            };
            const mockSelection: MockExitSelection = {
                transition: vi.fn().mockReturnThis(),
                duration: vi.fn().mockReturnThis(),
                delay: vi.fn().mockReturnThis(),
                attrTween: vi.fn().mockReturnThis(),
                on: vi.fn((event: string, handler: (d: any) => void) => {
                    // Simulate on('end')
                    handler(data[0]);
                    return mockSelection;
                }),
                size: vi.fn(() => 1),
            };
            // Use vi.spyOn to mock d3.select (read-only property)
            const selectSpy = vi
                .spyOn(d3, "select")
                .mockImplementation(() => minimalSelection as any);
            callPrivate(loading, "transitionExit", mockSelection);
            expect(mockSelection.transition).toHaveBeenCalled();
            expect(mockSelection.duration).toHaveBeenCalled();
            expect(mockSelection.delay).toHaveBeenCalled();
            expect(mockSelection.attrTween).toHaveBeenCalled();
            expect(mockSelection.on).toHaveBeenCalledWith(
                "end",
                expect.any(Function),
            );
            expect(data[0].active).toBe(false);
            expect(mockRemove).toHaveBeenCalled();
            selectSpy.mockRestore();
        });

        it("should call rotate and start/stop timers", () => {
            const [data] = loading.makeArray();
            data[0].hasTimer = false;
            data[0].active = true;
            data[0].rotationRate = 1;
            data[0].outerRadius = 10;
            const mockAttr = vi.fn();
            const mockSelection: MockRotateSelection = {
                each: (fn) => {
                    fn(data[0], 0);
                },
                attr: mockAttr,
            };
            // Use vi.spyOn to mock d3.interval (read-only property)
            const stopFn = vi.fn();
            // Timer interface shim for d3.interval
            const timerShim = { stop: stopFn, restart: vi.fn() };
            // Simulate the timer callback sequence
            const intervalSpy = vi
                .spyOn(d3, "interval")
                .mockImplementation(
                    (cb: (elapsed: number) => void, ms: number) => {
                        // First call: d.active is true, d.timer not yet set
                        cb(5);
                        // Now set d.timer to the mock timer object
                        data[0].timer = timerShim;
                        // Set d.active to false to trigger stop
                        data[0].active = false;
                        // Second call: should trigger d.timer.stop()
                        cb(10);
                        return timerShim;
                    },
                );
            callPrivate(loading, "rotate", mockSelection);
            expect(d3.interval).toHaveBeenCalled();
            expect(mockAttr).toHaveBeenCalledWith(
                "transform",
                expect.any(Function),
            );
            expect(stopFn).toHaveBeenCalled();
            // Test hasTimer short-circuit
            data[0].hasTimer = true;
            callPrivate(loading, "rotate", mockSelection);
            // Should not call d3.interval again
            expect(d3.interval).toHaveBeenCalledTimes(1);
            intervalSpy.mockRestore();
        });
    });
});
