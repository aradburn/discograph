/**
 * @fileoverview Relations visualization module for Discograph
 * This module handles the creation and management of radial/circular relationship visualizations
 * using D3.js. It provides functionality for creating interactive circular charts that display
 * relationships between different roles or entities.
 */

import * as d3 from "d3";

/**
 * Interface for arc data used in the loading animation
 */
interface ArcData {
    active: boolean;
    startAngle: number;
    endAngle: number;
    rotationRate: number;
    targetInnerRadius: number;
    targetOuterRadius: number;
    innerRadius?: number;
    outerRadius?: number;
    hasTimer?: boolean;
    timer?: d3.Timer;
}

/**
 * Class representing a loading visualization using D3.js
 */
export class Loading {
    private arc: d3.Arc<this, ArcData>;
    private barHeight: number;
    private layer: d3.Selection<
        SVGGElement,
        unknown,
        HTMLElement,
        unknown
    > | null;
    private selection: d3.Selection<
        SVGPathElement,
        ArcData,
        SVGGElement,
        unknown
    > | null;

    constructor() {
        this.arc = d3.arc<this, ArcData>();
        this.barHeight = 200;
        this.layer = null;
        this.selection = null;
    }

    /**
     * Initializes the loading animation by creating an SVG layer and setting up base configurations
     * @param {[number, number]} svgDimensions - Array containing [width, height] of the SVG
     */
    init(svgDimensions: [number, number]): void {
        const layer = d3
            .select("#svg")
            .append("g")
            .attr("id", "loadingLayer")
            .attr("class", "centered")
            .attr(
                "transform",
                `translate(${svgDimensions[0] / 2},${svgDimensions[1] / 2})`,
            );

        this.arc = d3
            .arc<this, ArcData>()
            .startAngle((d) => d.startAngle)
            .endAngle((d) => d.endAngle)
            .innerRadius((d) => d.innerRadius || 0)
            .outerRadius((d) => d.outerRadius || 0);

        this.barHeight = 200;
        this.layer = layer;
        this.selection = layer.selectAll<SVGPathElement, ArcData>("path");
    }

    /**
     * Generates random data for the loading animation arcs
     * @returns {[ArcData[], [number, number]]} [data, extent] - Array containing arc data objects and their value extents
     */
    makeArray(): [ArcData[], [number, number]] {
        const count = 10;
        const values: number[] = [];
        const data: ArcData[] = [];

        for (let i = 0; i < count; i++) {
            const pair = [Math.random(), Math.random()].sort();
            values.push(pair[0], pair[1]);

            data.push({
                active: true,
                startAngle: 2 * Math.PI * Math.random(),
                endAngle: 2 * Math.PI * Math.random(),
                rotationRate: Math.random() * 10,
                targetInnerRadius: pair[0],
                targetOuterRadius: pair[1],
            });
        }
        return [data, d3.extent(values) as [number, number]];
    }

    /**
     * Toggles the visibility of the loading animation
     * @param {boolean} status - Whether to show (true) or hide (false) the loading animation
     */
    toggle(status: boolean): void {
        const [data, extent] = status
            ? this.makeArray()
            : [[], [0, 0] as [number, number]];
        const pageLoadingElement = document.getElementById("page-loading");
        if (pageLoadingElement) {
            pageLoadingElement.style.display = status ? "block" : "none";
        }
        this.update(data, extent);
    }

    /**
     * Updates the loading animation with new data
     * @param {ArcData[]} data - Array of arc data objects
     * @param {[number, number]} extent - Min/max values for scaling
     */
    update(data: ArcData[], extent: [number, number]): void {
        if (!this.layer) {
            console.error("Layer is not initialized.");
            return;
        }

        const barScale = d3
            .scaleLinear()
            .domain(extent)
            .range([this.barHeight / 4, this.barHeight]);

        const dataSelection = this.layer
            .selectAll<SVGPathElement, ArcData>("path")
            .data(data);

        const selectionEnter = dataSelection.enter();
        this.transitionEnter(selectionEnter);
        this.transitionExit(dataSelection.exit());

        const updatedSelection = this.layer
            .selectAll<SVGPathElement, ArcData>("path")
            .data(data);

        this.transitionUpdate(updatedSelection, barScale);

        if (selectionEnter.size() > 0) {
            this.rotate(updatedSelection);
        }
    }

    /**
     * Handles the entry transition for new arcs
     * @param {d3.Selection<d3.EnterElement, ArcData, SVGGElement, unknown>} selection - D3 selection of entering elements
     */
    private transitionEnter(
        selection: d3.Selection<d3.EnterElement, ArcData, SVGGElement, unknown>,
    ): void {
        const scale = d3.scaleOrdinal(d3.schemeCategory10);

        selection
            .append("path")
            .attr("class", "arc")
            .attr("d", (d) => this.arc(d))
            .attr("fill", (_, i) => scale(i.toString()))
            .each((d) => {
                d.innerRadius = 0;
                d.outerRadius = 0;
                d.hasTimer = false;
            });
    }

    /**
     * Animates transitions for updating arcs
     * @param {d3.Selection<SVGPathElement, ArcData, SVGGElement, unknown>} selection - D3 selection to update
     * @param {d3.ScaleLinear<number, number>} barScale - Scale function for arc sizes
     */
    private transitionUpdate(
        selection: d3.Selection<SVGPathElement, ArcData, SVGGElement, unknown>,
        barScale: d3.ScaleLinear<number, number>,
    ): void {
        selection
            .transition()
            .duration(1000)
            .delay((_, i) => (selection.size() - i) * 100)
            .attrTween("d", (d) => {
                const inner = d3.interpolate(
                    d.innerRadius || 0,
                    barScale(d.targetInnerRadius),
                );
                const outer = d3.interpolate(
                    d.outerRadius || 0,
                    barScale(d.targetOuterRadius),
                );
                return (t) => {
                    d.innerRadius = inner(t);
                    d.outerRadius = outer(t);
                    return this.arc(d);
                };
            });
    }

    /**
     * Handles the exit transition for removing arcs
     * @param {d3.Selection<SVGPathElement, ArcData, SVGGElement, unknown>} selection - D3 selection of elements to remove
     */
    private transitionExit(
        selection: d3.Selection<SVGPathElement, ArcData, SVGGElement, unknown>,
    ): void {
        selection
            .transition()
            .duration(1000)
            .delay((_, i) => (selection.size() - i) * 100)
            .attrTween("d", (d) => {
                const inner = d3.interpolate(d.innerRadius || 0, 0);
                const outer = d3.interpolate(d.outerRadius || 0, 0);
                return (t) => {
                    d.innerRadius = inner(t);
                    d.outerRadius = outer(t);
                    return this.arc(d);
                };
            })
            .on("end", function (d) {
                d.active = false;
                d3.select(this).remove();
            });
    }

    /**
     * Manages continuous rotation animation of arcs
     * @param {d3.Selection<SVGPathElement, ArcData, SVGGElement, unknown>} selection - D3 selection of elements to rotate
     */
    private rotate(
        selection: d3.Selection<SVGPathElement, ArcData, SVGGElement, unknown>,
    ): void {
        selection.each((d) => {
            if (d.hasTimer) return;

            d.hasTimer = true;
            d.timer = d3.interval((elapsed) => {
                if (!d.active) {
                    console.log("stop timer");
                    d.timer?.stop();
                    d.hasTimer = false;
                    d.timer = undefined;
                }

                selection.attr("transform", () => {
                    let angle = elapsed * d.rotationRate;
                    const outerRadius = d.outerRadius || 0;
                    if (outerRadius > 0) {
                        angle = angle / outerRadius;
                    }
                    return `rotate(${angle})`;
                });
            }, 20);
        });
    }
}

// Export a singleton instance
export const loading = new Loading();
