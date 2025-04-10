/**
 * Network visualization initialization module
 * Sets up the SVG layers, zoom behavior, and force layout for the network visualization.
 * Creates a hierarchical structure of SVG groups for different visualization elements.
 */

import { initForceLayout, initForceSliders } from "./forceLayout";
import { dg } from "../dg";
import * as d3 from "d3";
import { hideAllTooltips } from "./tooltips";
import { VIEWPORT_SIZE_MULTIPLIER } from "../init";

type TransformFunction = (
    selection:
        | d3.Selection<d3.BaseType, unknown, d3.BaseType, unknown>
        | d3.Transition<d3.BaseType, unknown, d3.BaseType, unknown>,
    transform: d3.ZoomTransform,
    point?: [number, number],
) => void;

/**
 * Initializes the network visualization by setting up the SVG layers, zoom behavior, and force layout.
 * Creates a hierarchical structure of SVG groups for different visualization elements:
 * - halo: For node highlighting effects
 * - link: For connections between nodes
 * - node: For the actual nodes
 * - text: For node labels
 */
export const initNetwork = (): void => {
    const svgElement = d3.select("#svg");
    const root = svgElement.append("g").attr("id", "networkLayer");
    dg.network.layers.root = root;
    dg.network.layers.halo = root.append("g").attr("id", "haloLayer");
    dg.network.layers.link = root.append("g").attr("id", "linkLayer");
    dg.network.layers.node = root.append("g").attr("id", "nodeLayer");
    dg.network.layers.text = root.append("g").attr("id", "textLayer");

    dg.network.zoom = d3
        .zoom<SVGSVGElement, unknown>()
        .extent([
            [0, 0],
            [dg.svg_dimensions[0], dg.svg_dimensions[1]],
        ])
        .scaleExtent([1, 8])
        .on("zoom", onNetworkZoom);

    svgElement.call(dg.network.zoom);

    resetNetworkTransform();

    initForceLayout();
    initForceSliders();
};

/**
 * Resets the network visualization's transform to its default state.
 * Animates the transition over 750ms, centering the view based on the viewport size multiplier.
 * Uses the current zoom state to calculate the proper inversion for smooth animation.
 */
export const resetNetworkTransform = (): void => {
    const f = 0.5;
    const scale =
        Math.min(
            dg.svg_dimensions[0] / dg.dimensions[0],
            dg.svg_dimensions[1] / dg.dimensions[1],
        ) * f;
    console.log("scale: ", scale);

    const svgElement = d3.select("#svg");
    const initialTransform = d3.zoomIdentity
        .scale(scale)
        .translate(
            (dg.dimensions[0] / f - dg.svg_dimensions[0]) / 2.0,
            (dg.dimensions[1] / f - dg.svg_dimensions[1]) / 2.0,
        );

    const svgNode = svgElement.node();
    if (!(svgNode instanceof Element)) {
        console.error("SVG node is not an instance of Element");
        return;
    }
    const currentTransform = d3.zoomTransform(svgNode);
    //     const x = dg.svg_dimensions[0] / VIEWPORT_SIZE_MULTIPLIER;
    //     const y = dg.svg_dimensions[1] / VIEWPORT_SIZE_MULTIPLIER;
    //     const invertedPoint = currentTransform.invert([x, y]);
    const invertedPoint = currentTransform.invert([
        -(dg.dimensions[0] / f - dg.svg_dimensions[0]) / 2.0,
        -(dg.dimensions[1] / f - dg.svg_dimensions[1]) / 2.0,
    ]);

    const transform = dg.network.zoom.transform.bind(
        dg.network.zoom,
    ) as TransformFunction;
    svgElement
        .transition()
        .duration(750)
        .call(transform, initialTransform, invertedPoint);
};

/**
 * Handles zoom events on the network visualization.
 * Updates the root layer's transform to reflect the current zoom state and hides any visible tooltips.
 *
 * @param {d3.D3ZoomEvent<SVGSVGElement, unknown>} event - The zoom event object
 */
const onNetworkZoom = (event: d3.D3ZoomEvent<SVGSVGElement, unknown>): void => {
    if (dg.network.layers.root) {
        dg.network.layers.root.attr("transform", event.transform.toString());
    }
    hideAllTooltips();
};
