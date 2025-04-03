/**
 * Network visualization initialization module
 * Sets up the SVG layers, zoom behavior, and force layout for the network visualization.
 * Creates a hierarchical structure of SVG groups for different visualization elements.
 */

import { setupForceLayout } from "./forceLayout";
import { dg } from "../dg";
import * as d3 from "d3";
import { VIEWPORT_SIZE_MULTIPLIER } from "../init";
import { hideAllTooltips } from "./tooltips";
import type { SimNode, SimLink } from "./data";

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
    dg.network.layers = {
        root: root,
        halo: root.append("g").attr("id", "haloLayer"),
        link: root.append("g").attr("id", "linkLayer"),
        node: root.append("g").attr("id", "nodeLayer"),
        text: root.append("g").attr("id", "textLayer"),
    };

    dg.network.selections = {
        halo:
            dg.network.layers.halo?.selectAll<SVGGElement, SimNode>(".node") ??
            null,
        hull:
            dg.network.layers.halo?.selectAll<SVGGElement, SimNode[]>(
                ".hull",
            ) ?? null,
        link:
            dg.network.layers.link?.selectAll<SVGGElement, SimLink>(".link") ??
            null,
        node:
            dg.network.layers.node?.selectAll<SVGGElement, SimNode>(".node") ??
            null,
        text:
            dg.network.layers.text?.selectAll<SVGGElement, SimNode>(".node") ??
            null,
    };

    dg.network.zoom = d3
        .zoom<SVGSVGElement, unknown>()
        .extent([
            [0, 0],
            [dg.svg_dimensions[0], dg.svg_dimensions[1]],
        ])
        .scaleExtent([1, 8])
        .on("zoom", onNetworkZoom);

    svgElement.call(dg.network.zoom);

    const initialTransform = d3.zoomIdentity
        .scale(VIEWPORT_SIZE_MULTIPLIER)
        .translate(
            -dg.svg_dimensions[0] / VIEWPORT_SIZE_MULTIPLIER,
            -dg.svg_dimensions[1] / VIEWPORT_SIZE_MULTIPLIER,
        );

    //     dg.network.zoom.transform = initialTransform;
    //     console.log("dg.network.zoom.transform: ", dg.network.zoom.transform);

    const transform = dg.network.zoom.transform.bind(
        dg.network.zoom,
    ) as TransformFunction;
    svgElement.transition().duration(0).call(transform, initialTransform);

    setupForceLayout();
};

/**
 * Resets the network visualization's transform to its default state.
 * Animates the transition over 750ms, centering the view based on the viewport size multiplier.
 * Uses the current zoom state to calculate the proper inversion for smooth animation.
 */
export const resetNetworkTransform = (): void => {
    const svgElement = d3.select("#svg");
    const initialTransform = d3.zoomIdentity
        .scale(VIEWPORT_SIZE_MULTIPLIER)
        .translate(
            -dg.svg_dimensions[0] / VIEWPORT_SIZE_MULTIPLIER,
            -dg.svg_dimensions[1] / VIEWPORT_SIZE_MULTIPLIER,
        );
    console.log("initialTransform: ", initialTransform);

    const svgNode = svgElement.node();
    if (!(svgNode instanceof Element)) {
        console.error("SVG node is not an instance of Element");
        return;
    }
    const currentTransform = d3.zoomTransform(svgNode);
    const x = dg.svg_dimensions[0] / VIEWPORT_SIZE_MULTIPLIER;
    const y = dg.svg_dimensions[1] / VIEWPORT_SIZE_MULTIPLIER;
    const invertedPoint = currentTransform.invert([x, y]);
    console.log("currentTransform: ", currentTransform);
    console.log("invertedPoint: ", invertedPoint);

    const transform = dg.network.zoom.transform.bind(
        dg.network.zoom,
    ) as TransformFunction;
    svgElement
        .transition()
        .duration(750)
        .call(transform, initialTransform, invertedPoint);
    //     console.log("transform: ", transform);
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
