/**
 * Initializes the network visualization by setting up the SVG layers, zoom behavior, and force layout.
 * Creates a hierarchical structure of SVG groups for different visualization elements.
 * 
 * The visualization consists of multiple layers:
 * - halo: For node highlighting effects
 * - link: For connections between nodes
 * - node: For the actual nodes
 * - text: For node labels
 */

import { setupForceLayout } from './forceLayout';
import { dg } from '../dg';
import * as d3 from 'd3';
import { VIEWPORT_SIZE_MULTIPLIER } from '../init';
import { nodeToolTip } from './node';
import { linkToolTip } from './link';

export const initNetwork = () => {
    const svgElement = d3.select("#svg");
    const root = svgElement.append("g").attr("id", "networkLayer");
    dg.network.layers = {
        root: root,
        halo: root.append("g").attr("id", "haloLayer"),
        link: root.append("g").attr("id", "linkLayer"),
        node: root.append("g").attr("id", "nodeLayer"),
        text: root.append("g").attr("id", "textLayer")
    };

    dg.network.selections = {
        halo: dg.network.layers.halo?.selectAll(".node") ?? null,
        hull: dg.network.layers.halo?.selectAll(".hull") ?? null,
        link: dg.network.layers.link?.selectAll(".link") ?? null,
        node: dg.network.layers.node?.selectAll(".node") ?? null,
        text: dg.network.layers.text?.selectAll(".node") ?? null
    };

    // @ts-ignore
    dg.network.zoom = d3.zoom()
        .extent([[0, 0], [dg.svg_dimensions[0], dg.svg_dimensions[1]]])
        .scaleExtent([1, 8])
        .on("zoom", onNetworkZoom);
    
    // @ts-ignore
    svgElement.call(dg.network.zoom);

    const initialTransform = d3.zoomIdentity
        .scale(VIEWPORT_SIZE_MULTIPLIER)
        .translate(
            -dg.svg_dimensions[0] / VIEWPORT_SIZE_MULTIPLIER, 
            -dg.svg_dimensions[1] / VIEWPORT_SIZE_MULTIPLIER
        );

    svgElement
        .transition()
        .duration(0)
        // @ts-ignore
        .call(dg.network.zoom.transform, initialTransform);

    setupForceLayout();
}

/**
 * Resets the network visualization's transform to its default state.
 * Animates the transition over 750ms, centering the view based on the viewport size multiplier.
 * Uses the current zoom state to calculate the proper inversion for smooth animation.
 */
export const resetNetworkTransform = () => {
    const svgElement = d3.select("#svg");
    const initialTransform = d3.zoomIdentity
        .scale(VIEWPORT_SIZE_MULTIPLIER)
        .translate(
            -dg.svg_dimensions[0] / VIEWPORT_SIZE_MULTIPLIER,
            -dg.svg_dimensions[1] / VIEWPORT_SIZE_MULTIPLIER
        );

    const svgNode = svgElement.node();
    if (!(svgNode instanceof Element)) return;
    const currentTransform = d3.zoomTransform(svgNode);
    const x = dg.svg_dimensions[0] / VIEWPORT_SIZE_MULTIPLIER;
    const y = dg.svg_dimensions[1] / VIEWPORT_SIZE_MULTIPLIER;
    const invertedPoint = currentTransform.invert([x, y]);
    svgElement
        .transition()
        .duration(750)
        // @ts-ignore
        .call(dg.network.zoom.transform, initialTransform, invertedPoint);
}

/**
 * Handles zoom events on the network visualization.
 * Updates the root layer's transform to reflect the current zoom state and hides any visible tooltips.
 * 
 * @param {d3.D3ZoomEvent} event - The zoom event object
 */
const onNetworkZoom = (event) => {
    if (dg.network.layers.root) {
        dg.network.layers.root.attr("transform", event.transform.toString());
    }
    hideTooltips();
}

/**
 * Utility function to hide both node and link tooltips.
 * Called during zoom operations to prevent tooltips from appearing in incorrect positions.
 */
export const hideTooltips = () => {
    nodeToolTip.hide();
    linkToolTip.hide();
}