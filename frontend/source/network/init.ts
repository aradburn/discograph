/**
 * Network visualization initialization module
 * Sets up the SVG layers, zoom behavior, and force layout for the network visualization.
 * Creates a hierarchical structure of SVG groups for different visualization elements.
 */

import { setupForceLayout } from "./forceLayout";
import { dg } from "../dg";
import * as d3 from "d3";
import type { Selection, BaseType, D3ZoomEvent } from "d3";
import { ZoomBehavior } from "d3";
import { VIEWPORT_SIZE_MULTIPLIER } from "../init";
import { nodeToolTip } from "./node";
import { linkToolTip } from "./link";
import type { NetworkNode, NetworkLink } from "./node";

type NetworkSVGElement = SVGGElement | SVGSVGElement;
type NetworkBaseSelection = Selection<
  NetworkSVGElement,
  unknown,
  BaseType,
  unknown
>;
type NetworkNodeSelection = Selection<
  SVGGElement,
  NetworkNode,
  HTMLElement,
  unknown
>;
type NetworkLinkSelection = Selection<
  SVGGElement,
  NetworkLink,
  HTMLElement,
  unknown
>;
type NetworkLayerSelection = Selection<
  SVGGElement,
  NetworkNode | NetworkLink,
  HTMLElement,
  unknown
>;
type NetworkNodeArraySelection = Selection<
  SVGGElement,
  NetworkNode[],
  SVGGElement,
  unknown
>;
type NetworkLinkArraySelection = Selection<
  SVGGElement,
  NetworkLink[],
  SVGGElement,
  unknown
>;

/**
 * Initializes the network visualization by setting up the SVG layers, zoom behavior, and force layout.
 * Creates a hierarchical structure of SVG groups for different visualization elements:
 * - halo: For node highlighting effects
 * - link: For connections between nodes
 * - node: For the actual nodes
 * - text: For node labels
 */
export const initNetwork = (): void => {
  const svgElement: NetworkBaseSelection = d3.select("#svg");
  const root: NetworkBaseSelection = svgElement
    .append("g")
    .attr("id", "networkLayer");
  dg.network.layers = {
    root: root as NetworkLayerSelection,
    halo: root.append("g").attr("id", "haloLayer") as NetworkLayerSelection,
    link: root.append("g").attr("id", "linkLayer") as NetworkLayerSelection,
    node: root.append("g").attr("id", "nodeLayer") as NetworkLayerSelection,
    text: root.append("g").attr("id", "textLayer") as NetworkLayerSelection,
  };

  dg.network.selections = {
    halo:
      dg.network.layers.halo?.selectAll<SVGGElement, NetworkNode[]>(".node") ??
      null,
    hull:
      dg.network.layers.halo?.selectAll<SVGGElement, NetworkNode[]>(".hull") ??
      null,
    link:
      dg.network.layers.link?.selectAll<SVGGElement, NetworkLink[]>(".link") ??
      null,
    node:
      dg.network.layers.node?.selectAll<SVGGElement, NetworkNode[]>(".node") ??
      null,
    text:
      dg.network.layers.text?.selectAll<SVGGElement, NetworkNode[]>(".node") ??
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

  svgElement
    .transition()
    .duration(0)
    .call(dg.network.zoom.transform, initialTransform);

  setupForceLayout();
};

/**
 * Resets the network visualization's transform to its default state.
 * Animates the transition over 750ms, centering the view based on the viewport size multiplier.
 * Uses the current zoom state to calculate the proper inversion for smooth animation.
 */
export const resetNetworkTransform = (): void => {
  const svgElement: NetworkBaseSelection = d3.select("#svg");
  const initialTransform = d3.zoomIdentity
    .scale(VIEWPORT_SIZE_MULTIPLIER)
    .translate(
      -dg.svg_dimensions[0] / VIEWPORT_SIZE_MULTIPLIER,
      -dg.svg_dimensions[1] / VIEWPORT_SIZE_MULTIPLIER,
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
    .call(dg.network.zoom.transform, initialTransform, invertedPoint);
};

/**
 * Handles zoom events on the network visualization.
 * Updates the root layer's transform to reflect the current zoom state and hides any visible tooltips.
 *
 * @param {D3ZoomEvent<SVGSVGElement, unknown>} event - The zoom event object
 */
const onNetworkZoom = (event: D3ZoomEvent<SVGSVGElement, unknown>): void => {
  if (dg.network.layers.root) {
    dg.network.layers.root.attr("transform", event.transform.toString());
  }
  hideTooltips();
};

/**
 * Utility function to hide both node and link tooltips.
 * Called during zoom operations to prevent tooltips from appearing in incorrect positions.
 */
export const hideTooltips = (): void => {
  if (nodeToolTip && typeof nodeToolTip.hide === "function") {
    nodeToolTip.hide();
  }
  if (linkToolTip && typeof linkToolTip.hide === "function") {
    linkToolTip.hide();
  }
};
