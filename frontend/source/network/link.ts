/**
 * Network link visualization handlers
 * This module manages the visual representation of relationships between nodes,
 * including their creation, styling, tooltips, and event handling.
 */

import type { Selection, BaseType, EnterElement } from "d3";
import * as d3 from "d3";
import { tip as d3tip, type D3Tip } from "d3-v6-tip";
import { debounce } from "../init";
import { getLinkColorClass } from "../color";
import type { NetworkLink } from "./node";
import { NetworkNode } from "./node";

type LinkSelection = Selection<SVGGElement, NetworkLink, BaseType, unknown>;
type LinkEnterSelection = Selection<
  EnterElement,
  NetworkLink,
  BaseType,
  unknown
>;
type LinkUpdateSelection = Selection<
  SVGGElement,
  NetworkLink,
  BaseType,
  unknown
>;

/**
 * Constants for link behavior and styling
 */
const LINK_DEBOUNCE_TIME = 250; // Debounce time for link interactions in milliseconds
const LINK_OUT_TRANSITION_TIME = 500; // Duration of link exit transition in milliseconds
const LINK_PALETTE = "LinkGreenPalette"; // Default color palette for links

/**
 * Generates HTML content for link tooltips
 * @param {NetworkLink} d - Link data object containing source, target, and role information
 * @returns {string} HTML string for tooltip content
 */
const setLinkTooltip = (d: NetworkLink): string => {
  return [
    `<div>${d.source.name}</div>`,
    `<div>${d.role}</div>`,
    `<div>${d.target.name}</div>`,
  ].join("");
};

/**
 * Creates abbreviated text annotation for links
 * Takes the first letter of each word in the role
 * @param {NetworkLink} d - Link data object
 * @returns {string} Abbreviated role text
 */
const linkAnnotation = (d: NetworkLink): string => {
  return d.role
    .split(" ")
    .map((word) => word[0])
    .join("");
};

/**
 * Initialize D3 tooltip for links
 * Configures a tooltip that appears above the link with network relationship information
 */
// TODO: Using any here because d3-v6-tip's TypeScript types are not working correctly
// The library's type definitions exist but are causing linter errors
// This should be revisited when the library's types are fixed or when we switch to a different tooltip library
export const linkToolTip = (d3tip() as any)
  .attr("class", "d3-link-tooltip")
  .direction("n")
  .offset([20, 0])
  .html(setLinkTooltip) as D3Tip<SVGGElement, NetworkLink>;

/**
 * Handles the enter selection for new links in the network
 * Creates the basic structure for each link including its visual elements
 * @param {LinkEnterSelection} linkEnter - D3 selection of entering link elements
 */
export const onLinkEnter = (linkEnter: LinkEnterSelection): void => {
  const newLinkEnter = linkEnter
    .append("g")
    .attr("id", (d: NetworkLink) => `link-${d.key}`)
    .attr("class", (d: NetworkLink) => {
      const parts = d.key.split("-");
      const role = parts.slice(2, 2 + parts.length - 4).join("-");
      return ["link", role, LINK_PALETTE].join(" ");
    });
  onLinkEnterElementConstruction(newLinkEnter);
  onLinkEnterEventBindings(newLinkEnter);
};

/**
 * Constructs the visual elements for each link
 * Creates paths and text elements for link visualization
 * @param {LinkSelection} linkEnter - D3 selection of entering link elements
 */
const onLinkEnterElementConstruction = (linkEnter: LinkSelection): void => {
  linkEnter.append("path").attr("class", (d: NetworkLink) => {
    return [
      "inner",
      `distance-${Math.min(d.source.distance, d.target.distance)}`,
      getLinkColorClass(d),
    ].join(" ");
  });
  linkEnter.append("text").attr("class", "outer").text(linkAnnotation);
  linkEnter.append("text").attr("class", "inner").text(linkAnnotation);
};

/**
 * Binds mouse events to link elements
 * Handles mouseover/mouseout events and tooltip display
 * @param {LinkSelection} linkEnter - D3 selection of entering link elements
 */
const onLinkEnterEventBindings = (linkEnter: LinkSelection): void => {
  const handleTooltip = debounce(
    (element: SVGGElement, d: NetworkLink, status: boolean) => {
      if (status) {
        const textElement = element.querySelector("text") as SVGGElement;
        linkToolTip.show(d, textElement);
      } else {
        linkToolTip.hide();
      }
    },
    LINK_DEBOUNCE_TIME,
  );

  linkEnter.on("mouseover", function (event: MouseEvent, d: NetworkLink) {
    d3.select(this).classed("selected", true);
    handleTooltip(this, d, true);
  });

  linkEnter.on("mouseout", function (event: MouseEvent, d: NetworkLink) {
    d3.select(this)
      .classed("selected", false)
      .transition()
      .duration(LINK_OUT_TRANSITION_TIME);
    handleTooltip(this, d, false);
  });
};

/**
 * Handles the removal of links from the visualization
 * @param {LinkSelection} linkExit - D3 selection of exiting link elements
 */
export const onLinkExit = (linkExit: LinkSelection): void => {
  linkExit.remove();
};

/**
 * Handles updates to existing links in the visualization
 * Currently empty but available for future implementation
 * @param {LinkUpdateSelection} linkSelection - D3 selection of updating link elements
 */
export const onLinkUpdate = (linkSelection: LinkUpdateSelection): void => {
  // Available for future implementation
};
