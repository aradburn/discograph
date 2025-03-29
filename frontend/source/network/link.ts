/**
 * Network link visualization handlers
 * This module manages the visual representation of relationships between nodes,
 * including their creation, styling, tooltips, and event handling.
 */

import * as d3 from "d3";
import { debounce } from "../init";
import type { NetworkNode } from "./node";
import { getLinkColorClass } from "../color";
import { linkTooltip } from "./tooltips";

export interface NetworkLink {
    key: string;
    source: NetworkNode;
    target: NetworkNode;
    role: string;
    distance?: number;
    isSpline?: boolean;
    intermediate?: NetworkNode;
    pages?: unknown;
}

type LinkSelection = d3.Selection<
    SVGGElement,
    NetworkLink,
    d3.BaseType,
    unknown
>;
type LinkEnterSelection = d3.Selection<
    d3.EnterElement,
    NetworkLink,
    d3.BaseType,
    unknown
>;
type LinkUpdateSelection = d3.Selection<
    SVGGElement,
    NetworkLink,
    d3.BaseType,
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
 * @param {NetworkLink} d - Link data object
 * @returns {string} HTML string for tooltip content
 */
const linkAnnotation = (d: NetworkLink): string => {
    return [
        `<div>${d.source.name}</div>`,
        `<div>${d.role}</div>`,
        `<div>${d.target.name}</div>`,
    ].join("");
};

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
                const textElement = element.querySelector(
                    "text",
                ) as SVGGElement;
                linkTooltip.show(d, textElement);
            } else {
                linkTooltip.hide();
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
export const onLinkUpdate = (_linkSelection: LinkUpdateSelection): void => {
    // Available for future implementation
};
