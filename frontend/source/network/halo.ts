/**
 * Network visualization halo effect handlers
 * This module provides functions for managing halo effects around network nodes
 * @module network/halo
 */

import type { NetworkNode } from "./node";
import { getOuterRadius } from "./tick";
import type * as d3 from "d3";

type HaloSelection = d3.Selection<
    d3.EnterElement | d3.BaseType,
    NetworkNode,
    d3.BaseType,
    unknown
>;

/**
 * Handles the creation and styling of halos when nodes are entered/activated
 * @param {HaloSelection} haloEnter - D3 selection of elements entering the visualization
 * @returns {void}
 *
 * The function:
 * 1. Creates a group (g) element for each halo
 * 2. Assigns an ID based on the node's key
 * 3. Applies CSS classes based on the node type (first part of the key)
 * 4. Adds a circular halo effect around the node
 */
export const onHaloEnter = (haloEnter: HaloSelection): void => {
    const haloGroup = haloEnter
        .append("g")
        .attr("id", (d: NetworkNode) => d.key)
        .attr("class", (d: NetworkNode) => {
            const classes = ["node", d.key.split("-")[0]];
            return classes.join(" ");
        });

    haloGroup
        .append("circle")
        .attr("class", "halo")
        .attr("r", (d: NetworkNode) => getOuterRadius(d) + 40);
};

/**
 * Handles the removal of halos when nodes are exited/deactivated
 * @param {HaloSelection} haloExit - D3 selection of elements being removed from the visualization
 * @returns {void}
 */
export const onHaloExit = (haloExit: HaloSelection): void => {
    haloExit.remove();
};
