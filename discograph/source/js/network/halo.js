/**
 * Network visualization halo effect handlers
 * This module provides functions for managing halo effects around network nodes
 * @module network/halo
 */

import { getOuterRadius } from './tick.js';

/**
 * Handles the creation and styling of halos when nodes are entered/activated
 * @param {d3.Selection} haloEnter - D3 selection of elements entering the visualization
 * @returns {void}
 * 
 * The function:
 * 1. Creates a group (g) element for each halo
 * 2. Assigns an ID based on the node's key
 * 3. Applies CSS classes based on the node type (first part of the key)
 * 4. Adds a circular halo effect around the node
 */
export const onHaloEnter = (haloEnter) => {
    const haloGroup = haloEnter.append("g")
        .attr("id", d => d.key)
        .attr("class", d => {
            const classes = [
                "node",
                d.key.split('-')[0]
            ];
            return classes.join(" ");
        });

    haloGroup.append("circle")
        .attr("class", "halo")
        .attr("r", d => getOuterRadius(d) + 40);
};

/**
 * Handles the removal of halos when nodes are exited/deactivated
 * @param {d3.Selection} haloExit - D3 selection of elements being removed from the visualization
 * @returns {void}
 */
export const onHaloExit = (haloExit) => {
    haloExit.remove();
};

