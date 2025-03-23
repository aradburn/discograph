/**
 * Network Node Configuration and Event Handling
 * 
 * This module manages the creation, updating, and interaction of nodes in a D3.js network visualization.
 * It handles both artist and label nodes with different visual representations and behaviors.
 */

import * as d3 from 'd3';
import { tip as d3tip } from "d3-v6-tip";

import { debounce } from '../init';
import { getNodeColorClass } from '../color';
import { dg } from '../dg';
import { onDragStart, onDragEnd, onDrag, RequestNetworkEvent } from './events';
import { hideTooltips } from './init';
import { getOuterRadius, getInnerRadius } from './tick';
import { SelectEntityEvent } from './events';

// Configuration Constants
/**
 * @const {number} NODE_DEBOUNCE_TIME - Delay in milliseconds for debouncing node events (250ms)
 * @const {number} NODE_OUT_TRANSITION_TIME - Duration for node exit transitions (500ms)
 * @const {number} NODE_UPDATE_TRANSITION_TIME - Duration for node update transitions (5000ms)
 * @const {string} NODE_ARTIST_PALETTE - Color palette identifier for artist nodes
 * @const {string} NODE_LABEL_PALETTE - Color palette identifier for label nodes
 */

const NODE_DEBOUNCE_TIME = 250;
const NODE_OUT_TRANSITION_TIME = 500;
const NODE_UPDATE_TRANSITION_TIME = 5000;
const NODE_ARTIST_PALETTE = "Palette3";
const NODE_LABEL_PALETTE = "Palette4";

/**
 * Generates tooltip HTML content for nodes
 * @param {Object} d - Node data
 * @returns {string} HTML content for tooltip
 */
const setNodeToolTip = (d) => {
    return `<span>${d.name}</span>`;
}

/* Initialize node tooltip */
// @ts-ignore
export const nodeToolTip = d3tip()
    .attr('class', 'd3-node-tooltip')
    .direction('s')
    .offset([-20, 0])
    .html(setNodeToolTip);

/**
 * Handles the enter phase for new nodes in the D3 update pattern
 * @param {d3.Selection<d3.EnterElement, any, SVGGElement, unknown>} nodeEnter - D3 selection of entering nodes
 */
export const onNodeEnter = (nodeEnter) => {
    const nodeEnterSelection = nodeEnter.append("g")
        .attr("id", d => d.key)
        .attr("class", d => {
            const entity_type = d.key.split('-')[0];
            const classes = [
                "node",
                entity_type,
                entity_type === "artist" ? NODE_ARTIST_PALETTE : NODE_LABEL_PALETTE,
            ];
            return classes.join(" ");
        })
        // @ts-ignore
        .call(d3.drag()
            .on("start", onDragStart)
            .on("drag", onDrag)
            .on("end", onDragEnd));
    onNodeEnterElementConstruction(nodeEnterSelection);
    onNodeEnterEventBindings(nodeEnterSelection);
}

/**
 * Constructs the visual elements for nodes
 * @param {d3.Selection<SVGGElement, any, SVGGElement, unknown>} nodeEnter - D3 selection of entering nodes
 * 
 * For Artist nodes:
 * - Adds shadow circle
 * - Adds outer circle with size based on node data
 * - Adds inner circle
 * 
 * For Label nodes:
 * - Adds rectangular shape
 * 
 * For all nodes:
 * - Adds "more" indicator (+) symbol if node has hidden connections
 */
const onNodeEnterElementConstruction = (nodeEnter) => {
    // ARTISTS
    const artistEnter = nodeEnter.select(function(d) {
        return d.type === 'artist' ? this : null;
    });
    artistEnter
        .append("circle")
        .attr("class", "shadow")
        .attr("cx", d => getOuterRadius(d) / 3 + 1)
        .attr("cy", d => getOuterRadius(d) / 3 + 1)
        .attr("r", d => Math.pow(getOuterRadius(d), 1.2) - 2);
    artistEnter
        // .select(d => 0 < d.size ? this : null)
        .append("circle")
        .attr("class", d => {
            const classes = [
                "outer",
                getNodeColorClass(d),
            ];
            return classes.join(" ");
        })
        .attr("r", getOuterRadius);
    artistEnter
        .append("circle")
        .attr("class", d => {
            const classes = [
                "inner",
                getNodeColorClass(d),
            ];
            return classes.join(" ");
        })
        .attr("r", getInnerRadius);

    // LABELS
    const labelEnter = nodeEnter.select(function(d) {
        return d.type === 'label' ? this : null;
    });
    labelEnter
        .append("rect")
        .attr("class", d => {
            const classes = [
                "inner",
                getNodeColorClass(d),
            ];
            return classes.join(" ");
        })
        .attr("height", d => 2 * getInnerRadius(d))
        .attr("width", d => 2 * getInnerRadius(d))
        .attr("x", d => -1 * getInnerRadius(d))
        .attr("y", d => -1 * getInnerRadius(d));

    // MORE Show a + symbol if there are extra links from this node that are missing / not shown
    nodeEnter.append("path")
        .attr("class", "more")
        .attr("d", d3.symbol(d3.symbolCross, 64))
        .style("opacity", d => d.missing > 0 ? 1 : 0);
}

/**
 * Binds mouse and touch events to nodes
 * @param {d3.Selection<SVGGElement>} nodeEnter - D3 selection of entering nodes
 * 
 * Events handled:
 * - mouseover: Highlight node
 * - mouseenter: Show tooltip
 * - mouseleave: Hide tooltip
 * - mousedown: Handle single/double click timing
 * - dblclick: Request network update
 * - touchstart: Handle touch interactions
 */
const onNodeEnterEventBindings = (nodeEnter) => {
    const debounceToolTip = debounce((self, d, status) => {
        if (status) {
            nodeToolTip.show(d, d3.select(self).node());
            // Hide after 5 seconds
            setTimeout(() => {
                hideTooltips();
            }, 5000);
        } else {
            nodeToolTip.hide();
        }
    }, NODE_DEBOUNCE_TIME);

    nodeEnter
        .on("mouseover", (event, d) => onNodeMouseOver(event, d))
        .on("mouseenter", function(event, d) {
            debounceToolTip(this, d, true);
        })
        .on("mouseleave", (event, d) => {
            nodeToolTip.hide();
            console.log("tip mouseleave: ", event);
            console.log("tip mouseleave: ", d);
        })
        .on("mousedown", (event, d) => onNodeMouseDown(event, d))
        .on("dblclick", (event, d) => onNodeMouseDoubleClick(event, d))
        .on("touchstart", (event, d) => onNodeTouchStart(event, d));
}

/**
 * Handles node removal from the visualization
 * @param {d3.Selection<SVGGElement>} nodeExit - D3 selection of exiting nodes
 */
export const onNodeExit = (nodeExit) => {
    nodeExit.remove();
}

/**
 * Updates existing nodes in the visualization
 * @param {d3.Selection<SVGGElement>} nodeUpdate - D3 selection of updating nodes
 * Updates class names and visibility of node components
 */
export const onNodeUpdate = (nodeUpdate) => {
    nodeUpdate.selectAll(".outer")
        .attr("class", d => {
            const classes = [
                "outer",
                getNodeColorClass(d),
            ];
            return classes.join(" ");
        });
    nodeUpdate.selectAll(".inner")
        .attr("class", d => {
            const classes = [
                "inner",
                getNodeColorClass(d),
            ];
            return classes.join(" ");
        });
    nodeUpdate.selectAll(".more")
        .style("opacity", d => d.missing > 0 ? 1 : 0);
}

/**
 * Mouse event handlers
 */

/**
 * Handles mouse over events on nodes
 * @param {Event} event - DOM event object
 * @param {Object} d - Node data
 * Raises the hovered node to the top of the visualization
 */
export const onNodeMouseOver = (event, d) => {
    const debounceHandler = debounce((self, d) => {
        //console.log("node: ", d);
    }, NODE_DEBOUNCE_TIME);
    
    debounceHandler(this, d);

    // @ts-ignore
    dg.network.layers.node?.selectAll(".node").filter(n => n.key === d.key).raise();
    // @ts-ignore
    dg.network.layers.text?.selectAll(".node").filter(n => n.key === d.key).raise();
}

/**
 * Handles mouse down events on nodes
 * @param {Event} event - DOM event object
 * @param {Object} d - Node data
 * Implements single/double click timing logic for node selection and network updates
 */
export const onNodeMouseDown = (event, d) => {
    const thisTime = d3.now();
    const lastTime = d.lastClickTime;
    d.lastClickTime = thisTime;
    
    if (!lastTime || (thisTime - lastTime) < 700) {
        window.dispatchEvent(new SelectEntityEvent(d.key, true));
    } else if ((thisTime - lastTime) < 700) {
        window.dispatchEvent(new RequestNetworkEvent(d.key, true));
    }
}

/**
 * Handles double click events on nodes
 * @param {Event} event - DOM event object
 * @param {Object} d - Node data
 * Triggers network update request and prevents event propagation
 */
export const onNodeMouseDoubleClick = (event, d) => {
    console.log("dg_network_onNodeMouseDoubleClick: ", event, d);
    hideTooltips();
    window.dispatchEvent(new RequestNetworkEvent(d.key, true));
    event.stopPropagation();
}

/**
 * Handles touch events on nodes
 * @param {Event} event - DOM event object
 * @param {Object} d - Node data
 * Implements touch timing logic similar to mouse events
 */
export const onNodeTouchStart = (event, d) => {
    const thisTime = Date.now();
    const lastTime = d.lastTouchTime;
    d.lastTouchTime = thisTime;
    
    if (!lastTime || (500 < (thisTime - lastTime))) {
        window.dispatchEvent(new SelectEntityEvent(d.key, true));
    } else if ((thisTime - lastTime) < 500) {
        window.dispatchEvent(new RequestNetworkEvent(d.key, true));
    }
    event.stopPropagation();
}



