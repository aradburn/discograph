import { getLinkColorClass } from '../color';
import * as d3 from 'd3';
import { tip as d3tip } from "d3-v6-tip";
import { debounce } from '../init';

/**
 * Constants for link behavior and styling
 */
const LINK_DEBOUNCE_TIME = 250;           // Debounce time for link interactions in milliseconds
const LINK_OUT_TRANSITION_TIME = 500;      // Duration of link exit transition in milliseconds
const LINK_PALETTE = "LinkGreenPalette";   // Default color palette for links

/**
 * Generates HTML content for link tooltips
 * @param {Object} d - Link data object containing source, target, and role information
 * @returns {string} HTML string for tooltip content
 */
const setLinkTooltip = (d) => {
    return [
        `<div>${d.source.name}</div>`,
        `<div>${d.role}</div>`,
        `<div>${d.target.name}</div>`
    ].join('');
}

/**
 * Creates abbreviated text annotation for links
 * Takes the first letter of each word in the role
 * @param {Object} d - Link data object
 * @returns {string} Abbreviated role text
 */
const linkAnnotation = (d) => {
    return d.role.split(' ').map(word => word[0]).join('');
}

/**
 * Initialize D3 tooltip for links
 * Configures a tooltip that appears above the link with network relationship information
 */
// @ts-ignore
export const linkToolTip = d3tip()
    .attr('class', 'd3-link-tooltip')
    .direction('n')
    .offset([20, 0])
    .html(setLinkTooltip);

/**
 * Handles the enter selection for new links in the network
 * Creates the basic structure for each link including its visual elements
 * @param {d3.Selection<d3.EnterElement, any, SVGGElement, unknown>} linkEnter - D3 selection of entering link elements
 */
export const onLinkEnter = (linkEnter) => {
    const newLinkEnter = linkEnter.append("g")
        .attr("id", d => `link-${d.key}`)
        .attr("class", d => {
            const parts = d.key.split('-');
            const role = parts.slice(2, 2 + parts.length - 4).join('-');
            return ["link", role, LINK_PALETTE].join(" ");
        });
    onLinkEnterElementConstruction(newLinkEnter);
    onLinkEnterEventBindings(newLinkEnter);
}

/**
 * Constructs the visual elements for each link
 * Creates paths and text elements for link visualization
 * @param {d3.Selection<SVGGElement, any, SVGGElement, unknown>} linkEnter - D3 selection of entering link elements
 */
const onLinkEnterElementConstruction = (linkEnter) => {
    linkEnter
        .append("path")
        .attr("class", d => {
            return [
                "inner",
                `distance-${Math.min(d.source.distance, d.target.distance)}`,
                getLinkColorClass(d)
            ].join(" ");
        });
    linkEnter
        .append("text")
        .attr('class', 'outer')
        .text(linkAnnotation);
    linkEnter
        .append("text")
        .attr('class', 'inner')
        .text(linkAnnotation);
}

/**
 * Binds mouse events to link elements
 * Handles mouseover/mouseout events and tooltip display
 * @param {d3.Selection} linkEnter - D3 selection of entering link elements
 */
const onLinkEnterEventBindings = (linkEnter) => {
    const handleTooltip = debounce((element, d, status) => {
        if (status) {
            linkToolTip.show(d, element.querySelector('text'));
        } else {
            linkToolTip.hide(d);
        }
    }, LINK_DEBOUNCE_TIME);

    linkEnter.on("mouseover", function(event, d) {
        d3.select(this)
            .classed("selected", true);
        handleTooltip(this, d, true);
    });

    linkEnter.on("mouseout", function(event, d) {
        d3.select(this)
            .classed("selected", false)
            .transition()
            .duration(LINK_OUT_TRANSITION_TIME);
        handleTooltip(this, d, false);
    });
}

/**
 * Handles the removal of links from the visualization
 * @param {d3.Selection<SVGGElement>} linkExit - D3 selection of exiting link elements
 */
export const onLinkExit = (linkExit) => {
    linkExit.remove();
}

/**
 * Handles updates to existing links in the visualization
 * Currently empty but available for future implementation
 * @param {d3.Selection<SVGGElement>} linkSelection - D3 selection of updating link elements
 */
export const onLinkUpdate = (linkSelection) => {
}
