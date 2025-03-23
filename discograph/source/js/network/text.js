import { getOuterRadius } from "./tick";
import { getNodeColorClass } from "../color";
import { dg } from "../dg";

/**
 * Vertical offset for node labels from their center point
 * @constant {number}
 */
export const LABEL_OFFSET_Y = 9;

/**
 * Generates the display text for a network node
 * @param {Object} d - The node data object
 * @param {string} d.name - The name of the node
 * @returns {string} Truncated name (with debug info if debug mode is enabled)
 */
export const getNodeText = (d) => {
    let name = d.name;
    if (name.length > 50) {
        name = `${name.slice(0, 50)}...`;
    }
    if (dg.debug) {
        // @ts-ignore
        name = `${name}${getNodeDebug(d)}`;
    }
    return name;
};

/**
 * Generates debug information text for a network node
 * @param {Object} d - The node data object
 * @param {number} d.distance - Distance metric for the node
 * @param {number} d.radius - Radius of the node
 * @param {Array} [d.links] - Array of node connections
 * @param {number} d.missing - Count of missing connections
 * @param {number} d.cluster - Cluster identifier
 * @returns {string} Formatted debug information string
 */
export const getNodeDebug = (d) => {
    const links = d.links?.length ?? 0;
    return ` dist: ${d.distance}` +
           ` radi: ${d.radius}` +
           ` link: ${links}` +
           ` miss: ${d.missing}` +
           ` clus: ${d.cluster}` +
           ` colr: ${getNodeColorClass(d)}`;
};

/**
 * Handles the enter selection for network node text elements
 * Creates the text group and adds both outer and inner text elements
 * @param {d3.Selection<d3.EnterElement, any, SVGGElement, unknown>} textEnter - D3 enter selection for text elements
 */
export const onTextEnter = (textEnter) => {
    const textGroup = textEnter.append("g")
        .attr("id", d => d.key)
        .attr("class", d => {
            const classes = [
                "node",
                d.key.split('-')[0]
            ];
            if (d.cluster !== undefined) {
                classes.push("cluster");
            }
            return classes.join(" ");
        });

    textGroup.append("text")
        .attr("class", "outer")
        .attr("dy", d => getOuterRadius(d) + LABEL_OFFSET_Y)
        .attr("width", d => getOuterRadius(d) * 3)
        .text(getNodeText);

    textGroup.append("text")
        .attr("class", "inner")
        .attr("dy", d => getOuterRadius(d) + LABEL_OFFSET_Y)
        .attr("width", d => getOuterRadius(d) * 3)
        .text(getNodeText);
};

/**
 * Handles the exit selection for network node text elements
 * Removes text elements that are no longer needed
 * @param {d3.Selection} textExit - D3 exit selection for text elements
 */
export const onTextExit = (textExit) => {
    textExit.remove();
};

/**
 * Handles the update selection for network node text elements
 * Updates the text content of both outer and inner text elements
 * @param {d3.Selection} textUpdate - D3 update selection for text elements
 */
export const onTextUpdate = (textUpdate) => {
    textUpdate.select('.outer').text(getNodeText);
    textUpdate.select('.inner').text(getNodeText);
};

