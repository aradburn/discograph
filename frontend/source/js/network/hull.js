/**
 * Handles the enter selection for network hulls (convex hulls around groups of nodes).
 * Creates a new group element for each entering hull and appends a path element to it.
 * 
 * @param {d3.Selection<d3.EnterElement, any, SVGGElement, unknown>} hullEnter - The D3 enter selection for hulls
 * @returns {void}
 */
export const onHullEnter = (hullEnter) => {
    const hullGroup = hullEnter
        .append("g")
        .attr("class", (d) => "hull");
//        .attr("class", function(d) { return "hull hull-" + d.key });
    hullGroup.append("path");
};

/**
 * Handles the exit selection for network hulls.
 * Removes hull elements that are no longer needed from the DOM.
 * 
 * @param {d3.Selection<SVGGElement, any, SVGGElement, unknown>} hullExit - The D3 exit selection for hulls
 * @returns {void}
 */
export const onHullExit = (hullExit) => {
    hullExit.remove();
};

