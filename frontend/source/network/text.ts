/**
 * Network node text visualization handlers
 * This module manages the text labels for network nodes, including their creation,
 * updating, and removal, as well as debug information display.
 */

import type { Selection, BaseType, EnterElement } from "d3";
import { getOuterRadius } from "./tick";
import { getNodeColorClass } from "../color";
import { dg } from "../dg";
import type { NetworkNode } from "./node";

type TextSelection = Selection<SVGGElement, NetworkNode, BaseType, unknown>;
type TextEnterSelection = Selection<
  EnterElement,
  NetworkNode,
  BaseType,
  unknown
>;
type TextUpdateSelection = Selection<
  SVGGElement,
  NetworkNode,
  BaseType,
  unknown
>;

/**
 * Vertical offset for node labels from their center point
 * @constant {number}
 */
export const LABEL_OFFSET_Y = 9;

/**
 * Generates the display text for a network node
 * @param {NetworkNode} d - The node data object
 * @returns {string} Truncated name (with debug info if debug mode is enabled)
 */
export const getNodeText = (d: NetworkNode): string => {
  let name = d.name;
  if (name.length > 50) {
    name = `${name.slice(0, 50)}...`;
  }
  if (dg.debug) {
    name = `${name}${getNodeDebug(d)}`;
  }
  return name;
};

/**
 * Generates debug information text for a network node
 * @param {NetworkNode} d - The node data object
 * @returns {string} Formatted debug information string
 */
export const getNodeDebug = (d: NetworkNode): string => {
  const links = d.links?.length ?? 0;
  return (
    ` dist: ${d.distance}` +
    ` radi: ${d.radius}` +
    ` link: ${links}` +
    ` miss: ${d.missing}` +
    ` clus: ${d.cluster ?? "undefined"}` +
    ` colr: ${getNodeColorClass(d)}`
  );
};

/**
 * Handles the enter selection for network node text elements
 * Creates the text group and adds both outer and inner text elements
 * @param {TextEnterSelection} textEnter - D3 enter selection for text elements
 */
export const onTextEnter = (textEnter: TextEnterSelection): void => {
  const textGroup = textEnter
    .append("g")
    .attr("id", (d: NetworkNode) => d.key)
    .attr("class", (d: NetworkNode) => {
      const classes = ["node", d.key.split("-")[0]];
      if (d.cluster !== undefined) {
        classes.push("cluster");
      }
      return classes.join(" ");
    });

  textGroup
    .append("text")
    .attr("class", "outer")
    .attr("dy", (d: NetworkNode) => getOuterRadius(d) + LABEL_OFFSET_Y)
    .attr("width", (d: NetworkNode) => getOuterRadius(d) * 3)
    .text(getNodeText);

  textGroup
    .append("text")
    .attr("class", "inner")
    .attr("dy", (d: NetworkNode) => getOuterRadius(d) + LABEL_OFFSET_Y)
    .attr("width", (d: NetworkNode) => getOuterRadius(d) * 3)
    .text(getNodeText);
};

/**
 * Handles the exit selection for network node text elements
 * Removes text elements that are no longer needed
 * @param {TextSelection} textExit - D3 exit selection for text elements
 */
export const onTextExit = (textExit: TextSelection): void => {
  textExit.remove();
};

/**
 * Handles the update selection for network node text elements
 * Updates the text content of both outer and inner text elements
 * @param {TextUpdateSelection} textUpdate - D3 update selection for text elements
 */
export const onTextUpdate = (textUpdate: TextUpdateSelection): void => {
  textUpdate.select(".outer").text(getNodeText);
  textUpdate.select(".inner").text(getNodeText);
};
