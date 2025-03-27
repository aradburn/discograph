/**
 * Network Node Configuration and Event Handling
 *
 * This module manages the creation, updating, and interaction of nodes in a D3.js network visualization.
 * It handles both artist and label nodes with different visual representations and behaviors.
 */

import * as d3 from "d3";
import { tip as d3tip } from "d3-v6-tip";
import type { D3Tip } from "d3-v6-tip";
import type { Selection, BaseType, EnterElement } from "d3";
import { symbol, symbolCross, now } from "d3";

import { debounce } from "../init";
import { getNodeColorClass } from "../color";
import { dg } from "../dg";
import { onDragStart, onDragEnd, onDrag, RequestNetworkEvent } from "./events";
import { hideTooltips } from "./init";
import { getOuterRadius, getInnerRadius } from "./tick";
import { SelectEntityEvent } from "./events";

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

export interface BaseNode {
  key: string;
  name: string;
  size: number;
  missing?: number;
  hasMissing?: boolean;
  lastClickTime?: number;
  lastTouchTime?: number;
  x: number;
  y: number;
  distance: number;
  radius: number;
  links?: NetworkLink[];
  cluster?: number;
  fixed?: boolean;
  isIntermediate?: boolean;
  pages?: unknown;
}

export interface ArtistNode extends BaseNode {
  type: "artist";
}

export interface LabelNode extends BaseNode {
  type: "label";
}

export type NetworkNode = ArtistNode | LabelNode;

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

type NodeSelection = Selection<SVGGElement, NetworkNode, BaseType, unknown>;
type NodeEnterSelection = Selection<
  EnterElement,
  NetworkNode,
  BaseType,
  unknown
>;

/**
 * Generates tooltip HTML content for nodes
 * @param {NetworkNode} d - Node data
 * @returns {string} HTML content for tooltip
 */
const setNodeToolTip = (d: NetworkNode): string => {
  return `<span>${d.name}</span>`;
};

/**
 * Creates and configures a D3 tooltip
 * @returns {D3Tip<SVGGElement, NetworkNode>} Configured tooltip instance
 */
function createNodeTooltip(): D3Tip<SVGGElement, NetworkNode> {
  // TODO: Using any here because d3-v6-tip's TypeScript types are not working correctly
  // The library's type definitions exist but are causing linter errors
  // This should be revisited when the library's types are fixed or when we switch to a different tooltip library
  const tooltip = d3tip() as any;
  tooltip.attr("class", "d3-node-tooltip");
  tooltip.direction("s");
  tooltip.offset([-20, 0]);
  tooltip.html(setNodeToolTip);
  return tooltip as D3Tip<SVGGElement, NetworkNode>;
}

/* Initialize node tooltip */
export const nodeToolTip = createNodeTooltip();

/**
 * Handles the enter phase for new nodes in the D3 update pattern
 * @param {NodeEnterSelection} nodeEnter - D3 selection of entering nodes
 */
export const onNodeEnter = (nodeEnter: NodeEnterSelection): void => {
  const nodeEnterSelection = nodeEnter
    .append("g")
    .attr("id", (d) => d.key)
    .attr("class", (d) => {
      const entity_type = d.key.split("-")[0];
      const classes = [
        "node",
        entity_type,
        entity_type === "artist" ? NODE_ARTIST_PALETTE : NODE_LABEL_PALETTE,
      ];
      return classes.join(" ");
    })
    .call(
      d3
        .drag<SVGGElement, NetworkNode>()
        .on("start", onDragStart)
        .on("drag", onDrag)
        .on("end", onDragEnd),
    );
  onNodeEnterElementConstruction(nodeEnterSelection);
  onNodeEnterEventBindings(nodeEnterSelection);
};

/**
 * Constructs the visual elements for nodes
 * @param {NodeSelection} nodeEnter - D3 selection of entering nodes
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
const onNodeEnterElementConstruction = (nodeEnter: NodeSelection): void => {
  // ARTISTS
  const artistEnter = nodeEnter.select(function (d) {
    return d.type === "artist" ? this : null;
  });
  artistEnter
    .append("circle")
    .attr("class", "shadow")
    .attr("cx", (d) => getOuterRadius(d) / 3 + 1)
    .attr("cy", (d) => getOuterRadius(d) / 3 + 1)
    .attr("r", (d) => Math.pow(getOuterRadius(d), 1.2) - 2);
  artistEnter
    .append("circle")
    .attr("class", (d) => {
      const classes = ["outer", getNodeColorClass(d)];
      return classes.join(" ");
    })
    .attr("r", (d) => getOuterRadius(d));
  artistEnter
    .append("circle")
    .attr("class", (d) => {
      const classes = ["inner", getNodeColorClass(d)];
      return classes.join(" ");
    })
    .attr("r", (d) => getInnerRadius(d));

  // LABELS
  const labelEnter = nodeEnter.select(function (d) {
    return d.type === "label" ? this : null;
  });
  labelEnter
    .append("rect")
    .attr("class", (d) => {
      const classes = ["inner", getNodeColorClass(d)];
      return classes.join(" ");
    })
    .attr("height", (d) => 2 * getInnerRadius(d))
    .attr("width", (d) => 2 * getInnerRadius(d))
    .attr("x", (d) => -1 * getInnerRadius(d))
    .attr("y", (d) => -1 * getInnerRadius(d));

  // MORE Show a + symbol if there are extra links from this node that are missing / not shown
  nodeEnter
    .append("path")
    .attr("class", "more")
    .attr("d", symbol().type(symbolCross).size(64))
    .style("opacity", (d) => (d.missing > 0 ? 1 : 0));
};

/**
 * Binds mouse and touch events to nodes
 * @param {NodeSelection} nodeEnter - D3 selection of entering nodes
 *
 * Events handled:
 * - mouseover: Highlight node
 * - mouseenter: Show tooltip
 * - mouseleave: Hide tooltip
 * - mousedown: Handle single/double click timing
 * - dblclick: Request network update
 * - touchstart: Handle touch interactions
 */
const onNodeEnterEventBindings = (nodeEnter: NodeSelection): void => {
  const debounceToolTip = debounce(
    (self: SVGGElement, d: NetworkNode, status: boolean) => {
      if (status) {
        nodeToolTip.show(d, self);
        // Hide after 5 seconds
        setTimeout(() => {
          hideTooltips();
        }, 5000);
      } else {
        nodeToolTip.hide();
      }
    },
    NODE_DEBOUNCE_TIME,
  );

  nodeEnter
    .on("mouseover", (event: MouseEvent, d: NetworkNode) =>
      onNodeMouseOver(event, d),
    )
    .on(
      "mouseenter",
      function (this: SVGGElement, event: MouseEvent, d: NetworkNode) {
        debounceToolTip(this, d, true);
      },
    )
    .on("mouseleave", (event: MouseEvent, d: NetworkNode) => {
      nodeToolTip.hide();
      console.log("tip mouseleave: ", event);
      console.log("tip mouseleave: ", d);
    })
    .on("mousedown", (event: MouseEvent, d: NetworkNode) =>
      onNodeMouseDown(event, d),
    )
    .on("dblclick", (event: MouseEvent, d: NetworkNode) =>
      onNodeMouseDoubleClick(event, d),
    )
    .on("touchstart", (event: TouchEvent, d: NetworkNode) =>
      onNodeTouchStart(event, d),
    );
};

/**
 * Handles node removal from the visualization
 * @param {NodeSelection} nodeExit - D3 selection of exiting nodes
 */
export const onNodeExit = (nodeExit: NodeSelection): void => {
  nodeExit.remove();
};

/**
 * Updates existing nodes in the visualization
 * @param {NodeSelection} nodeUpdate - D3 selection of updating nodes
 */
export const onNodeUpdate = (nodeUpdate: NodeSelection): void => {
  nodeUpdate
    .selectAll<SVGGElement, NetworkNode>(".outer")
    .attr("class", (d) => {
      const classes = ["outer", getNodeColorClass(d)];
      return classes.join(" ");
    });
  nodeUpdate
    .selectAll<SVGGElement, NetworkNode>(".inner")
    .attr("class", (d) => {
      const classes = ["inner", getNodeColorClass(d)];
      return classes.join(" ");
    });
  nodeUpdate
    .selectAll<SVGGElement, NetworkNode>(".more")
    .style("opacity", (d) => (d.missing > 0 ? 1 : 0));
};

/**
 * Mouse event handlers
 */

/**
 * Handles mouse over events on nodes
 * @param {MouseEvent} event - DOM event object
 * @param {NetworkNode} d - Node data
 * Raises the hovered node to the top of the visualization
 */
export const onNodeMouseOver = (event: MouseEvent, d: NetworkNode): void => {
  const debounceHandler = debounce((self: unknown, d: NetworkNode) => {
    //console.log("node: ", d);
  }, NODE_DEBOUNCE_TIME);

  debounceHandler(this, d);

  dg.network.layers.node
    ?.selectAll<SVGGElement, NetworkNode>(".node")
    .filter((n) => n.key === d.key)
    .raise();
  dg.network.layers.text
    ?.selectAll<SVGGElement, NetworkNode>(".node")
    .filter((n) => n.key === d.key)
    .raise();
};

/**
 * Handles mouse down events on nodes
 * @param {MouseEvent} event - DOM event object
 * @param {NetworkNode} d - Node data
 * Implements single/double click timing logic for node selection and network updates
 */
export const onNodeMouseDown = (event: MouseEvent, d: NetworkNode): void => {
  const thisTime = now();
  const lastTime = d.lastClickTime;
  d.lastClickTime = thisTime;

  if (!lastTime || thisTime - lastTime > 700) {
    window.dispatchEvent(new SelectEntityEvent(d.key, true));
  } else {
    window.dispatchEvent(new RequestNetworkEvent(d.key, true));
  }
};

/**
 * Handles double click events on nodes
 * @param {MouseEvent} event - DOM event object
 * @param {NetworkNode} d - Node data
 * Triggers network update request and prevents event propagation
 */
export const onNodeMouseDoubleClick = (
  event: MouseEvent,
  d: NetworkNode,
): void => {
  console.log("dg_network_onNodeMouseDoubleClick: ", event, d);
  hideTooltips();
  window.dispatchEvent(new RequestNetworkEvent(d.key, true));
  event.stopPropagation();
};

/**
 * Handles touch events on nodes
 * @param {TouchEvent} event - DOM event object
 * @param {NetworkNode} d - Node data
 * Implements touch timing logic similar to mouse events
 */
export const onNodeTouchStart = (event: TouchEvent, d: NetworkNode): void => {
  const thisTime = Date.now();
  const lastTime = d.lastTouchTime;
  d.lastTouchTime = thisTime;

  if (!lastTime || 500 < thisTime - lastTime) {
    window.dispatchEvent(new SelectEntityEvent(d.key, true));
  } else if (thisTime - lastTime < 500) {
    window.dispatchEvent(new RequestNetworkEvent(d.key, true));
  }
  event.stopPropagation();
};
