/**
 * forceLayout.ts
 * This file implements a force-directed graph layout system using D3.js for the Discograph application.
 * It handles node positioning, link creation, and graph simulation with various forces applied.
 */

import * as d3 from "d3";
import { onHullEnter, onHullExit } from "./hull";
import { onHaloEnter, onHaloExit } from "./halo";
import { onNodeEnter, onNodeExit, onNodeUpdate } from "./node";
import type { NetworkNode, NetworkLink } from "./node";
import { onTextEnter, onTextExit, onTextUpdate } from "./text";
import { onLinkEnter, onLinkExit, onLinkUpdate } from "./link";
import { onTick, getOuterRadius } from "./tick";
import { onNetworkEnd } from "./events";
import type { DraggableNodeBase } from "./events";
import { dg } from "../dg";

/**
 * Configuration Constants
 */
// Force configuration for nodes
const NODE_STRENGTH = -1000; // Repulsion strength between nodes
const DISTANCE_MAX = 2000; // Maximum distance for force calculations
const COLLIDE_ITERATIONS = 2; // Number of collision detection iterations
const COLLIDE_BUFFER = 12; // Extra space around nodes for collision detection

// Simulation parameters
const THETA = 0.9; // Barnes-Hut approximation criterion
export const ALPHA = 1.0; // Initial simulation temperature
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const ALPHA_DECAY = 0.03; // Rate at which simulation cools down
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const VELOCITY_DECAY = 0.24; // Friction coefficient for node movement

// Link configuration
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const LINK_STRENGTH = 0.8; // Strength of links between nodes
const LINK_DISTANCE_ALIAS = 20; // Distance for alias relationships
const LINK_DISTANCE_RELEASED_ON = 200; // Distance for "Released On" relationships
const LINK_DISTANCE = 120; // Default link distance
const LINK_ITERATIONS = 3; // Number of iterations for link force calculation

// Graph size limits
const MAX_NODES_BEFORE_PRUNING = 600; // Maximum nodes before pruning is triggered
const MAX_LINKS_BEFORE_PRUNING = 1800; // Maximum links before pruning is triggered

let nodeStrengthMultiplier = 1.0;
let linkStrengthMultiplier = 1.0;
let gravStrengthMultiplier = 1.0;

/**
 * Properties added to nodes by D3's force simulation
 */
interface SimulationProps extends DraggableNodeBase {
  vx?: number;
  vy?: number;
  index?: number;
  isIntermediate?: boolean;
  cluster?: number;
  fixed?: boolean;
  missing?: number;
  hasMissing?: boolean;
  links?: NetworkLink[];
  pages?: unknown;
}

/**
 * Node type with simulation properties
 */
export type SimNode = NetworkNode & SimulationProps;

/**
 * Link type for force simulation
 */
interface SimLink
  extends Omit<NetworkLink, "source" | "target" | "intermediate"> {
  source: SimNode;
  target: SimNode;
  role: string;
  isSpline?: boolean;
  distance?: number;
  intermediate?: SimNode;
  pages?: unknown;
}

/**
 * D3 selections for different network elements
 */
interface NetworkSelections {
  halo: d3.Selection<SVGGElement, SimNode, HTMLElement, unknown> | null;
  node: d3.Selection<SVGGElement, SimNode, HTMLElement, unknown> | null;
  text: d3.Selection<SVGGElement, SimNode, HTMLElement, unknown> | null;
  link: d3.Selection<SVGGElement, SimLink, HTMLElement, unknown> | null;
  hull: d3.Selection<SVGGElement, SimNode[], HTMLElement, unknown> | null;
}

/**
 * Network state and configuration
 */
export interface Network {
  forceLayout: d3.Simulation<SimNode, SimLink>;
  pageData: {
    nodes: SimNode[];
    links: SimLink[];
  };
  selections: NetworkSelections;
  layers: {
    halo?: d3.Selection<SVGGElement, SimNode[], HTMLElement, unknown>;
    node?: d3.Selection<SVGGElement, SimNode, HTMLElement, unknown>;
    text?: d3.Selection<SVGGElement, SimNode, HTMLElement, unknown>;
    link?: d3.Selection<SVGGElement, SimLink, HTMLElement, unknown>;
  };
  data: {
    nodeMap: Map<string, SimNode>;
    linkMap: Map<string, SimLink>;
    maxDistance?: number;
  };
  newNodeCoords: [number, number];
}

declare global {
  interface DiscographCore {
    network: Network;
    svg_dimensions: [number, number];
  }
}

/**
 * Determines the distance between linked nodes based on their relationship type
 * @param {SimLink} d - The link object
 * @returns {number} The desired distance between nodes
 */
function linkDistance(d: SimLink): number {
  if (d.isSpline) {
    if (d.role === "Released On") {
      return (LINK_DISTANCE_RELEASED_ON * linkStrengthMultiplier) / 2;
    }
    return d.distance && d.distance < 1
      ? (LINK_DISTANCE * linkStrengthMultiplier) / 2
      : (LINK_DISTANCE * linkStrengthMultiplier) / 10;
  } else if (d.role === "Alias") {
    return LINK_DISTANCE_ALIAS * linkStrengthMultiplier;
  } else if (d.role === "Released On") {
    return LINK_DISTANCE_RELEASED_ON * linkStrengthMultiplier;
  } else {
    return LINK_DISTANCE * linkStrengthMultiplier;
  }
}

/**
 * Calculates the repulsion strength for each node
 * @param {SimNode} d - The node object
 * @returns {number} The repulsion strength
 */
function nodeStrength(d: SimNode): number {
  if (d.distance) {
    const dist = 1; // 4 - clamp(d.distance, 0, 3);
    return dist * NODE_STRENGTH * nodeStrengthMultiplier;
  } else if (d.isIntermediate) {
    return (NODE_STRENGTH * nodeStrengthMultiplier) / 10;
  } else if (d.cluster) {
    return 100 * nodeStrengthMultiplier;
  } else {
    return NODE_STRENGTH * nodeStrengthMultiplier;
  }
}

/**
 * Calculates the gravity strength for each node based on its position and distance
 * @param {SimNode} d - The node object
 * @returns {number} The gravity strength
 */
function gravityStrength(d: SimNode): number {
  if (typeof d.x !== "number" || typeof d.y !== "number") {
    return 0;
  }
  const maxDimension = Math.max(dg.svg_dimensions[0], dg.svg_dimensions[1]);
  const scaling = gravStrengthMultiplier / 10.0;
  const radialDistance =
    (maxDimension -
      Math.max(
        d.x - dg.svg_dimensions[0] / 2,
        d.y - dg.svg_dimensions[1] / 2,
      )) /
    maxDimension;
  return radialDistance * scaling;
}

/**
 * Sets up the initial force simulation with basic forces
 */
export const setupForceLayout = (): void => {
  console.log("setupForceLayout");

  dg.network.forceLayout = d3
    .forceSimulation<SimNode>()
    .nodes(dg.network.pageData.nodes)
    .force(
      "collide",
      d3
        .forceCollide<SimNode>()
        .radius((d) => (d.radius ?? 0) + COLLIDE_BUFFER)
        .iterations(COLLIDE_ITERATIONS),
    )
    .force(
      "charge",
      d3
        .forceManyBody<SimNode>()
        .strength(nodeStrength)
        .distanceMax(DISTANCE_MAX)
        .theta(THETA),
    )
    .force("bbox", bboxForce)
    .on("tick", function (this: d3.Simulation<SimNode, SimLink>) {
      onTick(this);
    })
    .on("end", function (this: d3.Simulation<SimNode, SimLink>) {
      onNetworkEnd(this);
    })
    .stop();

  const nodeSlider = document.getElementById("nodeRange") as HTMLInputElement;
  const linkSlider = document.getElementById("linkRange") as HTMLInputElement;
  const gravSlider = document.getElementById("gravRange") as HTMLInputElement;

  if (!nodeSlider || !linkSlider || !gravSlider) {
    console.error("Could not find one or more slider elements");
    return;
  }

  nodeSlider.oninput = function (this: HTMLInputElement) {
    nodeStrengthMultiplier = ((parseInt(this.value) - 50) * 2.0) / 10.0;
    if (dg.network.forceLayout) {
      dg.network.forceLayout.force(
        "charge",
        d3
          .forceManyBody<SimNode>()
          .strength(nodeStrength)
          .distanceMax(DISTANCE_MAX)
          .theta(THETA),
      );
      restartForceLayout(ALPHA / 10.0);
    }
  };

  linkSlider.oninput = function (this: HTMLInputElement) {
    linkStrengthMultiplier = ((parseInt(this.value) - 50) * 2.0) / 50.0;
    if (dg.network.forceLayout) {
      dg.network.forceLayout.force(
        "link",
        d3
          .forceLink<SimNode, SimLink>()
          .id((d) => d.key ?? "")
          .links(dg.network.pageData.links)
          .distance(linkDistance)
          .iterations(LINK_ITERATIONS),
      );
      restartForceLayout(ALPHA / 10.0);
    }
  };

  gravSlider.oninput = function (this: HTMLInputElement) {
    gravStrengthMultiplier = ((parseInt(this.value) - 50) * 2.0) / 10.0;
    if (dg.network.forceLayout) {
      dg.network.forceLayout
        .force(
          "x",
          d3
            .forceX<SimNode>(dg.svg_dimensions[0] / 2)
            .strength(gravityStrength),
        )
        .force(
          "y",
          d3
            .forceY<SimNode>(dg.svg_dimensions[1] / 2)
            .strength(gravityStrength),
        );
      restartForceLayout(ALPHA / 10.0);
    }
  };
};

/**
 * Initializes and starts the force layout simulation
 * Updates node and link selections and applies forces
 */
export const startForceLayout = (): void => {
  console.log("Start D3 layout");
  const keyFunc = (d: SimNode | SimLink): string => ("key" in d ? d.key : "");
  const nodeData = dg.network.pageData.nodes.filter((d) => !d.isIntermediate);
  console.log("nodeData: ", nodeData);
  const linkData = dg.network.pageData.links.filter((d) => !d.isSpline);
  console.log("linkData: ", linkData);

  dg.network.selections.halo =
    dg.network.layers.halo?.selectAll<SVGGElement, SimNode>(".node") ?? null;
  dg.network.selections.halo =
    dg.network.selections.halo?.data(nodeData, keyFunc) ?? null;

  dg.network.selections.node =
    dg.network.layers.node?.selectAll<SVGGElement, SimNode>(".node") ?? null;
  dg.network.selections.node =
    dg.network.selections.node?.data(nodeData, keyFunc) ?? null;

  dg.network.selections.text =
    dg.network.layers.text?.selectAll<SVGGElement, SimNode>(".node") ?? null;
  dg.network.selections.text =
    dg.network.selections.text?.data(nodeData, keyFunc) ?? null;

  dg.network.selections.link =
    dg.network.layers.link?.selectAll<SVGGElement, SimLink>(".link") ?? null;
  dg.network.selections.link =
    dg.network.selections.link?.data(linkData, keyFunc) ?? null;

  const clusterNodes = dg.network.pageData.nodes.filter(
    (d) => d.cluster !== undefined,
  );
  const hullGroups = Array.from(
    d3.group(clusterNodes, (d) => d.cluster).values(),
  );
  const hullData = hullGroups.filter((d) => d.length > 1);
  dg.network.selections.hull =
    dg.network.layers.halo?.selectAll<SVGGElement, SimNode[]>(".hull") ?? null;
  dg.network.selections.hull =
    dg.network.selections.hull?.data(hullData) ?? null;

  if (dg.network.selections.halo) {
    onHaloEnter(dg.network.selections.halo.enter());
    onHaloExit(dg.network.selections.halo.exit());
  }
  if (dg.network.selections.hull) {
    onHullEnter(dg.network.selections.hull.enter());
    onHullExit(dg.network.selections.hull.exit());
  }
  if (dg.network.selections.node) {
    onNodeEnter(dg.network.selections.node.enter());
    onNodeExit(dg.network.selections.node.exit());
    onNodeUpdate(dg.network.selections.node);
  }
  if (dg.network.selections.text) {
    onTextEnter(dg.network.selections.text.enter());
    onTextExit(dg.network.selections.text.exit());
    onTextUpdate(dg.network.selections.text);
  }
  if (dg.network.selections.link) {
    onLinkEnter(dg.network.selections.link.enter());
    onLinkExit(dg.network.selections.link.exit());
    onLinkUpdate(dg.network.selections.link);
  }

  dg.network.pageData.nodes.forEach((n) => {
    n.fixed = false;
  });
};

/**
 * Restarts the force layout simulation with a new alpha value
 * @param {number} alpha - The new alpha value for the simulation
 */
export const restartForceLayout = (alpha: number): void => {
  if (dg.network.forceLayout) {
    dg.network.forceLayout.alpha(alpha).restart();
  }
};

/**
 * Stops the force layout simulation
 */
export const stopForceLayout = (): void => {
  if (dg.network.forceLayout) {
    dg.network.forceLayout.stop();
  }
};

/**
 * Processes the JSON data to create nodes and links for the force layout
 * @param {Object} json - The JSON data containing nodes and links
 */
export const processJson = (json: {
  nodes: SimNode[];
  links: SimLink[];
}): void => {
  const newNodeMap = new Map<string, SimNode>();
  const newLinkMap = new Map<string, SimLink>();

  // Setup node size
  json.nodes.forEach((node) => {
    node.radius = getOuterRadius(node);
    newNodeMap.set(node.key, node);
  });

  // Setup links, add intermediate node at center of link
  json.links.forEach((link) => {
    const source = link.source;
    const target = link.target;
    if (link.role !== "Alias") {
      const role = link.role?.toLowerCase().replace(/\s+/g, "-") ?? "";
      const intermediateNode: SimNode = {
        key: link.key,
        isIntermediate: true,
        pages: link.pages,
        size: 0,
        name: "",
        type: "artist", // Default type
        x: 0,
        y: 0,
        distance: 0,
        radius: 0,
        missing: 0,
      };

      const s2iSplineLink: SimLink = {
        isSpline: true,
        key: `${source.key}-${role}-[${target.key}]`,
        pages: link.pages,
        source: source,
        target: intermediateNode,
        role: role,
      };

      const i2tSplineLink: SimLink = {
        isSpline: true,
        key: `[${source.key}]-${role}-${target.key}`,
        pages: link.pages,
        source: intermediateNode,
        target: target,
        role: role,
      };

      link.intermediate = intermediateNode;
      newNodeMap.set(link.key, intermediateNode);
      newLinkMap.set(s2iSplineLink.key, s2iSplineLink);
      newLinkMap.set(i2tSplineLink.key, i2tSplineLink);
    }
    newLinkMap.set(link.key, link);
  });

  // Update current lists of nodes and links
  const nodeKeysToRemove: string[] = [];
  Array.from(dg.network.data.nodeMap.keys()).forEach((key) => {
    if (!newNodeMap.has(key)) {
      nodeKeysToRemove.push(key);
    }
  });
  nodeKeysToRemove.forEach((key) => {
    dg.network.data.nodeMap.delete(key);
  });

  const linkKeysToRemove: string[] = [];
  Array.from(dg.network.data.linkMap.keys()).forEach((key) => {
    if (!newLinkMap.has(key)) {
      linkKeysToRemove.push(key);
    }
  });
  linkKeysToRemove.forEach((key) => {
    dg.network.data.linkMap.delete(key);
  });

  newNodeMap.forEach((newNode, key) => {
    if (dg.network.data.nodeMap.has(key)) {
      const oldNode = dg.network.data.nodeMap.get(key);
      if (oldNode) {
        oldNode.cluster = newNode.cluster;
        oldNode.distance = newNode.distance;
        oldNode.links = newNode.links;
        oldNode.missing = newNode.missing;
        oldNode.x = dg.network.newNodeCoords[0];
        oldNode.y = dg.network.newNodeCoords[1];
      }
    } else {
      newNode.x = dg.network.newNodeCoords[0];
      newNode.y = dg.network.newNodeCoords[1];
      dg.network.data.nodeMap.set(key, newNode);
    }
  });

  newLinkMap.forEach((newLink, key) => {
    if (dg.network.data.linkMap.has(key)) {
      const oldLink = dg.network.data.linkMap.get(key);
      if (oldLink) {
        oldLink.pages = newLink.pages;
      }
    } else {
      const sourceNode = dg.network.data.nodeMap.get(newLink.source.key);
      const targetNode = dg.network.data.nodeMap.get(newLink.target.key);
      if (sourceNode && targetNode) {
        newLink.source = sourceNode;
        newLink.target = targetNode;
        if (newLink.intermediate) {
          const intermediateNode = dg.network.data.nodeMap.get(
            newLink.intermediate.key,
          );
          if (intermediateNode) {
            newLink.intermediate = intermediateNode;
          }
        }
        dg.network.data.linkMap.set(key, newLink);
      }
    }
  });

  // Get some useful stats
  const distances: number[] = [];
  const distance_counts = [0, 0, 0, 0, 0, 0];
  Array.from(dg.network.data.nodeMap.values()).forEach((node) => {
    if (node.distance !== undefined) {
      distances.push(node.distance);
      if (node.distance < distance_counts.length) {
        distance_counts[node.distance]++;
      }
    }
  });
  dg.network.data.maxDistance = Math.max(...distances);
  console.log("maxDistance: ", dg.network.data.maxDistance);
  console.log("distance_counts: ", distance_counts);
  console.log("initial node size: ", dg.network.data.nodeMap.size);
  console.log("initial link size: ", dg.network.data.linkMap.size);

  // Prune dist==3
  prune(3, 1);
  prune(3, 2);
  prune(3, 3);
  prune(3, 4);
  prune(3, 5);
  prune(3, 10);
  prune(3, 100);
  prune(3, 1000000);
  prune(2, 1);
  prune(2, 2);
  prune(2, 3);
  prune(2, 4);
  prune(2, 5);
  prune(2, 10);
  prune(2, 100);
  prune(2, 100000);
};

/**
 * Prunes the network to keep it within size limits
 * @param {number} maxDist - Maximum distance from center to keep
 * @param {number} minLinks - Minimum number of links to keep a node
 */
const prune = (maxDist: number, minLinks: number): void => {
  if (
    dg.network.data.nodeMap.size > MAX_NODES_BEFORE_PRUNING ||
    dg.network.data.linkMap.size > MAX_LINKS_BEFORE_PRUNING
  ) {
    const nodeKeysToPrune: string[] = [];
    Array.from(dg.network.data.nodeMap.values()).forEach((node) => {
      if (
        node.distance &&
        node.distance >= maxDist &&
        node.links &&
        node.links.length <= minLinks
      ) {
        nodeKeysToPrune.push(node.key);
      }
    });
    nodeKeysToPrune.forEach((key) => {
      dg.network.data.nodeMap.delete(key);
    });
    console.log("pruned nodes: ", nodeKeysToPrune.length);

    const linkKeysToPrune: string[] = [];
    const intermediateNodesToPrune: string[] = [];
    const intermediateLinksToPrune: string[] = [];

    Array.from(dg.network.data.linkMap.values()).forEach((link) => {
      if (
        (link.source && nodeKeysToPrune.includes(link.source.key)) ||
        (link.target && nodeKeysToPrune.includes(link.target.key))
      ) {
        linkKeysToPrune.push(link.key);
        link.source.hasMissing = true;
        link.target.hasMissing = true;
        link.source.missing = (link.source.missing ?? 0) + 1;
        link.target.missing = (link.target.missing ?? 0) + 1;
      }
    });

    linkKeysToPrune.forEach((key) => {
      intermediateNodesToPrune.push(key);
      dg.network.data.linkMap.delete(key);
    });
    console.log("pruned links: ", linkKeysToPrune.length);

    intermediateNodesToPrune.forEach((key) => {
      dg.network.data.nodeMap.delete(key);
    });
    console.log("pruned intermediate nodes: ", intermediateNodesToPrune.length);

    Array.from(dg.network.data.linkMap.values()).forEach((link) => {
      if (
        (link.source && intermediateNodesToPrune.includes(link.source.key)) ||
        (link.target && intermediateNodesToPrune.includes(link.target.key))
      ) {
        intermediateLinksToPrune.push(link.key);
        link.source.hasMissing = true;
        link.target.hasMissing = true;
        link.source.missing = (link.source.missing ?? 0) + 1;
        link.target.missing = (link.target.missing ?? 0) + 1;
      }
    });

    intermediateLinksToPrune.forEach((key) => {
      dg.network.data.linkMap.delete(key);
    });
    console.log("pruned intermediate links: ", intermediateLinksToPrune.length);

    console.log(
      `node size after pruning (maxDist: ${maxDist}, minLinks: ${minLinks}): `,
      dg.network.data.nodeMap.size,
    );
    console.log(
      `link size after pruning (maxDist: ${maxDist}, minLinks: ${minLinks}): `,
      dg.network.data.linkMap.size,
    );
  }
};

/**
 * Force function to keep nodes within the SVG bounds
 */
const bboxForce = (): void => {
  dg.network.data.nodeMap.forEach((node) => {
    const padding = 2 * (node.radius ?? 0);
    const minX = padding;
    const maxX = dg.svg_dimensions[0] - padding;
    const minY = padding;
    const maxY = dg.svg_dimensions[1] - padding;

    if (node.x < minX) {
      node.x = minX;
    }
    if (node.x > maxX) {
      node.x = maxX;
    }
    if (node.y < minY) {
      node.y = minY;
    }
    if (node.y > maxY) {
      node.y = maxY;
    }
  });
};
