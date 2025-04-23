/**
 * forceLayout.ts
 * This file implements a force-directed graph layout system using D3.js for the Discograph application.
 * It handles node positioning, link creation, and graph simulation with various forces applied.
 */

import * as d3 from "d3";
import type { SimNode, SimLink } from "./data";
import { onHullEnter, onHullUpdate, onHullExit } from "./hull";
import { onHaloEnter, onHaloUpdate, onHaloExit } from "./halo";
import { onNodeEnter, onNodeUpdate, onNodeExit } from "./node";
import { onTextEnter, onTextUpdate, onTextExit } from "./text";
import { onLinkEnter, onLinkUpdate, onLinkExit } from "./link";
import { onTick } from "./tick";
import { onNetworkStart, onNetworkEnd } from "./events";
import { discographManager, networkManager } from "../core";
import { FORCE } from "../constants";

// function linkDistance(d: SimLink): number {
//     if (d.role === FORCE.LINK.ROLES.ALIAS) return FORCE.DISTANCE.LINK_ALIAS;
//     if (d.role === FORCE.LINK.ROLES.RELEASED_ON)
//         return FORCE.DISTANCE.LINK_RELEASED_ON;
//     if (d.isSpline) {
//         return d.distance < 1
//             ? FORCE.DISTANCE.LINK / 5
//             : FORCE.DISTANCE.LINK / 10;
//     } else {
//         return FORCE.DISTANCE.LINK;
//     }
// }

// function nodeStrength(d: SimNode): number {
//     if (d.isIntermediate) return FORCE.NODE.STRENGTH_INTERMEDIATE;
//     if (d.cluster) return FORCE.NODE.STRENGTH_CLUSTER;
//     if (d.distance) {
//         return (4 - clamp(d.distance, 0, 3)) * FORCE.NODE.STRENGTH;
//     } else {
//         return FORCE.NODE.STRENGTH;
//     }
// }

// function gravityStrength(d: SimNode): number {
//     var dist = d.distance ? 4 - clamp(d.distance, 0, 3) : 1.0;
//     var maxDimension = Math.max(
//         discographManager.svgDimensions[0],
//         discographManager.svgDimensions[1],
//     );
//     var scaling = dist / 10.0;
//     var radialDistance =
//         (maxDimension -
//             Math.max(
//                 d.x - discographManager.svgDimensions[0] / 2,
//                 d.y - discographManager.svgDimensions[1] / 2,
//             )) /
//         maxDimension;
//     var g = radialDistance * scaling;
//     return g;
// }

/**
 * Determines the distance between linked nodes based on their relationship type
 * @param {SimLink} d - The link object
 * @returns {number} The desired distance between nodes
 */
// function calculateLinkDistance(d: SimLink): number {
//     return linkDistance(d) * linkStrengthMultiplier;
// }

/**
 * Calculates the repulsion strength for each node
 * @param {SimNode} d - The node object
 * @returns {number} The repulsion strength
 */
// function calculateNodeStrength(d: SimNode): number {
//     return nodeStrength(d) * nodeStrengthMultiplier;
// }

/**
 * Calculates the gravity strength for each node based on its position and distance
 * @param {SimNode} d - The node object
 * @returns {number} The gravity strength
 */
// function calculateGravityStrength(d: SimNode): number {
//     return gravityStrength(d) * gravStrengthMultiplier;
// }

/**
 * Sets up the initial force simulation with basic forces
 */
export const initForceLayout = (): void => {
    console.log("initForceLayout");

    networkManager.forceLayout = d3
        .forceSimulation<SimNode>(
            Array.from(networkManager.data.nodeMap.values()),
        )
        .force(
            "collide",
            d3
                .forceCollide<SimNode>()
                .radius((d) => (d.radius ?? 0) + FORCE.COLLIDE.BUFFER)
                .iterations(FORCE.COLLIDE.ITERATIONS),
        )
        // .force(
        //     "charge",
        //     d3
        //         .forceManyBody<SimNode>()
        //         .strength(calculateNodeStrength)
        //         .distanceMax(FORCE.DISTANCE.MAX)
        //         .theta(FORCE.SIMULATION.THETA),
        // )
        .force("bbox", bboxForce)
        .on("tick", function (this: d3.Simulation<SimNode, SimLink>) {
            onTick(this);
        })
        .on("end", function (this: d3.Simulation<SimNode, SimLink>) {
            onNetworkEnd(this);
        })
        .stop();

    console.log(
        "init networkManager.forceLayout: ",
        networkManager.forceLayout,
    );
};

/**
 * Initializes and starts the force layout simulation
 * Updates node and link selections and applies forces
 */
export const displayForceLayout = (): void => {
    console.log("displayForceLayout");

    const keyFunc = (d: SimNode | SimLink): string => d.key;

    const nodeData = Array.from(networkManager.data.nodeMap.values()).filter(
        (d) => !d.isIntermediate,
    );

    const linkData = Array.from(networkManager.data.linkMap.values()).filter(
        (d) => !d.isSpline,
    );

    // Debug nodes and links
    //     const nodeData = Array.from(networkStore.data.nodeMap.values());
    //     const linkData = Array.from(networkStore.data.linkMap.values());

    console.log("nodeData: ", nodeData);
    console.log("linkData: ", linkData);

    networkManager.layers.halo
        .selectAll<SVGGElement, SimNode>(".node")
        .data(nodeData, keyFunc)
        .join(
            (enter) => {
                return onHaloEnter(enter);
            },
            (update) => {
                return onHaloUpdate(update);
            },
            (exit) => {
                return onHaloExit(exit);
            },
        );

    networkManager.layers.node
        .selectAll<SVGGElement, SimNode>(".node")
        .data(nodeData, keyFunc)
        .join(
            (enter) => {
                return onNodeEnter(enter);
            },
            (update) => {
                return onNodeUpdate(update);
            },
            (exit) => {
                return onNodeExit(exit);
            },
        );

    networkManager.layers.text
        .selectAll<SVGGElement, SimNode>(".node")
        .data(nodeData, keyFunc)
        .join(
            (enter) => {
                return onTextEnter(enter);
            },
            (update) => {
                return onTextUpdate(update);
            },
            (exit) => {
                return onTextExit(exit);
            },
        );

    networkManager.layers.link
        .selectAll<SVGGElement, SimLink>(".link")
        .data(linkData, keyFunc)
        .join(
            (enter) => {
                return onLinkEnter(enter);
            },
            (update) => {
                return onLinkUpdate(update);
            },
            (exit) => {
                return onLinkExit(exit);
            },
        );

    const clusterNodes = Array.from(
        networkManager.data.nodeMap.values(),
    ).filter((d) => d.cluster !== undefined);
    const hullGroups = Array.from(
        d3.group(clusterNodes, (d) => d.cluster).values(),
    );
    const hullData = hullGroups.filter((d) => d.length > 1);

    networkManager.layers.halo
        .selectAll<SVGGElement, SimNode[]>(".hull")
        .data(hullData)
        .join(
            (enter) => {
                return onHullEnter(enter);
            },
            (update) => {
                return onHullUpdate(update);
            },
            (exit) => {
                return onHullExit(exit);
            },
        );

    Array.from(networkManager.data.nodeMap.values()).forEach(
        (n) => (n.fixed = false),
    );
};

/**
 * Initializes and starts the force layout simulation
 * Updates node and link selections and applies forces
 */
export const startForceLayout = (nodes: SimNode[]): void => {
    console.log("Start D3 layout nodes:", nodes);

    // Restart simulation
    console.log("Updating forceLayout");
    networkManager.forceLayout.nodes(nodes);
};

/**
 * Restarts the force layout simulation with a new alpha value
 * @param {number} alpha - The new alpha value for the simulation
 */
export const restartForceLayout = (alpha: number): void => {
    console.log("restartForceLayout:", alpha);

    if (networkManager.forceLayout) {
        onNetworkStart();
        networkManager.forceLayout.alpha(alpha).restart();
    } else {
        console.error("Force layout is not initialized");
    }
};

/**
 * Stops the force layout simulation
 */
export const stopForceLayout = (): void => {
    if (networkManager.forceLayout) {
        networkManager.forceLayout.stop();
    }
};

/**
 * Custom force to keep nodes within the SVG boundaries
 */
const bboxForce = (): void => {
    const bbox = {
        width: discographManager.svgDimensions[0],
        height: discographManager.svgDimensions[1],
    };

    if (networkManager.forceLayout && networkManager.forceLayout.nodes()) {
        // Update nodes to keep them within bounds
        for (const node of networkManager.forceLayout.nodes()) {
            if (!node.radius) continue;

            const radius = node.radius + FORCE.COLLIDE.BUFFER * 2;
            const maxX = bbox.width - radius;
            const maxY = bbox.height - radius;
            const minX = radius;
            const minY = radius;

            if (node.x > maxX) node.x = maxX;
            if (node.x < minX) node.x = minX;
            if (node.y > maxY) node.y = maxY;
            if (node.y < minY) node.y = minY;
        }
    }
};
