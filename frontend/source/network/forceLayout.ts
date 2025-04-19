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
import { onNetworkEnd } from "./events";
import { discographManager, networkManager } from "../core";
import { clamp } from "../utils";
import { FORCE } from "../constants";

// Simulation parameters
export const ALPHA = 1.0; // Initial simulation temperature

let nodeStrengthMultiplier = 1.0;
let linkStrengthMultiplier = 1.0;
let gravStrengthMultiplier = 1.0;

function linkDistance(d: SimLink): number {
    if (d.role === FORCE.LINK.ROLES.ALIAS) return FORCE.DISTANCE.LINK_ALIAS;
    if (d.role === FORCE.LINK.ROLES.RELEASED_ON)
        return FORCE.DISTANCE.LINK_RELEASED_ON;
    if (d.isSpline) {
        return d.distance < 1
            ? FORCE.DISTANCE.LINK / 5
            : FORCE.DISTANCE.LINK / 10;
    } else {
        return FORCE.DISTANCE.LINK;
    }
}

function nodeStrength(d: SimNode): number {
    if (d.isIntermediate) return FORCE.NODE.STRENGTH_INTERMEDIATE;
    if (d.cluster) return FORCE.NODE.STRENGTH_CLUSTER;
    if (d.distance) {
        return (4 - clamp(d.distance, 0, 3)) * FORCE.NODE.STRENGTH;
    } else {
        return FORCE.NODE.STRENGTH;
    }
}

function gravityStrength(d: SimNode): number {
    var dist = d.distance ? 4 - clamp(d.distance, 0, 3) : 1.0;
    var maxDimension = Math.max(
        discographManager.svgDimensions[0],
        discographManager.svgDimensions[1],
    );
    var scaling = dist / 10.0;
    var radialDistance =
        (maxDimension -
            Math.max(
                d.x - discographManager.svgDimensions[0] / 2,
                d.y - discographManager.svgDimensions[1] / 2,
            )) /
        maxDimension;
    var g = radialDistance * scaling;
    return g;
}

/**
 * Determines the distance between linked nodes based on their relationship type
 * @param {SimLink} d - The link object
 * @returns {number} The desired distance between nodes
 */
function calculateLinkDistance(d: SimLink): number {
    return linkDistance(d) * linkStrengthMultiplier;
}

/**
 * Calculates the repulsion strength for each node
 * @param {SimNode} d - The node object
 * @returns {number} The repulsion strength
 */
function calculateNodeStrength(d: SimNode): number {
    return nodeStrength(d) * nodeStrengthMultiplier;
}

/**
 * Calculates the gravity strength for each node based on its position and distance
 * @param {SimNode} d - The node object
 * @returns {number} The gravity strength
 */
function calculateGravityStrength(d: SimNode): number {
    return gravityStrength(d) * gravStrengthMultiplier;
}

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
        .force(
            "charge",
            d3
                .forceManyBody<SimNode>()
                .strength(calculateNodeStrength)
                .distanceMax(FORCE.DISTANCE.MAX)
                .theta(FORCE.SIMULATION.THETA),
        )
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

export const initForceSliders = (): void => {
    const nodeSlider = document.getElementById("nodeRange") as HTMLInputElement;
    const linkSlider = document.getElementById("linkRange") as HTMLInputElement;
    const gravSlider = document.getElementById("gravRange") as HTMLInputElement;

    if (!nodeSlider || !linkSlider || !gravSlider) {
        console.error("Could not find one or more slider elements");
        return;
    }

    nodeSlider.oninput = function (this: HTMLInputElement): void {
        if (networkManager.forceLayout) {
            setupChargeForce(parseInt(this.value));
            restartForceLayout(FORCE.SIMULATION.ALPHA / 10.0);
        }
    };

    linkSlider.oninput = function (this: HTMLInputElement): void {
        if (networkManager.forceLayout) {
            setupLinkForce(parseInt(this.value));
            restartForceLayout(FORCE.SIMULATION.ALPHA / 5.0);
        }
    };

    gravSlider.oninput = function (this: HTMLInputElement): void {
        if (networkManager.forceLayout) {
            setupGravityForce(parseInt(this.value));
            restartForceLayout(FORCE.SIMULATION.ALPHA / 10.0);
        }
    };
};

const setupChargeForce = (nodeStrength: number): void => {
    nodeStrengthMultiplier =
        nodeStrength / FORCE.MULTIPLIER.NODE_STRENGTH_SCALE +
        FORCE.MULTIPLIER.NODE_STRENGTH_BASE;
    console.log("nodeStrengthMultiplier: ", nodeStrengthMultiplier);
    networkManager.forceLayout.force(
        "charge",
        d3
            .forceManyBody<SimNode>()
            .strength(calculateNodeStrength)
            .distanceMax(FORCE.DISTANCE.MAX)
            .theta(FORCE.SIMULATION.THETA),
    );
};

const setupLinkForce = (linkStrength: number): void => {
    linkStrengthMultiplier =
        linkStrength / FORCE.MULTIPLIER.LINK_STRENGTH_SCALE;
    console.log("linkStrengthMultiplier: ", linkStrengthMultiplier);
    networkManager.forceLayout.force(
        "link",
        d3
            .forceLink<SimNode, SimLink>()
            .id((d) => d.key ?? "")
            .links(Array.from(networkManager.data.linkMap.values()))
            .distance(calculateLinkDistance)
            .iterations(FORCE.LINK.ITERATIONS),
    );
};

const setupGravityForce = (gravityStrength: number): void => {
    gravStrengthMultiplier =
        gravityStrength / FORCE.MULTIPLIER.GRAVITY_STRENGTH_SCALE;
    console.log("gravStrengthMultiplier: ", gravStrengthMultiplier);
    networkManager.forceLayout
        .force(
            "x",
            d3
                .forceX<SimNode>(discographManager.svgDimensions[0] / 2)
                .strength(calculateGravityStrength),
        )
        .force(
            "y",
            d3
                .forceY<SimNode>(discographManager.svgDimensions[1] / 2)
                .strength(calculateGravityStrength),
        );
};

export const setupForceSliders = (): void => {
    const nodeSlider = document.getElementById("nodeRange") as HTMLInputElement;
    const linkSlider = document.getElementById("linkRange") as HTMLInputElement;
    const gravSlider = document.getElementById("gravRange") as HTMLInputElement;

    nodeSlider.value = "12";
    linkSlider.value = "40";
    gravSlider.value = "10";
    setupChargeForce(parseInt(nodeSlider.value));
    setupLinkForce(parseInt(linkSlider.value));
    setupGravityForce(parseInt(gravSlider.value));
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
    if (networkManager.forceLayout) {
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
 * Force function to keep nodes within the SVG bounds
 */
const bboxForce = (): void => {
    networkManager.data.nodeMap.forEach((node) => {
        const padding = 2 * (node.radius ?? 0);
        const minX = padding;
        const maxX = discographManager.svgDimensions[0] - padding;
        const minY = padding;
        const maxY = discographManager.svgDimensions[1] - padding;

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
