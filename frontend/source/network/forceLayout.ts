/**
 * forceLayout.ts
 * This file implements a force-directed graph layout system using D3.js for the Discograph application.
 * It handles node positioning, link creation, and graph simulation with various forces applied.
 */

import * as d3 from "d3";
import type { SimNode, SimLink } from "./data";
import { onHullEnter, onHullExit } from "./hull";
import { onHaloEnter, onHaloExit } from "./halo";
import { onNodeEnter, onNodeExit, onNodeUpdate } from "./node";
import { onTextEnter, onTextExit, onTextUpdate } from "./text";
import { onLinkEnter, onLinkExit, onLinkUpdate } from "./link";
import { onTick } from "./tick";
import { onNetworkEnd } from "./events";
import { dg } from "../dg";
import { clamp } from "../utils";

/**
 * Configuration Constants
 */
// Force configuration for nodes
const NODE_STRENGTH = -800; // Repulsion strength between nodes
const NODE_STRENGTH_CLUSTER = 100; // Repulsion strength between cluster nodes
const NODE_STRENGTH_INTERMEDIATE = NODE_STRENGTH / 2; // Repulsion strength for intermediate nodes

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
const LINK_STRENGTH = 1.8; // Strength of links between nodes
const LINK_DISTANCE_ALIAS = 20; // Distance for alias relationships
const LINK_DISTANCE_RELEASED_ON = 200; // Distance for "Released On" relationships
const LINK_DISTANCE = 60; // Default link distance
const LINK_ITERATIONS = 3; // Number of iterations for link force calculation

let nodeStrengthMultiplier = 1.0;
let linkStrengthMultiplier = 1.0;
let gravStrengthMultiplier = 1.0;

function linkDistance(d: SimLink): number {
    if (d.role == "Alias") return LINK_DISTANCE_ALIAS;
    if (d.role == "Released On") return LINK_DISTANCE_RELEASED_ON;
    if (d.isSpline) {
        return d.distance < 1 ? LINK_DISTANCE / 2 : LINK_DISTANCE / 10;
    } else {
        return LINK_DISTANCE;
    }
}

function nodeStrength(d: SimNode): number {
    if (d.isIntermediate) return NODE_STRENGTH_INTERMEDIATE;
    if (d.cluster) return NODE_STRENGTH_CLUSTER;
    if (d.distance) {
        return (4 - clamp(d.distance, 0, 3)) * NODE_STRENGTH;
    } else {
        return NODE_STRENGTH;
    }
}

function gravityStrength(d: SimNode): number {
    var dist = d.distance ? 4 - clamp(d.distance, 0, 3) : 1.0;
    var maxDimension = Math.max(dg.svg_dimensions[0], dg.svg_dimensions[1]);
    var scaling = dist / 10.0;
    var radialDistance =
        (maxDimension -
            Math.max(
                d.x - dg.svg_dimensions[0] / 2,
                d.y - dg.svg_dimensions[1] / 2,
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

    dg.network.forceLayout = d3
        .forceSimulation<SimNode>(Array.from(dg.network.data.nodeMap.values()))
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
                .strength(calculateNodeStrength)
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

    console.log("init dg.network.forceLayout: ", dg.network.forceLayout);
};

export const initForceSliders = (): void => {
    const nodeSlider = document.getElementById("nodeRange") as HTMLInputElement;
    const linkSlider = document.getElementById("linkRange") as HTMLInputElement;
    const gravSlider = document.getElementById("gravRange") as HTMLInputElement;

    if (!nodeSlider || !linkSlider || !gravSlider) {
        console.error("Could not find one or more slider elements");
        return;
    }

    nodeSlider.oninput = function (this: HTMLInputElement) {
        if (dg.network.forceLayout) {
            setupChargeForce(parseInt(this.value));
            restartForceLayout(ALPHA / 10.0);
        }
    };

    linkSlider.oninput = function (this: HTMLInputElement) {
        if (dg.network.forceLayout) {
            setupLinkForce(parseInt(this.value));
            restartForceLayout(ALPHA / 5.0);
        }
    };

    gravSlider.oninput = function (this: HTMLInputElement) {
        if (dg.network.forceLayout) {
            setupGravityForce(parseInt(this.value));
            restartForceLayout(ALPHA / 10.0);
        }
    };
};

const setupChargeForce = (nodeStrength: number): void => {
    nodeStrengthMultiplier = (nodeStrength / 20.0) + 0.4;
    console.log("nodeStrengthMultiplier: ", nodeStrengthMultiplier);
    dg.network.forceLayout.force(
        "charge",
        d3
            .forceManyBody<SimNode>()
            .strength(calculateNodeStrength)
            .distanceMax(DISTANCE_MAX)
            .theta(THETA),
    );
};

const setupLinkForce = (linkStrength: number): void => {
    linkStrengthMultiplier = linkStrength / 20.0;
    console.log("linkStrengthMultiplier: ", linkStrengthMultiplier);
    dg.network.forceLayout.force(
        "link",
        d3
            .forceLink<SimNode, SimLink>()
            .id((d) => d.key ?? "")
            .links(Array.from(dg.network.data.linkMap.values()))
            .distance(calculateLinkDistance)
            .iterations(LINK_ITERATIONS),
    );
};

const setupGravityForce = (gravityStrength: number): void => {
    gravStrengthMultiplier = gravityStrength / 10.0;
    console.log("gravStrengthMultiplier: ", gravStrengthMultiplier);
    dg.network.forceLayout
        .force(
            "x",
            d3
                .forceX<SimNode>(dg.svg_dimensions[0] / 2)
                .strength(calculateGravityStrength),
        )
        .force(
            "y",
            d3
                .forceY<SimNode>(dg.svg_dimensions[1] / 2)
                .strength(calculateGravityStrength),
        );
};

export const setupForceSliders = (): void => {
    const nodeSlider = document.getElementById("nodeRange") as HTMLInputElement;
    const linkSlider = document.getElementById("linkRange") as HTMLInputElement;
    const gravSlider = document.getElementById("gravRange") as HTMLInputElement;

    nodeSlider.value = "12";
    linkSlider.value = "20";
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
    console.log("Start D3 layout");
    const keyFunc = (d: SimNode | SimLink): string => ("key" in d ? d.key : "");

    const nodeData = Array.from(dg.network.data.nodeMap.values()).filter(
        (d) => !d.isIntermediate,
    );

    const linkData = Array.from(dg.network.data.linkMap.values()).filter(
        (d) => !d.isSpline,
    );

    // Debug nodes and links
    //     const nodeData = Array.from(dg.network.data.nodeMap.values());
    //     const linkData = Array.from(dg.network.data.linkMap.values());

    console.log("nodeData: ", nodeData);
    console.log("linkData: ", linkData);

    dg.network.selections.halo =
        dg.network.layers.halo?.selectAll<SVGGElement, SimNode>(".node") ??
        null;
    dg.network.selections.halo =
        dg.network.selections.halo?.data(nodeData, keyFunc) ?? null;

    dg.network.selections.node =
        dg.network.layers.node?.selectAll<SVGGElement, SimNode>(".node") ??
        null;
    dg.network.selections.node =
        dg.network.selections.node?.data(nodeData, keyFunc) ?? null;

    dg.network.selections.text =
        dg.network.layers.text?.selectAll<SVGGElement, SimNode>(".node") ??
        null;
    dg.network.selections.text =
        dg.network.selections.text?.data(nodeData, keyFunc) ?? null;

    dg.network.selections.link =
        dg.network.layers.link?.selectAll<SVGGElement, SimLink>(".link") ??
        null;
    dg.network.selections.link =
        dg.network.selections.link?.data(linkData, keyFunc) ?? null;

    const clusterNodes = Array.from(dg.network.data.nodeMap.values()).filter(
        (d) => d.cluster !== undefined,
    );
    const hullGroups = Array.from(
        d3.group(clusterNodes, (d) => d.cluster).values(),
    );
    const hullData = hullGroups.filter((d) => d.length > 1);
    dg.network.selections.hull =
        dg.network.layers.halo?.selectAll<SVGGElement, SimNode[]>(".hull") ??
        null;
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

    Array.from(dg.network.data.nodeMap.values()).forEach((n) => {
        n.fixed = false;
    });
};

/**
 * Initializes and starts the force layout simulation
 * Updates node and link selections and applies forces
 */
export const startForceLayout = (): void => {
    console.log("Start D3 layout");

    // Restart simulation
    console.log("Updating forceLayout");
    // console.log("dg.network.pageData.nodes: ", dg.network.pageData.nodes);
    dg.network.forceLayout.nodes(Array.from(dg.network.data.nodeMap.values()));
};

/**
 * Restarts the force layout simulation with a new alpha value
 * @param {number} alpha - The new alpha value for the simulation
 */
export const restartForceLayout = (alpha: number): void => {
    if (dg.network.forceLayout) {
        dg.network.forceLayout.alpha(alpha).restart();
    } else {
        console.error("Force layout is not initialized");
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
