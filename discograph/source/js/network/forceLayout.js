/**
 * forceLayout.js
 * This file implements a force-directed graph layout system using D3.js for the Discograph application.
 * It handles node positioning, link creation, and graph simulation with various forces applied.
 */

// Type declarations for our custom data types
/** @typedef {d3.SimulationNodeDatum & { key: string, radius: number }} NodeType */

// Import necessary modules using ES6 syntax
import * as d3 from 'd3';
import { onHullEnter, onHullExit } from './hull';
import { onHaloEnter, onHaloExit } from './halo';
import { onNodeEnter, onNodeExit, onNodeUpdate } from './node';
import { onTextEnter, onTextExit, onTextUpdate } from './text';
import { onLinkEnter, onLinkExit, onLinkUpdate } from './link';
import { onTick , getOuterRadius } from './tick';
import { onNetworkEnd, onNetworkStart } from './events';
import { clamp } from '../init';
import { dg } from '../dg';
/**
 * Configuration Constants
 */
// Force configuration for nodes
const NODE_STRENGTH = -800           // Repulsion strength between nodes
const DISTANCE_MAX = 2000           // Maximum distance for force calculations
const COLLIDE_ITERATIONS = 2        // Number of collision detection iterations
const COLLIDE_BUFFER = 12          // Extra space around nodes for collision detection
const CENTER_STRENGTH = 0.025      // Strength of centering force
const RADIAL_STRENGTH = 0.08      // Strength of radial force

// Simulation parameters
const THETA = 0.9                  // Barnes-Hut approximation criterion
export const ALPHA = 1.0                  // Initial simulation temperature
const ALPHA_DECAY = 0.03          // Rate at which simulation cools down
const VELOCITY_DECAY = 0.24       // Friction coefficient for node movement

// Link configuration
const LINK_STRENGTH = 1.8         // Strength of links between nodes
const LINK_DISTANCE_ALIAS = 20    // Distance for alias relationships
const LINK_DISTANCE_RELEASED_ON = 200  // Distance for "Released On" relationships
const LINK_DISTANCE = 60          // Default link distance
const LINK_DISTANCE_RANDOM = 20   // Random variation in link distance
const LINK_ITERATIONS = 3         // Number of iterations for link force calculation

// Graph size limits
const MAX_NODES_BEFORE_PRUNING = 600   // Maximum nodes before pruning is triggered
const MAX_LINKS_BEFORE_PRUNING = 1800  // Maximum links before pruning is triggered

// To give repeatable and predictable random behaviour, any number in [0, 1)
const random_seed = 0.42;
const random = d3.randomNormal.source(d3.randomLcg(random_seed))(0, 1);

/**
 * Determines the distance between linked nodes based on their relationship type
 * @param {Object} d - The link object
 * @returns {number} The desired distance between nodes
 */
function linkDistance(d, i) {
    if (d.isSpline) {
        if (d.role == 'Released On') {
            return LINK_DISTANCE_RELEASED_ON / 2;
        }
        return d.distance < 1 ? LINK_DISTANCE / 2 : LINK_DISTANCE / 10;
    } else if (d.role == 'Alias') {
        return LINK_DISTANCE_ALIAS;
    } else if (d.role == 'Released On') {
        return LINK_DISTANCE_RELEASED_ON;
    } else {
        return LINK_DISTANCE;
    }
}

/**
 * Calculates the repulsion strength for each node
 * @param {Object} d - The node object
 * @returns {number} The repulsion strength
 */
function nodeStrength(d, i) {
    if (d.distance) {
        var dist = 4 - clamp(d.distance, 0, 3);
        return dist * NODE_STRENGTH;
    } else if (d.isIntermediate) {
        return NODE_STRENGTH / 10;
    } else if (d.cluster) {
              return 100;
    } else {
        return NODE_STRENGTH;
    }
}

/**
 * Calculates the gravity strength for each node based on its position and distance
 * @param {Object} d - The node object
 * @returns {number} The gravity strength
 */
function gravityStrength(d, i) {
    var dist = d.distance ? 4 - clamp(d.distance, 0, 3) : 1.0;
    var maxDimension = Math.max(dg.svg_dimensions[0], dg.svg_dimensions[1]);
    var scaling = dist / 10.0;
    var radialDistance = (maxDimension - Math.max(d.x - dg.svg_dimensions[0] / 2, d.y - dg.svg_dimensions[1] / 2)) / maxDimension;
    var g = radialDistance * scaling;
    return g;
}

/**
 * Sets up the initial force simulation with basic forces
 */
export const setupForceLayout = () => {
    console.log("setupForceLayout");
    
    dg.network.forceLayout = d3.forceSimulation(/** @type {NodeType[]} */ (dg.network.pageData.nodes))
        .force("collide", d3.forceCollide().radius((d) => (/** @type {NodeType} */ (d)).radius + COLLIDE_BUFFER).iterations(COLLIDE_ITERATIONS))
        .force("charge", d3.forceManyBody().strength(nodeStrength).distanceMax(DISTANCE_MAX).theta(THETA))
        .force("bbox", bboxForce)
        .on("tick", function() { onTick(this); })
        .on("end", function() { onNetworkEnd(this); })
        .stop();
}

/**
 * Initializes and starts the force layout simulation
 * Updates node and link selections and applies forces
 */
export const startForceLayout = () => {
    console.log("Start D3 layout");
    var keyFunc = function(d) { return d.key }
    var nodeData = dg.network.pageData.nodes.filter(function(d) {
        return !d.isIntermediate;
    })
    console.log("nodeData: ", nodeData);
    var linkData = dg.network.pageData.links.filter(function(d) {
        return !d.isSpline;
    })
    console.log("linkData: ", linkData);

    dg.network.selections.halo = dg.network.layers.halo?.selectAll(".node") ?? null;
    dg.network.selections.halo = dg.network.selections.halo?.data(nodeData, keyFunc) ?? null;

    dg.network.selections.node = dg.network.layers.node?.selectAll(".node") ?? null;
    dg.network.selections.node = dg.network.selections.node?.data(nodeData, keyFunc) ?? null;

    dg.network.selections.text = dg.network.layers.text?.selectAll(".node") ?? null;
    dg.network.selections.text = dg.network.selections.text?.data(nodeData, keyFunc) ?? null;

    dg.network.selections.link = dg.network.layers.link?.selectAll(".link") ?? null;
    dg.network.selections.link = dg.network.selections.link?.data(linkData, keyFunc) ?? null;

    var clusterNodes = dg.network.pageData.nodes.filter(function(d) {
        return d.cluster !== undefined;
    });
    var hullGroup = d3.group(clusterNodes, function(d) { return d.cluster; }).values();
    var hullData = d3.filter(hullGroup, function(d) { return 1 < Array.from(d.values()).length; });
    dg.network.selections.hull = dg.network.layers.halo?.selectAll(".hull") ?? null;
    dg.network.selections.hull = dg.network.selections.hull?.data(hullData) ?? null;

    if (dg.network.selections.halo) onHaloEnter(dg.network.selections.halo.enter());
    if (dg.network.selections.halo) onHaloExit(dg.network.selections.halo.exit());
    if (dg.network.selections.hull) onHullEnter(dg.network.selections.hull.enter());
    if (dg.network.selections.hull) onHullExit(dg.network.selections.hull.exit());
    if (dg.network.selections.node) onNodeEnter(dg.network.selections.node.enter());
    if (dg.network.selections.node) onNodeExit(dg.network.selections.node.exit());
    if (dg.network.selections.node) onNodeUpdate(dg.network.selections.node);
    if (dg.network.selections.text) onTextEnter(dg.network.selections.text.enter());
    if (dg.network.selections.text) onTextExit(dg.network.selections.text.exit());
    if (dg.network.selections.text) onTextUpdate(dg.network.selections.text);
    if (dg.network.selections.link) onLinkEnter(dg.network.selections.link.enter());
    if (dg.network.selections.link) onLinkExit(dg.network.selections.link.exit());
    if (dg.network.selections.link) onLinkUpdate(dg.network.selections.link);
    dg.network.pageData.nodes.forEach(function(n) { n.fixed = false; });

    // Restart simulation
    console.log("Updating forceLayout");
    if (!dg.network.forceLayout) return;
    
    dg.network.forceLayout.nodes(dg.network.pageData.nodes);

    if (nodeData.length > 16 && nodeData.length < 500) {
        dg.network.forceLayout.force("x", d3.forceX(dg.svg_dimensions[0] / 2).strength(gravityStrength));
        dg.network.forceLayout.force("y", d3.forceY(dg.svg_dimensions[1] / 2).strength(gravityStrength));
    } else {
        dg.network.forceLayout.force("x", null);
        dg.network.forceLayout.force("y", null);
    }
    dg.network.forceLayout.force("link", d3.forceLink().id((d) => (/** @type {NodeType} */ (d)).key).links(dg.network.pageData.links).distance(linkDistance).iterations(LINK_ITERATIONS));

    restartForceLayout();
}

/**
 * Restarts the force layout simulation with optional alpha value
 * @param {number} [alpha] - Optional initial alpha value for the simulation
 */
export const restartForceLayout = (alpha) => {
    if (!alpha) {
        alpha = ALPHA;
    }

    if (!dg.network.forceLayout) return;

    onNetworkStart();
    dg.network.forceLayout
        .force("center", d3.forceCenter(dg.svg_dimensions[0] / 2, dg.svg_dimensions[1] / 2))
        .alpha(alpha).alphaDecay(ALPHA_DECAY).velocityDecay(VELOCITY_DECAY).restart();
}

/**
 * Immediately stops the force layout simulation
 * Sets the alpha value to 0 to halt all force calculations
 */
export const stopForceLayout = () => {
    console.log("stopForceLayout: ");
    if (!dg.network.forceLayout) return;
    dg.network.forceLayout.alpha(0);
    dg.network.forceLayout.stop();
}

/**
 * Processes incoming JSON data to create nodes and links
 * Handles intermediate nodes creation and graph structure setup
 * @param {Object} json - Input JSON data containing nodes and links
 */
export const processJson = (json) => {
    var newNodeMap = new Map();
    var newLinkMap = new Map();

    // Setup node size
    json.nodes.forEach(function(node) {
        node.radius = getOuterRadius(node);
        newNodeMap.set(node.key, node);
    });

    // Setup links, add intermediate node at center of link
    json.links.forEach(function(link) {
        var source = link.source,
            target = link.target;
        if (link.role != 'Alias') {
            var role = link.role.toLocaleLowerCase().replace(/\s+/g, "-");
            var intermediateNode = {
                key: link.key,
                isIntermediate: true,
                pages: link.pages,
                size: 0,
                };
            var s2iSplineLink = {
                isSpline: true,
                key: source + "-" + role + "-[" + target + "]",
                pages: link.pages,
                source: source,
                target: link.key,
            };
            var i2tSplineLink = {
                isSpline: true,
                key: "[" + source + "]-" + role + "-" + target,
                pages: link.pages,
                source: link.key,
                target: target,
            };
            link.intermediate = link.key;
            newNodeMap.set(link.key, intermediateNode);
            newLinkMap.set(s2iSplineLink.key, s2iSplineLink);
            newLinkMap.set(i2tSplineLink.key, i2tSplineLink);
        }
        newLinkMap.set(link.key, link);
    });

    // Update current lists of nodes and links
    var nodeKeysToRemove = [];
    Array.from(dg.network.data.nodeMap.keys())
        .forEach(function(key) {
            if (!newNodeMap.has(key)) {
                nodeKeysToRemove.push(key);
            };
        });
    nodeKeysToRemove.forEach(function(key) {
        dg.network.data.nodeMap.delete(key);
    });
    var linkKeysToRemove = [];
    Array.from(dg.network.data.linkMap.keys())
        .forEach(function(key) {
            if (!newLinkMap.has(key)) {
                linkKeysToRemove.push(key);
            };
        });
    linkKeysToRemove.forEach(function(key) {
        dg.network.data.linkMap.delete(key);
    });
    newNodeMap.forEach(function(newNode, key) {
        if (dg.network.data.nodeMap.has(key)) {
            var oldNode = dg.network.data.nodeMap.get(key);
            oldNode.cluster = newNode.cluster;
            oldNode.distance = newNode.distance;
            oldNode.links = newNode.links;
            oldNode.missing = newNode.missing;
            oldNode.missingByPage = newNode.missingByPage;
            oldNode.pages = newNode.pages;
            var dist = oldNode.distance ? oldNode.distance : 1.0;
            var dx = (random() * 2.0 - 1.0) * LINK_DISTANCE * dist * 10;
            var dy = (random() * 2.0 - 1.0) * LINK_DISTANCE * dist * 10;
            oldNode.x = dg.network.newNodeCoords[0] + dx;
            oldNode.y = dg.network.newNodeCoords[1] + dy;
        } else {
            var dist = newNode.distance ? newNode.distance : 1.0;
            var dx = (random() * 2.0 - 1.0) * LINK_DISTANCE * dist * 10;
            var dy = (random() * 2.0 - 1.0) * LINK_DISTANCE * dist * 10;
            //console.log("dx: " + dx + " dy: " + dy);
            newNode.x = dg.network.newNodeCoords[0] + dx;
            newNode.y = dg.network.newNodeCoords[1] + dy;
            dg.network.data.nodeMap.set(key, newNode);
        }
    });
    newLinkMap.forEach(function(newLink, key) {
        if (dg.network.data.linkMap.has(key)) {
            var oldLink = dg.network.data.linkMap.get(key);
            oldLink.pages = newLink.pages;
        } else {
            newLink.source = dg.network.data.nodeMap.get(newLink.source);
            newLink.target = dg.network.data.nodeMap.get(newLink.target);
            if (newLink.intermediate !== undefined) {
                newLink.intermediate = dg.network.data.nodeMap.get(newLink.intermediate);
            }
            dg.network.data.linkMap.set(key, newLink);
        }
    });

    // Get some useful stats
    var distances = []
    var distance_counts = [0, 0, 0, 0, 0, 0]
    Array.from(dg.network.data.nodeMap.values())
        .forEach(function(node) {
            if (node.distance !== undefined) {
                distances.push(node.distance);
                if (node.distance < distance_counts.length) {
                    distance_counts[node.distance]++;
                }
            }
    })
    dg.network.data.maxDistance = Math.max.apply(Math, distances);
    console.log("maxDistance: ", dg.network.data.maxDistance);
    console.log("distance_counts: ", distance_counts);
    console.log("initial node size: ", dg.network.data.nodeMap.size);
    console.log("initial link size: ", dg.network.data.linkMap.size);

    // Prune dist==3
    prune(3, 1)
    prune(3, 2)
    prune(3, 3)
    prune(3, 100)
    prune(3, 1000000)
    prune(2, 1)
    prune(2, 2)
    prune(2, 3)
    prune(2, 100)
    prune(2, 100000)

    console.log("final nodes: ", dg.network.data.nodeMap);
}

/**
 * Prunes the graph to reduce complexity when it exceeds size limits
 * @param {number} maxDist - Maximum distance threshold for pruning
 * @param {number} minLinks - Minimum number of links a node must have to avoid pruning
 */
const prune = (maxDist, minLinks) => {
    if (dg.network.data.nodeMap.size > MAX_NODES_BEFORE_PRUNING ||
        dg.network.data.linkMap.size > MAX_LINKS_BEFORE_PRUNING) {
        var nodeKeysToPrune = [];
        Array.from(dg.network.data.nodeMap.values())
            .forEach(function(node) {
                if (node.distance >= maxDist && node.links && node.links.length <= minLinks) {
                    nodeKeysToPrune.push(node.key);
                };
            });
        nodeKeysToPrune.forEach(function(key) {
            dg.network.data.nodeMap.delete(key);
        });
        console.log("pruned nodes: ", nodeKeysToPrune.length);

        var linkKeysToPrune = [];
        var intermediateNodesToPrune = [];
        var intermediateLinksToPrune = [];
        Array.from(dg.network.data.linkMap.values())
            .forEach(function(link) {
                if ((link.source && nodeKeysToPrune.indexOf(link.source.key) != -1) ||
                    (link.target && nodeKeysToPrune.indexOf(link.target.key) != -1)) {
                    linkKeysToPrune.push(link.key);
                    link.source.hasMissing = true;
                    link.target.hasMissing = true;
                    if (link.source.missing === undefined) {
                        link.source.missing = 1;
                    } else {
                        link.source.missing = link.source.missing + 1;
                    }
                    if (link.target.missing === undefined) {
                        link.target.missing = 1;
                    } else {
                        link.target.missing = link.target.missing + 1;
                    }
//                    console.log("link.source: ", link.source);
//                    console.log("link.target: ", link.target);
                };
            });
        linkKeysToPrune.forEach(function(key) {
            intermediateNodesToPrune.push(key);
            dg.network.data.linkMap.delete(key);
        });
        console.log("pruned links: ", linkKeysToPrune.length);
        intermediateNodesToPrune.forEach(function(key) {
            dg.network.data.nodeMap.delete(key);
        });
        console.log("pruned intermediate nodes: ", intermediateNodesToPrune.length);

        Array.from(dg.network.data.linkMap.values())
            .forEach(function(link) {
                if ((link.source && intermediateNodesToPrune.indexOf(link.source.key) != -1) ||
                    (link.target && intermediateNodesToPrune.indexOf(link.target.key) != -1)) {
                    intermediateLinksToPrune.push(link.key);
                    link.source.hasMissing = true;
                    link.target.hasMissing = true;
                    if (link.source.missing === undefined) {
                        link.source.missing = 1;
                    } else {
                        link.source.missing = link.source.missing + 1;
                    }
                    if (link.target.missing === undefined) {
                        link.target.missing = 1;
                    } else {
                        link.target.missing = link.target.missing + 1;
                    }
//                    console.log("link.source: ", link.source);
//                    console.log("link.target: ", link.target);
                };
            });
        intermediateLinksToPrune.forEach(function(key) {
            dg.network.data.linkMap.delete(key);
        });
        console.log("pruned intermediate links: ", intermediateLinksToPrune.length);

        console.log("node size after pruning (maxDist: " + maxDist + ", minLinks: " + minLinks + "): ", dg.network.data.nodeMap.size);
        console.log("link size after pruning (maxDist: " + maxDist + ", minLinks: " + minLinks + "): ", dg.network.data.linkMap.size);
    }

}

/**
 * Applies bounding box constraints to keep nodes within the SVG viewport
 * Ensures nodes don't move outside the visible area
 */
const bboxForce = () => {
    dg.network.data.nodeMap.forEach(node => {
        var padding = 2 * node.radius;
        var minX = padding;
        var maxX = dg.svg_dimensions[0] - padding;
        var minY = padding;
        var maxY = dg.svg_dimensions[1] - padding;
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
    })
}
