
NODE_STRENGTH = -350
DISTANCE_MAX = 2000

COLLIDE_ITERATIONS = 2
//COLLIDE_RADIUS_POWER = 1.5
COLLIDE_BUFFER = 12

CENTER_STRENGTH = 0.025
RADIAL_STRENGTH = 0.08

THETA = 0.9; // defaults to 0.9
ALPHA = 1.0
ALPHA_DECAY = 0.03
VELOCITY_DECAY = 0.24; // like friction, defaults to 0.4. less velocity decay may converge on a better solution, but risks numerical instabilities and oscillation.

LINK_STRENGTH = 1.8
LINK_DISTANCE_ALIAS = 20
LINK_DISTANCE_RELEASED_ON = 200
LINK_DISTANCE = 180
LINK_DISTANCE_RANDOM = 50
LINK_ITERATIONS = 3

MAX_NODES_BEFORE_PRUNING = 600
MAX_LINKS_BEFORE_PRUNING = 1800

const seed = 0.42; // any number in [0, 1)
const random = d3.randomNormal.source(d3.randomLcg(seed))(0, 1);


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
//        console.log("d.source: ", d.source);
//        console.log("d.target: ", d.target);
        var dist = d.source.distance == 0 || d.target.distance == 0 || d.source.distance == 3 || d.target.distance == 3 ? 1.0 : (d.source.distance + d.target.distance) / 2.0
        return LINK_DISTANCE;
//        return LINK_DISTANCE * dist + (random() * LINK_DISTANCE_RANDOM * dist);
    }
}

function nodeStrength(d, i) {
    if (d.distance) {
        var dist = 4 - d.distance;
        return dist * NODE_STRENGTH;
//        return d.distance == 0 ? 3 * NODE_STRENGTH : d.distance == 1 ? 2.5 * NODE_STRENGTH : NODE_STRENGTH;
    } else if (d.isIntermediate) {
        return Math.hypot(d.x - dg.dimensions[0] / 2, d.y - dg.dimensions[1] / 2) <= 300 ? 3 * NODE_STRENGTH : NODE_STRENGTH / 2;
    } else {
        return 0;
    }
}

function gravityStrength(d, i) {
    if (d.distance) {
        var maxDimension = Math.max(dg.dimensions[0], dg.dimensions[1]);
        var dist = 3 - d.distance;
        var scaling = dist / 5.0;
        var radialDistance = (maxDimension - Math.hypot(d.x - dg.dimensions[0] / 2, d.y - dg.dimensions[1] / 2)) / maxDimension;
        var g = radialDistance * scaling;
        return g;
    } else {
        return 0;
    }
}

function dg_network_setupForceLayout() {
    console.log("dg_network_setupForceLayout");
    return d3.forceSimulation(dg.network.pageData.nodes)
        .force("collide", d3.forceCollide().radius(d => d.radius + COLLIDE_BUFFER).iterations(COLLIDE_ITERATIONS))
        .force("charge", d3.forceManyBody().strength(nodeStrength).distanceMax(DISTANCE_MAX).theta(THETA))
        .force("bbox", dg_network_bbox_force)
        .on("tick", dg_network_tick)
        .on("end", dg_network_end)
        .stop();
}

function dg_network_startForceLayout() {
    console.log("Start D3 layout");
    var keyFunc = function(d) { return d.key }
    var nodeData = dg.network.pageData.nodes.filter(function(d) {
        return (!d.isIntermediate) && (d.pages.indexOf(dg.network.pageData.currentPage) != -1);
    })
    // console.log("nodeData: ", nodeData);
    var linkData = dg.network.pageData.links.filter(function(d) {
        return (!d.isSpline) && (d.pages.indexOf(dg.network.pageData.currentPage) != -1);
    })
    // console.log("linkData: ", linkData);

    dg.network.selections.halo = dg.network.layers.halo.selectAll(".node");
    dg.network.selections.halo = dg.network.selections.halo.data(nodeData, keyFunc);

    dg.network.selections.node = dg.network.layers.node.selectAll(".node");
    dg.network.selections.node = dg.network.selections.node.data(nodeData, keyFunc);

    dg.network.selections.text = dg.network.layers.text.selectAll(".node");
    dg.network.selections.text = dg.network.selections.text.data(nodeData, keyFunc);

    dg.network.selections.link = dg.network.layers.link.selectAll(".link");
    dg.network.selections.link = dg.network.selections.link.data(linkData, keyFunc);

    var clusterNodes = dg.network.pageData.nodes
        .filter(function(d) { return d.cluster !== undefined; });
    var hullGroup = d3.group(clusterNodes, function(d) { return d.cluster; }).values();
    var hullData = d3.filter(hullGroup, function(d) { return 1 < Array.from(d.values()).length; });
    dg.network.selections.hull = dg.network.layers.halo.selectAll(".hull");
    dg.network.selections.hull = dg.network.selections.hull.data(hullData);

    dg_network_onHaloEnter(dg.network.selections.halo.enter());
    dg_network_onHaloExit(dg.network.selections.halo.exit());
    dg_network_onHullEnter(dg.network.selections.hull.enter());
    dg_network_onHullExit(dg.network.selections.hull.exit());
    dg_network_onNodeEnter(dg.network.selections.node.enter());
    dg_network_onNodeExit(dg.network.selections.node.exit());
    dg_network_onNodeUpdate(dg.network.selections.node);
    dg_network_onTextEnter(dg.network.selections.text.enter());
    dg_network_onTextExit(dg.network.selections.text.exit());
    dg_network_onTextUpdate(dg.network.selections.text);
    dg_network_onLinkEnter(dg.network.selections.link.enter());
    dg_network_onLinkExit(dg.network.selections.link.exit());
    dg_network_onLinkUpdate(dg.network.selections.link);
    dg.network.pageData.nodes.forEach(function(n) { n.fixed = false; });

    // Restart simulation
    console.log("Updating forceLayout");
    // console.log("dg.network.pageData.nodes: ", dg.network.pageData.nodes);
    dg.network.forceLayout.nodes(dg.network.pageData.nodes);

    if (nodeData.length > 16 && nodeData.length < 500) {
        dg.network.forceLayout.force("x", d3.forceX(dg.dimensions[0] / 2).strength(gravityStrength));
        dg.network.forceLayout.force("y", d3.forceY(dg.dimensions[1] / 2).strength(gravityStrength));
    } else {
        dg.network.forceLayout.force("x", null);
        dg.network.forceLayout.force("y", null);
    }
//    if (linkData.length > 16 && linkData.length < 500) {
        dg.network.forceLayout.force("link", d3.forceLink().id(d => d.key).links(dg.network.pageData.links).distance(linkDistance).iterations(LINK_ITERATIONS));
//    } else {
//        dg.network.forceLayout.force("link", d3.forceLink().id(d => d.key).links(dg.network.pageData.links).distance(d => linkDistance(d) / 10.0).iterations(LINK_ITERATIONS));
//    }

    dg_network_forceLayout_restart();
}

function dg_network_forceLayout_restart(alpha) {
    if (!alpha) {
        alpha = ALPHA;
    }

//    dg.network.forceLayout.force("x", null);
//    dg.network.forceLayout.force("y", null);
//    dg.network.forceLayout.force("x", d3.forceX(dg.dimensions[0] / 2).strength(CENTER_STRENGTH));
//    dg.network.forceLayout.force("y", d3.forceY(dg.dimensions[1] / 2).strength(CENTER_STRENGTH));
//  dg.network.forceLayout.force("x", d3.forceX(dg.dimensions[0] / 2).strength(d => (4 - d.distance) * CENTER_STRENGTH))
//  dg.network.forceLayout.force("y", d3.forceY(dg.dimensions[1] / 2).strength(d => (4 - d.distance) * CENTER_STRENGTH))
//        .force("center", d3.forceCenter(dg.dimensions[0] / 2, dg.dimensions[1] / 2))
//        .force("center", d3.forceCenter(dg.dimensions[0] / 2, dg.dimensions[1] / 2).strength(CENTER_STRENGTH))
//    dg.network.forceLayout.force("radial",
//        d3.forceRadial()
//            .radius(Math.max(dg.dimensions[0] / 2, dg.dimensions[1] / 2))
//            .x(dg.dimensions[0] / 2)
//            .y(dg.dimensions[1] / 2)
//            .strength(d => d.distance == 3 ? RADIAL_STRENGTH : 0)
//    )
    dg_network_start();
    dg.network.forceLayout
        .force("center", d3.forceCenter(dg.dimensions[0] / 2, dg.dimensions[1] / 2))
//        .force("center", d3.forceCenter(dg.dimensions[0] / 2, dg.dimensions[1] / 2).strength(CENTER_STRENGTH))
        .alpha(alpha).alphaDecay(ALPHA_DECAY).velocityDecay(VELOCITY_DECAY).restart();
}

function dg_network_processJson(json) {
    var newNodeMap = new Map();
    var newLinkMap = new Map();

    // Setup node size
    json.nodes.forEach(function(node) {
        node.radius = dg_network_getOuterRadius(node);
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
            var dx = (random() * 2.0 - 1.0) * LINK_DISTANCE * oldNode.distance
            var dy = (random() * 2.0 - 1.0) * LINK_DISTANCE * oldNode.distance
            oldNode.x = dg.network.newNodeCoords[0] + dx;
            oldNode.y = dg.network.newNodeCoords[1] + dy;
        } else {
            var dx = (random() * 2.0 - 1.0) * LINK_DISTANCE * newNode.distance
            var dy = (random() * 2.0 - 1.0) * LINK_DISTANCE * newNode.distance
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
    dg_network_prune(3, 1)
    dg_network_prune(3, 2)
    dg_network_prune(3, 3)
    dg_network_prune(3, 100)
    dg_network_prune(3, 1000000)
    dg_network_prune(2, 1)
    dg_network_prune(2, 2)
    dg_network_prune(2, 3)
    dg_network_prune(2, 100)
    dg_network_prune(2, 100000)

    console.log("final nodes: ", dg.network.data.nodeMap);
}

function dg_network_prune(maxDist, minLinks) {
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

function dg_network_bbox_force() {
    dg.network.data.nodeMap.forEach(node => {

        var minX = 20 + node.radius;
        var maxX = dg.dimensions[0] - 100 - node.radius;
        var minY = 100 + node.radius;
        var maxY = dg.dimensions[1] - 100 - node.radius;
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

// Reheat the simulation when drag starts, and fix the subject position.
function dg_network_dragstarted(event) {
    event.subject.fx = event.subject.x;
    event.subject.fy = event.subject.y;
    event.subject.dragx = event.subject.x;
    event.subject.dragy = event.subject.y;
    if (event.sourceEvent.type == 'mousedown') {
        dg_network_onNodeMouseDown(event.sourceEvent, event.subject);
    }
}

// Update the subject (dragged node) position during drag.
function dg_network_dragged(event) {
    event.subject.fx = event.x;
    event.subject.fy = event.y;
    if (event.subject.dragx != event.subject.x ||
        event.subject.dragy != event.subject.y) {
        event.subject.dragx = event.subject.x;
        event.subject.dragy = event.subject.y;
        if (!event.active) dg_network_forceLayout_restart();
    }
}

// Restore the target alpha so the simulation cools after dragging ends.
// Unfix the subject position now that it’s no longer being dragged.
function dg_network_dragended(event) {
    if (event.subject.dragx == event.subject.x &&
        event.subject.dragy == event.subject.y)
        return;
    if (!event.active) dg.network.forceLayout.alphaTarget(0);
    event.subject.fx = null;
    event.subject.fy = null;
    if (event.sourceEvent.type == 'mouseup') {
        dg_network_onNodeMouseDown(event.sourceEvent, event.subject);
    }
}

function dg_network_start() {
    dg.network.isRunningLayout = true;
    dg.network.tick = 0;
    $('#network-running')
                .addClass('glyphicon-animate glyphicon-refresh');
    dg.network.layers.link.selectAll('.link')
        .classed('noninteractive', false);
    dg.network.layers.node.selectAll('.node')
        .classed('noninteractive', false);
}

function dg_network_end(event) {
    $('#network-running')
                .removeClass('glyphicon-animate glyphicon-refresh');
    dg.network.layers.link.selectAll('.link')
        .classed('noninteractive', false);
    dg.network.layers.node.selectAll('.node')
        .classed('noninteractive', false);
    dg.network.isRunningLayout = false;
    dg_network_tick();
}

function dg_network_forceLayout_stop() {
    console.log("forceLayout_stop: ");
    dg.network.forceLayout.alpha(0);
}