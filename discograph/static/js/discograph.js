! function() {
    var dg = {
        version: "0.2"
    };

    function dg_color_class(d) {
        if (d.type == 'artist') {
            return dg_color_artist_class(d);
        } else {
            return dg_color_label_class(d);
        }
    }

    function dg_color_artist_class(d) {
        return 'q' + ((d.distance * 2) + 1) + '-9';
    }

    function dg_color_label_class(d) {
        return 'q' + ((d.distance * 2) + 2) + '-9';
    }
    dg.loading = {};

    function dg_loading_init() {
        var layer = d3.select('#svg')
            .append('g')
            .attr('id', 'loadingLayer')
            .attr('class', 'centered')
            .attr('transform', 'translate(' +
                dg.dimensions[0] / 2 +
                ',' +
                dg.dimensions[1] / 2 +
                ')'
            );
        dg.loading.arc = d3.arc()
            .startAngle(function(d) {
                return d.startAngle;
            })
            .endAngle(function(d) {
                return d.endAngle;
            })
            .innerRadius(function(d) {
                return d.innerRadius;
            })
            .outerRadius(function(d) {
                return d.outerRadius;
            });
        dg.loading.barHeight = 200;
        dg.loading.layer = layer;
        dg.loading.selection = layer.selectAll('path');
    }

    function dg_loading_toggle(status) {
        if (status) {
            var input = dg_loading_makeArray();
            var data = input[0],
                extent = input[1];
            $("#page-loading")
                .addClass("glyphicon-animate glyphicon-refresh");
        } else {
            var data = [],
                extent = [0, 0];
            $("#page-loading")
                .removeClass("glyphicon-animate glyphicon-refresh")
        }
        dg_loading_update(data, extent);
    }

    function dg_loading_makeArray() {
        var count = 10;
        var values = [];
        var data = [];
        for (var i = 0; i < count; i++) {
            var pair = [Math.random(), Math.random()];
            pair.sort();
            values.push(pair[0]);
            values.push(pair[1]);
            data.push({
                active: true,
                startAngle: 2 * Math.PI * Math.random(),
                endAngle: 2 * Math.PI * Math.random(),
                rotationRate: Math.random() * 10,
                targetInnerRadius: pair[0],
                targetOuterRadius: pair[1],
            });
        }
        return [data, d3.extent(values)];
    }

    function dg_loading_update(data, extent) {
        var barScale = d3.scaleLinear()
            .domain(extent)
            .range([
                dg.loading.barHeight / 4,
                dg.loading.barHeight
            ]);

        var dataSelection = dg.loading.layer.selectAll('path')
            .data(data);

        var selectionEnter = dataSelection.enter()
            .call(dg_loading_transition_enter);
        dataSelection.exit()
            .call(dg_loading_transition_exit);

        dataSelection = dg.loading.layer.selectAll('path')
            .data(data);
        dg_loading_transition_update(dataSelection, barScale);
        if (selectionEnter.size() > 0) {
            dg_loading_rotate(dataSelection);
        }
    }

    //function dg_loading_update(data, extent) {
    //    var barScale = d3.scaleLinear()
    //        .domain(extent)
    //        .range([
    //            dg.loading.barHeight / 4,
    //            dg.loading.barHeight
    //        ]);
    //    dg.loading.selection = dg.loading.layer.selectAll('path');
    //    dg.loading.selection = dg.loading.selection.data(data);
    ////        data,
    ////        function(d) { return d; });
    //    var scale = d3.scaleOrdinal(d3.schemeCategory10);
    //    var selectionEnter = dg.loading.selection
    //        .enter()
    //        .append('path')
    //        .attr('class', 'arc')
    //        .attr('d', dg.loading.arc)
    //        .attr('fill', function(d, i) {
    //            return scale(i);
    //        })
    //        .each(function(d, i) {
    //            d.innerRadius = 0;
    //            d.outerRadius = 0;
    //            d.hasTimer = false;
    //        })
    //        .merge(dg.loading.selection);
    //    var selectionExit = dg.loading.selection
    //        .exit();
    //    dg_loading_transition_update(selectionEnter, barScale);
    //    dg_loading_transition_exit(selectionExit);
    //    if (selectionEnter.size() > 0) {
    //        dg_loading_rotate(selectionEnter);
    //    }
    //}

    function dg_loading_transition_enter(selection) {
        var scale = d3.scaleOrdinal(d3.schemeCategory10);
        selection
            .append('path')
            .attr('class', 'arc')
            .attr('d', dg.loading.arc)
            .attr('fill', function(d, i) {
                return scale(i);
            })
            .each(function(d, i) {
                d.innerRadius = 0;
                d.outerRadius = 0;
                d.hasTimer = false;
            });
    }

    function dg_loading_transition_update(selection, barScale) {
        selection
            .transition()
            .duration(1000)
            .delay(function(d, i) {
                return (selection.size() - i) * 100;
            })
            .attrTween('d', function(d, i) {
                var inner = d3.interpolate(d.innerRadius, barScale(d.targetInnerRadius));
                var outer = d3.interpolate(d.outerRadius, barScale(d.targetOuterRadius));
                return function(t) {
                    d.innerRadius = inner(t);
                    d.outerRadius = outer(t);
                    return dg.loading.arc(d, i);
                };
            });
    }

    function dg_loading_transition_exit(selection) {
        return selection
            .transition()
            .duration(1000)
            .delay(function(d, i) {
                return (selection.size() - i) * 100;
            })
            .attrTween('d', function(d, i) {
                var inner = d3.interpolate(d.innerRadius, 0);
                var outer = d3.interpolate(d.outerRadius, 0);
                return function(t) {
                    d.innerRadius = inner(t);
                    d.outerRadius = outer(t);
                    return dg.loading.arc(d, i);
                };
            })
            .on('end', function(d) {
                d.active = false;
                this.remove();
            });
    }

    function dg_loading_rotate(selection) {
        selection
            .each(function(d) {
                if (d.hasTimer) {
                    return;
                }
                d.hasTimer = true;
                d.timer = d3.interval(function(elapsed) {
                    if (!d.active) {
                        console.log("stop timer");
                        d.timer.stop();
                        d.hasTimer = false;
                        d.timer = null;
                    }
                    //                console.log("element: ", element);
                    selection.attr('transform', function() {
                        var angle = elapsed * d.rotationRate;
                        if (0 < d.outerRadius) {
                            angle = angle / d.outerRadius;
                        }
                        return 'rotate(' + angle + ')';
                    });
                }, 20);
            });
    }
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
            //        .force("collide", d3.forceCollide().radius(d => Math.pow(dg_network_getOuterRadius(d) - 10, COLLIDE_RADIUS_POWER) + COLLIDE_OVERLAP).iterations(COLLIDE_ITERATIONS))
            //        .force("charge", d3.forceManyBody())
            //        .force("charge", d3.forceManyBody().strength(d => d.isIntermediate ? 0 : Math.sqrt(d.radius) * NODE_STRENGTH))
            //        .force("charge", d3.forceManyBody().strength(NODE_STRENGTH).distanceMax(DISTANCE_MAX).theta(THETA))
            .force("charge", d3.forceManyBody().strength(nodeStrength).distanceMax(DISTANCE_MAX).theta(THETA))




            //        .force("charge", d3.forceManyBody().strength(d => Math.sqrt(dg_network_getOuterRadius(d)) * NODE_STRENGTH).distanceMax(DISTANCE_MAX).theta(THETA))

            //        .force("magnetic", d3.forceMagnetic().charge(d => d.isIntermediate ? 0 : d.radius * d.distance * NODE_STRENGTH).polarity(false).strength(1.0))

            //        .force("charge", d3.forceManyBody().strength(d => Math.sqrt(d.radius) * NODE_STRENGTH))
            //        .force("gravity", d3.forceManyBody().strength(GRAVITY))
            //        .force("link", d3.forceLink().id(d => d.key).links(dg.network.pageData.links).distance(linkDistance).strength(LINK_STRENGTH).iterations(LINK_ITERATIONS))

            .force("bbox", dg_network_bbox_force)
            .on("tick", dg_network_tick)
            .on("end", dg_network_end)
            .stop();
    }

    function dg_network_startForceLayout() {
        console.log("Start D3 layout");
        var keyFunc = function(d) {
            return d.key
        }
        var nodeData = dg.network.pageData.nodes.filter(function(d) {
            return (!d.isIntermediate) && (d.pages.indexOf(dg.network.pageData.currentPage) != -1);
        })
        console.log("nodeData: ", nodeData);
        var linkData = dg.network.pageData.links.filter(function(d) {
            return (!d.isSpline) && (d.pages.indexOf(dg.network.pageData.currentPage) != -1);
        })
        console.log("linkData: ", linkData);

        dg.network.selections.halo = dg.network.layers.halo.selectAll(".node");
        dg.network.selections.halo = dg.network.selections.halo.data(nodeData, keyFunc);

        dg.network.selections.node = dg.network.layers.node.selectAll(".node");
        dg.network.selections.node = dg.network.selections.node.data(nodeData, keyFunc);

        dg.network.selections.text = dg.network.layers.text.selectAll(".node");
        dg.network.selections.text = dg.network.selections.text.data(nodeData, keyFunc);

        dg.network.selections.link = dg.network.layers.link.selectAll(".link");
        dg.network.selections.link = dg.network.selections.link.data(linkData, keyFunc);

        var clusterNodes = dg.network.pageData.nodes
            .filter(function(d) {
                return d.cluster !== undefined;
            });
        var hullGroup = d3.group(clusterNodes, function(d) {
            return d.cluster;
        }).values();
        var hullData = d3.filter(hullGroup, function(d) {
            return 1 < Array.from(d.values()).length;
        });
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
        dg.network.pageData.nodes.forEach(function(n) {
            n.fixed = false;
        });

        // Restart simulation
        console.log("Updating forceLayout");
        console.log("dg.network.pageData.nodes: ", dg.network.pageData.nodes);
        dg.network.forceLayout.nodes(dg.network.pageData.nodes);
        //    dg.network.forceLayout.force("link", d3.forceLink().id(d => d.key).links(dg.network.pageData.links).distance(linkDistance))
        //    dg.network.forceLayout.force("link", d3.forceLink().id(d => d.key).links(dg.network.pageData.links).distance(linkDistance).strength(LINK_STRENGTH))
        //    dg.network.forceLayout.force("link", d3.forceLink().id(d => d.key).links(dg.network.pageData.links).distance(linkDistance).strength(LINK_STRENGTH).iterations(LINK_ITERATIONS))

        //    dg.network.forceLayout.force("x", d3.forceX(dg.dimensions[0] / 2));
        //    dg.network.forceLayout.force("y", d3.forceY(dg.dimensions[1] / 2));

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
        console.log("dragstart: ", event);

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
        console.log("dragged: ", event);
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
        console.log("dragend: ", event);
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
            .classed('noninteractive', true);
        dg.network.layers.node.selectAll('.node')
            .classed('noninteractive', true);
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

    function dg_network_onHaloEnter(haloEnter) {
        var haloEnter = haloEnter.append("g")
            .attr("id", function(d) {
                return d.key;
            })
            .attr("class", function(d) {
                var classes = [
                    "node",
                    //                d.key,
                    d.key.split('-')[0],
                ];
                return classes.join(" ");
            })
        haloEnter.append("circle")
            .attr("class", "halo")
            .attr("r", function(d) {
                return dg_network_getOuterRadius(d) + 40;
            });
    }

    function dg_network_onHaloExit(haloExit) {
        haloExit.remove();
    }

    function dg_network_onHullEnter(hullEnter) {
        console.log("hullEnter", hullEnter);
        var hullGroup = hullEnter
            .append("g")
            .attr("class", function(d) {
                return "hull"
            });
        //        .attr("class", function(d) { return "hull hull-" + d.key });
        hullGroup.append("path");
    }

    function dg_network_onHullExit(hullExit) {
        hullExit.remove();
    }

    function dg_network_init() {
        var root = d3.select("#svg").append("g").attr("id", "networkLayer");
        dg.network.layers.root = root;
        dg.network.layers.halo = root.append("g").attr("id", "haloLayer");
        dg.network.layers.link = root.append("g").attr("id", "linkLayer");
        dg.network.layers.node = root.append("g").attr("id", "nodeLayer");
        dg.network.layers.text = root.append("g").attr("id", "textLayer");
        dg.network.selections.halo = dg.network.layers.halo.selectAll(".node");
        dg.network.selections.hull = dg.network.layers.halo.selectAll(".hull");
        dg.network.selections.link = dg.network.layers.link.selectAll(".link");
        dg.network.selections.node = dg.network.layers.node.selectAll(".node");
        dg.network.selections.text = dg.network.layers.text.selectAll(".node");
        dg.network.forceLayout = dg_network_setupForceLayout();
    }
    LINK_DEBOUNCE_TIME = 250
    LINK_OUT_TRANSITION_TIME = 500


    /* Initialize link tooltip */
    var linkToolTip = d3.tip()
        .attr('class', 'd3-link-tooltip')
        .direction('n')
        //    .rootElement(e => )
        .offset([20, 0])
        .html(dg_network_link_tooltip);

    function dg_network_onLinkEnter(linkEnter) {
        var linkEnter = linkEnter.append("g")
            .attr("id", function(d) {
                return "link-" + d.key;
            })
            .attr("class", function(d) {
                var parts = d.key.split('-');
                var role = parts.slice(2, 2 + parts.length - 4).join('-')
                var classes = [
                    "link",
                    //                "link-" + d.key,
                    role,
                ];
                return classes.join(" ");
            });
        dg_network_onLinkEnterElementConstruction(linkEnter);
        dg_network_onLinkEnterEventBindings(linkEnter);
    }

    function dg_network_onLinkEnterElementConstruction(linkEnter) {
        linkEnter
            .append("path")
            .attr("class", "inner");
        linkEnter
            .append("text")
            .attr('class', 'outer')
            .text(dg_network_linkAnnotation);
        linkEnter
            .append("text")
            .attr('class', 'inner')
            .text(dg_network_linkAnnotation);
    }

    function dg_network_onLinkEnterEventBindings(linkEnter) {
        var debounceToolTip = $.debounce(LINK_DEBOUNCE_TIME, function(self, d, status) {
            if (status) {
                //console.log("link: ", self, d);
                linkToolTip.show(d, d3.select(self).select('text').node());
            } else {
                linkToolTip.hide(d);
            }
        });
        linkEnter.on("mouseover", function(event, d) {
            d3.select(this)
                .classed("selected", true);
            debounceToolTip(this, d, true);
        });
        linkEnter.on("mouseout", function(event, d) {
            d3.select(this)
                .classed("selected", false)
                .transition()
                .duration(LINK_OUT_TRANSITION_TIME);
            debounceToolTip(this, d, false);
        });
    }

    function dg_network_link_tooltip(d) {
        var parts = [
            '<div>' + d.source.name + '</div>',
            '<div>' + d.role + '</div>',
            '<div>' + d.target.name + '</div>',
        ];
        //    var parts = [
        //        '<span class="link-label-top">' + d.source.name + '</span><br/>',
        //        '<span class="link-label-middle">' + d.role + '</span><br/>',
        //        '<span class="link-label-bottom">' + d.target.name + '</span>',
        //        ];
        return parts.join('');
    }

    //function dg_network_tooltip(d) {
    //    var topBackgroundColor = (d.source.type == 'artist') ? dg_color_heatmap(d.source) : dg_color_greyscale(d.source);
    //    var bottomBackgroundColor = (d.target.type == 'artist') ? dg_color_heatmap(d.target) : dg_color_greyscale(d.target);
    //
    //    var parts = [
    //        '<span class="link-label-top" style="border-color:' + topBackgroundColor + '">' + d.source.name + '</span><br/>',
    //        '<span class="link-label-middle">' + d.role + '</span><br/>',
    //        '<span class="link-label-bottom" style="border-color:' + bottomBackgroundColor + '">' + d.target.name + '</span>',
    //        ];
    //    return parts.join('');
    //}

    function dg_network_linkAnnotation(d) {
        return d.role.split(' ').map(function(x) {
            return x[0];
        }).join('');
    }

    function dg_network_onLinkExit(linkExit) {
        linkExit.remove();
    }

    function dg_network_onLinkUpdate(linkSelection) {}
    NODE_DEBOUNCE_TIME = 250
    NODE_OUT_TRANSITION_TIME = 500
    NODE_UPDATE_TRANSITION_TIME = 5000
    NODE_ARTIST_PALETTE = "Palette3"
    NODE_LABEL_PALETTE = "Palette4"

    /* Initialize node tooltip */
    var nodeToolTip = d3.tip()
        .attr('class', 'd3-node-tooltip')
        .direction('s')
        .offset([-20, 0])
        .html(dg_network_node_tooltip);

    function dg_network_onNodeEnter(nodeEnter) {
        var nodeEnter = nodeEnter.append("g")
            .attr("id", function(d) {
                return d.key;
            })
            .attr("class", function(d) {
                var entity_type = d.key.split('-')[0]
                var classes = [
                    "node",
                    entity_type,
                    entity_type == "artist" ? NODE_ARTIST_PALETTE : NODE_LABEL_PALETTE,
                ];
                return classes.join(" ");
            })
            .call(d3.drag()
                .on("start", dg_network_dragstarted)
                .on("drag", dg_network_dragged)
                .on("end", dg_network_dragended));
        dg_network_onNodeEnterElementConstruction(nodeEnter);
        dg_network_onNodeEnterEventBindings(nodeEnter);
    }

    function dg_network_onNodeEnterElementConstruction(nodeEnter) {
        // ARTISTS
        var artistEnter = nodeEnter.select(function(d) {
            return d.type == 'artist' ? this : null;
        });
        artistEnter
            .append("circle")
            .attr("class", "shadow")
            .attr("cx", function(d) {
                return dg_network_getOuterRadius(d) / 3 + 1;
            })
            .attr("cy", function(d) {
                return dg_network_getOuterRadius(d) / 3 + 1;
            })
            .attr("r", function(d) {
                return Math.pow(dg_network_getOuterRadius(d), 1.2) - 2;
            });
        artistEnter
            .select(function(d, i) {
                return 0 < d.size ? this : null;
            })
            .append("circle")
            .attr("class", function(d) {
                var classes = [
                    "outer",
                    dg_color_class(d),
                ];
                return classes.join(" ");
            })
            .attr("r", dg_network_getOuterRadius);
        artistEnter
            .append("circle")
            .attr("class", function(d) {
                var classes = [
                    "inner",
                    dg_color_class(d),
                ];
                return classes.join(" ");
            })
            .attr("r", dg_network_getInnerRadius);

        // LABELS
        var labelEnter = nodeEnter.select(function(d) {
            return d.type == 'label' ? this : null;
        });
        labelEnter
            .append("rect")
            .attr("class", function(d) {
                var classes = [
                    "inner",
                    dg_color_class(d),
                ];
                return classes.join(" ");
            })
            .attr("height", function(d) {
                return 2 * dg_network_getInnerRadius(d);
            })
            .attr("width", function(d) {
                return 2 * dg_network_getInnerRadius(d);
            })
            .attr("x", function(d) {
                return -1 * dg_network_getInnerRadius(d);
            })
            .attr("y", function(d) {
                return -1 * dg_network_getInnerRadius(d);
            });

        // MORE
        nodeEnter.append("path")
            .attr("class", "more")
            // Show a + symbol if there are extra links from this node that are missing / not shown
            .attr("d", d3.symbol(d3.symbolCross, 64))
            .style("opacity", function(d) {
                return d.missing > 0 ? 1 : 0;
            });
    }

    function dg_network_onNodeEnterEventBindings(nodeEnter) {
        var debounceToolTip = $.debounce(LINK_DEBOUNCE_TIME, function(self, d, status) {
            if (status) {
                nodeToolTip.show(d, d3.select(self).node());
            } else {
                nodeToolTip.hide(d);
            }
        });
        nodeEnter.on("mouseover", function(event, d) {
            dg_network_onNodeMouseOver(event, d);
            debounceToolTip(this, d, true);
        });
        nodeEnter.on("mouseout", function(event, d) {
            debounceToolTip(this, d, false);
        });
        nodeEnter.on("mousedown", function(event, d) {
            dg_network_onNodeMouseDown(event, d);
        });
        nodeEnter.on("dblclick", function(event, d) {
            dg_network_onNodeMouseDoubleClick(event, d);
        });
        nodeEnter.on("touchstart", function(event, d) {
            dg_network_onNodeTouchStart(event, d);
        });
    }

    function dg_network_onNodeExit(nodeExit) {
        nodeExit.remove();
    }

    function dg_network_onNodeUpdate(nodeUpdate) {
        nodeUpdate.selectAll(".outer")
            //        .transition()
            //        .duration(NODE_UPDATE_TRANSITION_TIME)
            .attr("class", function(d) {
                var classes = [
                    "outer",
                    dg_color_class(d),
                ];
                return classes.join(" ");
            })
        nodeUpdate.selectAll(".inner")
            //        .transition()
            //        .duration(NODE_UPDATE_TRANSITION_TIME)
            .attr("class", function(d) {
                var classes = [
                    "inner",
                    dg_color_class(d),
                ];
                return classes.join(" ");
            })
        nodeUpdate.selectAll(".more")
            .style("opacity", function(d) {
                return d.missing > 0 ? 1 : 0;
            });

        //    nodeUpdate.selectAll(".more").each(function(d, i) {
        //        var prevMissing = Boolean(d.hasMissing);
        //        var prevMissingByPage = Boolean(d.hasMissingByPage);
        //        var currMissing = Boolean(d.missing);
        //        if (!d.missingByPage) {
        //            var currMissingByPage = false;
        //        } else {
        //            var currMissingByPage = Boolean(
        //                d.missingByPage[dg.network.pageData.currentPage]
        //                );
        //        }
        //        d3.select(this)
        //            .transition()
        //            .duration(NODE_UPDATE_TRANSITION_TIME)
        //            .style('opacity', function(d) {
        //                return (currMissing || currMissingByPage) ? 1 : 0;
        //                })
        //            .attrTween('transform', function(d) {
        //                var start = prevMissingByPage ? 45 : 0;
        //                var stop = currMissingByPage ? 45 : 0;
        //                return d3.interpolateString(
        //                    "rotate(" + start + ")",
        //                    "rotate(" + stop + ")"
        //                    );
        //                });
        //        d.hasMissing = currMissing;
        //        d.hasMissingByPage = currMissingByPage;
        //    });
    }

    function dg_network_onNodeMouseOver(event, d) {
        var debounce = $.debounce(NODE_DEBOUNCE_TIME, function(self, d) {
            //console.log("node: ", d);
        });
        debounce(this, d);

        dg.network.layers.node.selectAll(".node").filter(n => {
            return n.key == d.key;
        }).raise();
        dg.network.layers.text.selectAll(".node").filter(n => {
            return n.key == d.key;
        }).raise();
    }

    function dg_network_onNodeMouseDown(event, d) {
        var thisTime = d3.now();
        var lastTime = d.lastClickTime;
        //    console.log("lastTime: ", lastTime);
        //    console.log("thisTime: ", thisTime);
        d.lastClickTime = thisTime;
        if (!lastTime || (thisTime - lastTime) < 700) {
            //    console.log("mousedown single click");
            $(window).trigger({
                type: 'discograph:select-entity',
                entityKey: d.key,
                fixed: true,
            });
        } else if ((thisTime - lastTime) < 700) {
            //    console.log("mousedown double click");
            $(window).trigger({
                type: 'discograph:request-network',
                entityKey: d.key,
                pushHistory: true,
            });
        }
        //   event.stopPropagation(); // Prevents propagation to #svg element.
    }

    function dg_network_onNodeMouseDoubleClick(event, d) {
        //    console.log("mousedown double click");
        nodeToolTip.hide(d);
        linkToolTip.hide();
        $(window).trigger({
            type: 'discograph:request-network',
            entityKey: d.key,
            pushHistory: true,
        });
        event.stopPropagation(); // Prevents propagation to #svg element.
    }

    function dg_network_onNodeTouchStart(event, d) {
        //console.log("touchstart ", d);
        var thisTime = $.now();
        var lastTime = d.lastTouchTime;
        d.lastTouchTime = thisTime;
        if (!lastTime || (500 < (thisTime - lastTime))) {
            $(window).trigger({
                type: 'discograph:select-entity',
                entityKey: d.key,
                fixed: true,
            });
        } else if ((thisTime - lastTime) < 500) {
            $(window).trigger({
                type: 'discograph:request-network',
                entityKey: d.key,
                pushHistory: true,
            });
        }
        event.stopPropagation(); // Prevents propagation to #svg element.
    }

    function dg_network_node_tooltip(d) {
        var parts = [
            '<span>' + d.name + '</span>',
        ];
        return parts.join('');
    }
    dg.network = {
        dimensions: [0, 0],
        forceLayout: null,
        isUpdating: false,
        isRunningLayout: false,
        tick: 0,
        newNodeCoords: [0, 0],
        data: {
            json: null,
            nodeMap: new Map(),
            linkMap: new Map(),
            maxDistance: 0,
            pageCount: 1,
        },
        pageData: {
            currentPage: 1,
            links: [],
            nodes: [],
            selectedNodeKey: null,
        },
        selections: {
            halo: null,
            hull: null,
            node: null,
            link: null,
            text: null,
        },
        layers: {
            root: null,
            halo: null,
            text: null,
            node: null,
            link: null,
        },
    };

    function dg_network_selectPage(page) {
        if ((page >= 1) && (page <= dg.network.data.pageCount)) {
            dg.network.pageData.currentPage = page;
        } else {
            dg.network.pageData.currentPage = 1;
        }
        var currentPage = dg.network.pageData.currentPage;
        var pageCount = dg.network.data.pageCount;
        if (currentPage == 1) {
            var prevPage = pageCount;
        } else {
            var prevPage = currentPage - 1;
        }
        var prevText = prevPage + ' / ' + pageCount;
        if (currentPage == pageCount) {
            var nextPage = 1;
        } else {
            var nextPage = currentPage + 1;
        }
        var nextText = nextPage + ' / ' + pageCount;
        $('#paging .previous-text').text(prevText);
        $('#paging .next-text').text(nextText);

        var filteredNodes = Array.from(dg.network.data.nodeMap.values()).filter(function(d) {
            return (d.pages.indexOf(currentPage) != -1);
        });
        var filteredLinks = Array.from(dg.network.data.linkMap.values()).filter(function(d) {
            return (d.pages.indexOf(currentPage) != -1);
        });
        dg.network.pageData.nodes.length = 0;
        dg.network.pageData.links.length = 0;
        Array.prototype.push.apply(dg.network.pageData.nodes, filteredNodes);
        Array.prototype.push.apply(dg.network.pageData.links, filteredLinks);
    }
    LABEL_OFFSET_Y = 9

    function dg_network_getNodeText(d) {
        var name = d.name;
        if (50 < name.length) {
            name = name.slice(0, 50) + "...";
        }
        if (dg.debug) {
            var pages = '[' + d.pages + ']';
            return pages + ' ' + name;
        }
        return name;
        //    return name + dg_network_getNodeDebug(d);
    }

    function dg_network_getNodeDebug(d) {
        var links = d.links !== undefined ? d.links.length : 0;
        return " dist: " + d.distance +
            " rad: " + d.radius +
            " lnk: " + links +
            " miss: " + d.missing +
            //           " clu: " + d.cluster +
            " col: " + dg_color_class(d);
    }


    function dg_network_onTextEnter(textEnter) {
        var textEnter = textEnter.append("g")
            .attr("id", function(d) {
                return d.key;
            })
            .attr("class", function(d) {
                var classes = [
                    "node",
                    //                d.key,
                    d.key.split('-')[0],
                ];
                return classes.join(" ");
            })
        textEnter.append("text")
            .attr("class", "outer")
            //        .attr("dx", function(d) { return dg_network_getOuterRadius(d) + 3; })
            //        .attr("dy", ".35em")
            //        .attr("dy", "1.2em")
            .attr("dy", function(d) {
                return dg_network_getOuterRadius(d) + LABEL_OFFSET_Y;
            })
            .attr("width", function(d) {
                return dg_network_getOuterRadius(d) * 3;
            })
            .text(dg_network_getNodeText);
        textEnter.append("text")
            .attr("class", "inner")
            //        .attr("dx", function(d) { return dg_network_getOuterRadius(d) + 3; })
            //        .attr("dy", ".35em")
            //        .attr("dy", "1.2em")
            .attr("dy", function(d) {
                return dg_network_getOuterRadius(d) + LABEL_OFFSET_Y;
            })
            .attr("width", function(d) {
                return dg_network_getOuterRadius(d) * 3;
            })
            .text(dg_network_getNodeText);
    }

    function dg_network_onTextExit(textExit) {
        textExit.remove();
    }

    function dg_network_onTextUpdate(textUpdate) {
        textUpdate.select('.outer').text(dg_network_getNodeText);
        textUpdate.select('.inner').text(dg_network_getNodeText);
    }

    NODE_INNER_RADIUS = 8
    NODE_OUTER_RADIUS = 11
    //NODE_INNER_RADIUS = 9
    //NODE_OUTER_RADIUS = 12

    function dg_network_getRadius(d) {
        var boost1 = d.distance === 0 ? 10 : d.distance === 1 ? 5 : 0;
        var boost2 = d.links && d.links.length >= 20 ? 10 : d.links && d.links.length >= 10 ? 5 : 0;
        var alias = (d.cluster !== undefined) ? 2 : 1;
        return Math.round(((Math.sqrt(d.size) * 2) + boost1 + boost2) / alias);
    }

    function dg_network_getOuterRadius(d) {
        return NODE_OUTER_RADIUS + dg_network_getRadius(d);
    }

    function dg_network_getInnerRadius(d) {
        return NODE_INNER_RADIUS + dg_network_getRadius(d);
    }

    function dg_network_splineInner(sX, sY, sR, cX, cY) {
        var dX = (sX - cX),
            dY = (sY - cY);
        var angle = Math.atan(dY / dX);
        dX = Math.abs(Math.cos(angle) * sR);
        dY = Math.abs(Math.sin(angle) * sR);
        sX = (sX < cX) ? sX + dX : sX - dX;
        sY = (sY < cY) ? sY + dY : sY - dY;
        return [sX, sY];
    }

    function dg_network_spline(d) {
        var sX = d.source.x;
        var sY = d.source.y;
        var tX = d.target.x;
        var tY = d.target.y;
        var tR = d.target.radius;
        var sR = d.source.radius;
        if (d.intermediate) {
            var cX = d.intermediate.x;
            var cY = d.intermediate.y;
            sXY = dg_network_splineInner(sX, sY, sR, cX, cY);
            tXY = dg_network_splineInner(tX, tY, tR, cX, cY);
            return (
                'M ' + sXY[0] + ',' + sXY[1] + ' ' +
                'S ' + cX + ',' + cY + ' ' +
                ' ' + tXY[0] + ',' + tXY[1] + ' '
            );
        } else {
            return 'M ' + [sX, sY] + ' L ' + [tX, tY];
        }
    }

    function dg_network_getHullVertices(nodes) {
        var vertices = [];
        nodes.forEach(function(d) {
            var radius = d.radius / 3;
            vertices.push([d.x + radius, d.y + radius]);
            vertices.push([d.x + radius, d.y - radius]);
            vertices.push([d.x - radius, d.y + radius]);
            vertices.push([d.x - radius, d.y - radius]);
        });
        return vertices;
    }

    var unlabeled_roles = [
        'Alias',
        'Member Of',
        'Sublabel Of',
    ];

    function dg_network_tick_link(d, i) {
        var group = d3.select(this);
        var path = group.select('path');
        path.attr('d', dg_network_spline(d));
        path.classed('distance-0', d.source.distance == 0);
        path.classed('distance-1', d.source.distance == 1);
        path.classed('distance-2', d.source.distance == 2);
        var x1 = d.source.x,
            y1 = d.source.y,
            x2 = d.target.x,
            y2 = d.target.y;
        var node = path.node();
        if (node && node.getTotalLength() > 0) {
            var point = node.getPointAtLength(node.getTotalLength() / 2);
            var angle = Math.atan2((y2 - y1), (x2 - x1)) * (180 / Math.PI);
            var text = group.selectAll('text')
                .attr('transform', [
                    'rotate(' + angle + ' ' + point.x + ' ' + point.y + ')',
                    'translate(' + point.x + ',' + point.y + ')',
                ].join(' '));
        }
    }

    function dg_network_translate(d) {
        return 'translate(' + d.x + ',' + d.y + ')';
    }

    function dg_network_tick(e) {
        dg.network.tick += 1;
        console.log("tick: ", dg.network.tick);
        var k = 1.0; //e.alpha * 5;
        if (dg.network.data.json) {
            var centerNode = dg.network.data.nodeMap.get(dg.network.data.json.center.key);
            if (!centerNode.fixed) {
                var dx = ((dg.dimensions[0] / 2) - centerNode.x) * k;
                var dy = ((dg.dimensions[1] / 2) - centerNode.y) * k;
                centerNode.x += dx;
                centerNode.y += dy;
            }
        }
        dg.network.layers.link
            .selectAll(".link")
            .each(dg_network_tick_link);
        dg.network.layers.halo
            .selectAll(".node")
            .attr('transform', dg_network_translate);
        dg.network.layers.node
            .selectAll(".node")
            .attr('transform', dg_network_translate);
        dg.network.layers.text
            .selectAll(".node")
            .attr('transform', dg_network_translate);
        dg.network.layers.halo
            .selectAll(".hull")
            .select('path')
            .attr('d', function(d) {
                var vertices = d3.polygonHull(dg_network_getHullVertices(d.flat()));
                return 'M' + vertices.join('L') + 'Z';
            });

    }

    function dg_svg_init() {
        // Setup window dimensions on SVG element
        dg_svg_set_size();

        // Setup SVG common definitions
        dg_svg_setupDefs();

        // Initialise tooltip
        d3.select('#svg')
            .call(nodeToolTip)
            .call(linkToolTip);
    }

    function dg_svg_set_size() {
        // Setup window dimensions on SVG element
        d3.select("#svg")
            .attr("width", dg.dimensions[0])
            .attr("height", dg.dimensions[1]);
    }

    function dg_svg_setupDefs() {
        var defs = d3.select("#svg").append("defs");
        // ARROWHEAD
        defs.append("marker")
            .attr("id", "arrowhead")
            .attr("viewBox", "-5 -5 10 10")
            .attr("refX", 4)
            .attr("refY", 0)
            .attr("markerWidth", 5)
            .attr("markerHeight", 5)
            .attr("markerUnits", "strokeWidth")
            .attr("orient", "auto")
            .append("path")
            .attr("d", "M 0,0 m -5,-5 L 5,0 L -5,5 L -2.5,0 L -5,-5 Z")
            .attr("stroke-linecap", "round")
            .attr("stroke-linejoin", "round");
        // AGGREGATE
        defs.append("marker")
            .attr("id", "aggregate")
            .attr("viewBox", "-5 -5 10 10")
            .attr("refX", 5)
            .attr("refY", 0)
            .attr("markerWidth", 5)
            .attr("markerHeight", 5)
            .attr("markerUnits", "strokeWidth")
            .attr("orient", "auto")
            .append("path")
            .attr("d", "M 0,0 m 5,0 L 0,-3 L -5,0 L 0,3 L 5,0 Z")
            .attr("fill", "#fff")
            .attr("stroke", "#000")
            .attr("stroke-linecap", "round")
            .attr("stroke-linejoin", "round")
            .attr("stroke-width", 1.5);
        // RADIAL GRADIENT
        var gradient = defs.append('radialGradient')
            .attr('id', 'radial-gradient');
        gradient.append('stop')
            .attr('offset', '0%')
            .attr('stop-color', '#333')
            .attr('stop-opacity', '1.0');
        gradient.append('stop')
            .attr('offset', '50%')
            .attr('stop-color', '#333')
            .attr('stop-opacity', '0.333');
        gradient.append('stop')
            .attr('offset', '75%')
            .attr('stop-color', '#333')
            .attr('stop-opacity', '0.111');
        gradient.append('stop')
            .attr('offset', '100%')
            .attr('stop-color', '#333')
            .attr('stop-opacity', '0.0');
    }

    function dg_svg_print(width, height) {
        var svgNode = d3.select("#svg").node();
        console.log("SVG node: ", svgNode);

        var svgString = dg_svg_getSVGString(svgNode);
        console.log("SVG str: ", svgString);
        dg_svg_string2Image(svgString, 2 * width, 2 * height, 'png', saveBlob); // passes Blob and filesize String to the callback

        function saveBlob(dataBlob, filesize) {
            console.log("Save SVG blob");
            // Call FileSaver.js function
            saveAs(dataBlob, 'Discograph2 exported to PNG.png');
        }
    }

    function dg_svg_getSVGString(svgNode) {
        svgNode.setAttribute('xlink', 'http://www.w3.org/1999/xlink');
        var cssStyleText = getCSSStyles(svgNode);
        console.log("final cssStyleText: ", cssStyleText);
        appendCSS(cssStyleText, svgNode);

        var serializer = new XMLSerializer();
        var svgString = serializer.serializeToString(svgNode);
        svgString = svgString.replace(/(\w+)?:?xlink=/g, 'xmlns:xlink='); // Fix root xlink without namespace
        svgString = svgString.replace(/NS\d+:href/g, 'xlink:href'); // Safari NS namespace fix

        return svgString;

        function getCSSStyles(parentElement) {
            var selectorTextArr = [];

            // Add Parent element Id and Classes to the list
            selectorTextArr.push('#' + parentElement.id);
            for (var c = 0; c < parentElement.classList.length; c++)
                if (!contains('.' + parentElement.classList[c], selectorTextArr))
                    selectorTextArr.push('.' + parentElement.classList[c]);
            console.log("selectorTextArr: ", selectorTextArr);
            // Add Children element Ids and Classes to the list
            var nodes = parentElement.getElementsByTagName("*");
            for (var i = 0; i < nodes.length; i++) {
                var id = nodes[i].id;
                if (!contains('#' + id, selectorTextArr))
                    selectorTextArr.push('#' + id);

                var classes = nodes[i].classList;
                //			console.log("nodes[i]: ", nodes[i]);

                // .nodeClass
                for (var c = 0; c < classes.length; c++) {
                    var selector = '.' + classes[c];
                    if (!contains(selector, selectorTextArr)) {
                        selectorTextArr.push(selector);
                        console.log("add1: ", selector);
                    }
                    // nodeName.nodeClass
                    if (nodes[i].nodeName) {
                        var selector = nodes[i].nodeName + '.' + classes[c];
                        if (!contains(selector, selectorTextArr)) {
                            selectorTextArr.push(selector);
                            console.log("add2: ", selector);
                        }
                    }
                }

                // #parent .nodeClass
                for (var c = 0; c < classes.length; c++) {
                    var parentId = nodes[i].parentNode.id;
                    var selector = '#' + parentId + ' .' + classes[c];
                    if (parentId && !contains(selector, selectorTextArr)) {
                        selectorTextArr.push(selector);
                        console.log("add3: ", selector);
                    }
                    // #parent nodeName.nodeClass
                    if (nodes[i].nodeName) {
                        var selector = '#' + parentId + " " + nodes[i].nodeName + '.' + classes[c];
                        if (!contains(selector, selectorTextArr)) {
                            selectorTextArr.push(selector);
                            console.log("add4: ", selector);
                        }
                    }
                }

                // #parent's parent .nodeClass
                for (var c = 0; c < classes.length; c++) {
                    var parentNode = nodes[i].parentNode
                    if (parentNode) {
                        var parentId = parentNode.parentNode.id;
                        var selector = '#' + parentId + ' .' + classes[c];
                        if (parentId && !contains(selector, selectorTextArr)) {
                            selectorTextArr.push(selector);
                            console.log("add5: ", selector);
                        }
                        if (nodes[i].nodeName) {
                            var selector = '#' + parentId + " " + nodes[i].nodeName + '.' + classes[c];
                            if (!contains(selector, selectorTextArr)) {
                                selectorTextArr.push(selector);
                                console.log("add6: ", selector);
                            }
                        }
                    }
                }

                for (var c = 0; c < classes.length; c++) {
                    var parentNode = nodes[i].parentNode
                    var parentId = parentNode.id;
                    if (parentNode) {
                        var parentClasses = parentNode.classList;
                        var parentParentId = parentNode.parentNode.id;

                        for (var pc = 0; pc < parentClasses.length; pc++) {
                            // No nodeClass
                            var selector = '.' + parentClasses[pc];
                            if (!contains(selector, selectorTextArr)) {
                                selectorTextArr.push(selector);
                                console.log("add7: ", selector);
                            }
                            // No nodeClass + nodeName
                            if (nodes[i].nodeName) {
                                var selector = '.' + parentClasses[pc] + ' ' + nodes[i].nodeName;
                                if (!contains(selector, selectorTextArr)) {
                                    selectorTextArr.push(selector);
                                    console.log("add8: ", selector);
                                }
                            }

                            // Parent class + nodeClass
                            var selector = '.' + parentClasses[pc] + ' .' + classes[c];
                            if (!contains(selector, selectorTextArr)) {
                                selectorTextArr.push(selector);
                                console.log("add9: ", selector);
                            }
                            // Parent class + nodeName.nodeClass
                            if (nodes[i].nodeName) {
                                var selector = '.' + parentClasses[pc] + ' ' + nodes[i].nodeName + '.' + classes[c];
                                if (!contains(selector, selectorTextArr)) {
                                    selectorTextArr.push(selector);
                                    console.log("add10: ", selector);
                                }
                            }

                            // ParentParentID + Parent class
                            selector = '#' + parentParentId + ' .' + parentClasses[pc];
                            if (parentParentId && !contains(selector, selectorTextArr)) {
                                selectorTextArr.push(selector);
                                console.log("add11: ", selector);
                            }
                            // ParentParentID + Parent class + nodeName
                            if (parentParentId && nodes[i].nodeName) {
                                var selector = '#' + parentParentId + ' .' + parentClasses[pc] + ' ' + nodes[i].nodeName;
                                if (!contains(selector, selectorTextArr)) {
                                    selectorTextArr.push(selector);
                                    console.log("add12: ", selector);
                                }
                            }

                            // ParentParentID + Parent class + nodeClass
                            selector = '#' + parentParentId + ' .' + parentClasses[pc] + ' .' + classes[c];
                            if (parentParentId && !contains(selector, selectorTextArr)) {
                                selectorTextArr.push(selector);
                                console.log("add11: ", selector);
                            }
                            // ParentParentID + Parent class + nodeName.nodeClass
                            if (parentParentId && nodes[i].nodeName) {
                                var selector = '#' + parentParentId + ' .' + parentClasses[pc] + ' ' + nodes[i].nodeName + '.' + classes[c];
                                if (!contains(selector, selectorTextArr)) {
                                    selectorTextArr.push(selector);
                                    console.log("add12: ", selector);
                                }
                            }

                            // ParentParentParentID + Parent class
                            if (parentNode.parentNode) {
                                var parentParentParentId = parentNode.parentNode.parentNode.id;
                                selector = '#' + parentParentParentId + ' .' + parentClasses[pc];
                                if (parentParentParentId && !contains(selector, selectorTextArr)) {
                                    selectorTextArr.push(selector);
                                    console.log("add11: ", selector);
                                }
                                if (parentParentParentId && nodes[i].nodeName) {
                                    var selector = '#' + parentParentParentId + ' .' + parentClasses[pc] + ' ' + nodes[i].nodeName;
                                    if (!contains(selector, selectorTextArr)) {
                                        selectorTextArr.push(selector);
                                        console.log("add12: ", selector);
                                    }
                                }
                            }

                            // ParentParentParentID + Parent class + nodeClass
                            if (parentNode.parentNode) {
                                var parentParentParentId = parentNode.parentNode.parentNode.id;
                                selector = '#' + parentParentParentId + ' .' + parentClasses[pc] + ' .' + classes[c];
                                if (parentParentParentId && !contains(selector, selectorTextArr)) {
                                    selectorTextArr.push(selector);
                                    console.log("add11: ", selector);
                                }
                                if (parentParentParentId && nodes[i].nodeName) {
                                    var selector = '#' + parentParentParentId + ' .' + parentClasses[pc] + ' ' + nodes[i].nodeName + '.' + classes[c];
                                    if (!contains(selector, selectorTextArr)) {
                                        selectorTextArr.push(selector);
                                        console.log("add12: ", selector);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            console.log("selectorTextArr: ", selectorTextArr);

            // Extract CSS Rules
            var extractedCSSText = "";
            for (var i = 0; i < document.styleSheets.length; i++) {
                var s = document.styleSheets[i];

                try {
                    if (!s.cssRules)
                        continue;
                } catch (e) {
                    if (e.name !== 'SecurityError') throw e; // for Firefox
                    continue;
                }

                var cssRules = s.cssRules;

                for (var r = 0; r < cssRules.length; r++) {
                    //console.log("cssRules[r].selectorText: ", cssRules[r].selectorText);
                    if (contains(cssRules[r].selectorText, selectorTextArr)) {
                        extractedCSSText += cssRules[r].cssText + "\n";
                        console.log("add cssRules[r].cssText: ", cssRules[r].cssText);
                        //console.log("extractedCSSText: ", extractedCSSText);
                    }
                }
            }

            return extractedCSSText;

            function contains(str, arr) {
                return arr.indexOf(str) === -1 ? false : true;
            }
        }

        function appendCSS(cssText, element) {
            var styleElement = document.createElement("style");
            styleElement.setAttribute("type", "text/css");
            styleElement.innerHTML = cssText;
            var refNode = element.hasChildNodes() ? element.children[0] : null;
            element.insertBefore(styleElement, refNode);
        }
    }

    function dg_svg_string2Image(svgString, width, height, format, callback) {
        var format = format ? format : 'png';

        // Convert SVG string to data URL
        var imgsrc = 'data:image/svg+xml;base64,' + btoa(unescape(encodeURIComponent(svgString)));

        var canvas = document.createElement("canvas");
        var context = canvas.getContext("2d");

        canvas.width = width;
        canvas.height = height;

        var image = new Image();
        image.onload = function() {
            context.clearRect(0, 0, width, height);
            context.drawImage(image, 0, 0, width, height);

            canvas.toBlob(function(blob) {
                var filesize = Math.round(blob.length / 1024) + ' KB';
                if (callback) callback(blob, filesize);
            });
        };

        image.src = imgsrc;
    }
    dg.relations = {
        layers: {
            root: null,
        },
    };

    function dg_relations_init() {
        dg.relations.layers.root = d3.select('#svg').append('g')
            .attr('id', 'relationsLayer');
    }

    function dg_relations_chartRadial() {
        var textAnchor = function(d, i) {
            var angle = (i + 0.5) / numBars;
            if (angle < 0.5) {
                return 'start';
            } else {
                return 'end';
            }
        };
        var transform = function(d, i) {
            var hypotenuse = barScale(d.values) + 5;
            var angle = (i + 0.5) / numBars;
            var degrees = (angle * 360);
            if (180 <= degrees) {
                degrees -= 180;
            }
            degrees -= 90;
            var radians = angle * 2 * Math.PI;
            var x = Math.sin(radians) * hypotenuse;
            var y = -Math.cos(radians) * hypotenuse;
            return [
                'rotate(' + degrees + ',' + x + ',' + y + ')',
                'translate(' + x + ',' + y + ')'
            ].join(' ');
        }
        dg.relations.layers.root = d3.select('#svg')
            .append('g')
            .attr('id', 'relationsLayer');
        var barHeight = d3.min(dg.dimensions) / 3;
        var data = dg.relations.byRole;
        var extent = d3.extent(data, function(d) {
            return d.values;
        });
        var barScale = d3.scaleSqrt()
            .exponent(0.25)
            .domain(extent)
            .range([barHeight / 4, barHeight]);
        var keys = data.map(function(d, i) {
            return d.key;
        });
        var numBars = keys.size();
        var arc = d3.arc()
            .startAngle(function(d, i) {
                return (i * 2 * Math.PI) / numBars;
            })
            .endAngle(function(d, i) {
                return ((i + 1) * 2 * Math.PI) / numBars;
            })
            .innerRadius(0);
        dg.arc = arc;
        var radialGroup = dg.relations.layers.root.append('g')
            .attr('class', 'radial centered')
            .attr('transform', 'translate(' +
                (dg.dimensions[0] / 2) +
                ',' +
                (dg.dimensions[1] / 2) +
                ')'
            );
        var selectedRoles = $('#filter select').val();
        var segments = radialGroup.selectAll('g')
            .data(data)
            .enter().append('g')
            .attr('class', 'segment')
            .classed('selected', function(d) {
                return selectedRoles.indexOf(d.key) != -1;
            })
            .on('mouseover', function(event) {
                d3.select(this).raise();
            });
        var arcs = segments.append('path')
            .attr('class', 'arc')
            .attr('d', arc)
            .each(function(d) {
                d.outerRadius = 0;
            })
            .on('mousedown', function(event, d) {
                var values = $('#filter-roles').val();
                values.push(d.key);
                $('#filter-roles').val(values).trigger('change');
                dg.fsm.requestNetwork(dg.network.data.json.center.key, true);
                event.stopPropagation();
            })
            .transition()
            .ease('elastic')
            .duration(500)
            .delay(function(d, i) {
                return (numBars - i) * 25;
            })
            .attrTween('d', function(d, index) {
                var i = d3.interpolate(d.outerRadius, barScale(+d.values));
                return function(t) {
                    d.outerRadius = i(t);
                    return arc(d, index);
                };
            });
        var outerLabels = segments.append('text')
            .attr('class', 'outer')
            .attr('text-anchor', textAnchor)
            .attr('transform', transform)
            .text(function(d) {
                return d.key;
            });
        var innerLabels = segments.append('text')
            .attr('class', 'inner')
            .attr('text-anchor', textAnchor)
            .attr('transform', transform)
            .text(function(d) {
                return d.key;
            });
    }

    function dg_typeahead_init() {
        var dg_typeahead_bloodhound = new Bloodhound({
            datumTokenizer: Bloodhound.tokenizers.whitespace,
            queryTokenizer: Bloodhound.tokenizers.whitespace,
            remote: {
                url: "/api/search/%QUERY",
                wildcard: "%QUERY",
                filter: function(response) {
                    return response.results;
                },
                rateLimitBy: 'debounce',
                rateLimitWait: 300,
            },
        });
        var inputElement = $("#typeahead");
        var loadingElement = $("#search .loading");
        inputElement.typeahead({
                hint: true,
                highlight: true,
                minLength: 2,
            }, {
                name: "results",
                display: "name",
                limit: 20,
                source: dg_typeahead_bloodhound,
                templates: {
                    suggestion: function(data) {
                        return '<div>' +
                            '<span>' + data.name + '</span>' +
                            ' <em>(' + data.key.split('-')[0] + ')</em></div>';
                    },
                },
            })
            .on('keydown', function(event) {
                if (event.keyCode == 13) {
                    event.preventDefault();
                    dg_typeahead_navigate();
                } else if (event.keyCode == 27) {
                    inputElement.typeahead("close");
                }
            })
            .on("typeahead:asynccancel typeahead:asyncreceive", function(obj, datum) {
                loadingElement.addClass("invisible");
            })
            .on("typeahead:asyncrequest", function(obj, datum) {
                loadingElement.removeClass("invisible");
            })
            .on("typeahead:autocomplete", function(obj, datum) {
                $(this).data("selectedKey", datum.key);
            })
            .on("typeahead:render", function(event, suggestion, async, name) {
                if (suggestion !== undefined) {
                    $(this).data("selectedKey", suggestion.key);
                } else {
                    $(this).data("selectedKey", null);
                }
            })
            .on("typeahead:selected", function(obj, datum) {
                $(this).data("selectedKey", datum.key);
                dg_typeahead_navigate();
            });
        $('#search .clear').on("click", function(event) {
            $('#typeahead').typeahead('val', '');
        });
    }

    function dg_typeahead_navigate() {
        var datum = $("#typeahead").data("selectedKey");
        if (datum) {
            $("#typeahead").typeahead("close");
            $("#typeahead").blur();
            $('.navbar-toggle').click();
            $(window).trigger({
                type: 'discograph:request-network',
                entityKey: datum,
                pushHistory: true,
            });
        };
    }
    var DiscographFsm = machina.Fsm.extend({
        initialize: function(options) {
            var self = this;
            $(window).on('discograph:request-network', function(event) {
                self.requestNetwork(event.entityKey, event.pushHistory);
            });
            $(window).on('discograph:request-random', function(event) {
                self.requestRandom();
            });
            $(window).on('discograph:select-entity', function(event) {
                self.selectEntity(event.entityKey, event.fixed);
            });
            $(window).on('discograph:select-next-page', function(event) {
                self.selectNextPage();
            });
            $(window).on('discograph:select-previous-page', function(event) {
                self.selectPreviousPage();
            });
            $(window).on('discograph:show-network', function(event) {
                self.showNetwork();
            });
            $(window).on('discograph:show-radial', function(event) {
                self.showRadial();
            });
            $(window).on('select2:selecting', function(event) {
                self.rolesBackup = $('#filter select').val();
            });
            $(window).on('select2:unselecting', function(event) {
                self.rolesBackup = $('#filter select').val();
            });
            window.onpopstate = function(event) {
                if (!event || !event.state || !event.state.key) {
                    return;
                }
                var entityKey = event.state.key;
                var entityType = entityKey.split("-")[0];
                var entityId = entityKey.split("-")[1];
                var url = "/" + entityType + "/" + entityId;
                // ### TODO setup analytics ga('send', 'pageview', url);
                // ### TODO setup analytics ga('set', 'page', url);
                $(window).trigger({
                    type: 'discograph:request-network',
                    entityKey: event.state.key,
                    pushHistory: false,
                });
            };
            $(window).on('resize', $.debounce(100, function(event) {
                dg_window_init();
                dg_svg_container_setup();
                dg_svg_set_size();

                var transform = [
                    'translate(',
                    (dg.dimensions[0] / 2),
                    ',',
                    (dg.dimensions[1] / 2),
                    ')'
                ].join('');
                d3.selectAll('.centered')
                    .transition()
                    .duration(250)
                    .attr('transform', transform);

                if (self.state == 'viewing-network') {
                    console.log("start d3 layout");
                    //                dg_network_processJson(dg.network.data.json);
                    //                dg_network_selectPage(1);
                    //                dg_network_startForceLayout();
                    dg_network_forceLayout_restart(ALPHA / 10.0);
                }
            }));
            $('#svg').on('mousedown', function(event) {
                if (self.state == 'viewing-network') {
                    self.selectEntity(null);
                } else if (self.state == 'viewing-radial') {
                    self.showNetwork();
                }
            });
            self.on("*", function(event, data) {
                // console.log("FSM: ", event, data);
            });
            this.loadInlineData();
            this.toggleRadial(false);
            self.rolesBackup = $('#filter select').val();
        },
        namespace: 'discograph',
        initialState: 'uninitialized',
        states: {
            'uninitialized': {
                'request-network': function(entityKey) {
                    console.log("UNINITIALIZED request-network");
                    this.requestNetwork(entityKey);
                },
                'request-random': function() {
                    this.requestRandom();
                },
                'load-inline-data': function() {
                    console.log("UNINITIALIZED load-inline-data");
                    var params = {
                        'roles': $('#filter select').val()
                    };
                    //                this.handle("received-network", dgData, false, params);
                    this.deferAndTransition("requesting");
                    this.handle("received-network", dgData, false, params);
                    console.log("load-inline-data end");
                },
            },
            'viewing-network': {
                '_onEnter': function() {
                    console.log("VIEWING-NETWORK _onEnter");
                    this.toggleNetwork(true);
                },
                '_onExit': function() {
                    console.log("VIEWING-NETWORK _onExit");
                    this.toggleNetwork(false);
                },
                'request-network': function(entityKey) {
                    console.log("VIEWING-NETWORK request-network");
                    this.requestNetwork(entityKey);
                },
                'request-random': function() {
                    this.requestRandom();
                },
                'show-radial': function() {
                    if (dg.network.pageData.selectedNodeKey) {
                        this.requestRadial(dg.network.pageData.selectedNodeKey);
                    }
                },
                'select-entity': function(entityKey, fixed) {
                    console.log("VIEWING-NETWORK select-entity", entityKey, fixed);
                    dg.network.pageData.selectedNodeKey = entityKey;
                    if (entityKey !== null) {
                        var selectedNode = dg.network.data.nodeMap.get(entityKey);
                        var currentPage = dg.network.pageData.currentPage;
                        if (selectedNode.pages.indexOf(currentPage) == -1) {
                            dg.network.pageData.selectedNodeKey = null;
                        }
                    }
                    entityKey = dg.network.pageData.selectedNodeKey;
                    if (entityKey !== null) {
                        var nodeOn = dg.network.layers.root.selectAll('#' + entityKey);
                        var nodeOff = dg.network.layers.root.selectAll('.node:not(#' + entityKey + ')');
                        //                    var nodeOn = dg.network.layers.root.selectAll('.' + entityKey);
                        //                    var nodeOff = dg.network.layers.root.selectAll('.node:not(.' + entityKey + ')');
                        var linkKeys = nodeOn.datum().links;
                        var linkOn = dg.network.selections.link.filter(function(d) {
                            return linkKeys.indexOf(d.key) >= 0;
                        });
                        var linkOff = dg.network.selections.link.filter(function(d) {
                            return linkKeys.indexOf(d.key) == -1;
                        });
                        var node = dg.network.data.nodeMap.get(entityKey);
                        var url = 'http://discogs.com/' + node.type + '/' + node.id;
                        $('#entity-name').text(node.name);
                        $('#entity-link').attr('href', url);
                        $('#entity-details').removeClass('hidden').show(0);
                        $('#navbar-title').text(node.name);
                        nodeOn.raise();
                        nodeOn.classed('selected', true);
                        if (fixed) {
                            //nodeOn.each(function(d) { d.fixed = true; });
                            node.fixed = true;
                        }
                        // linkOn.classed('selected', true);
                    } else {
                        var nodeOff = dg.network.layers.root.selectAll('.node');
                        var linkOff = dg.network.selections.link;
                        $('#entity-details').hide();
                    }
                    if (nodeOff) {
                        nodeOff.classed('selected', false);
                        nodeOff.each(function(d) {
                            d.fixed = false;
                        });
                    }
                    if (linkOff) {
                        linkOff.classed('selected', false);
                    }
                },
            },
            'viewing-radial': {
                '_onEnter': function() {
                    this.toggleRadial(true);
                    d3.select('#relationsLayer').remove();
                    dg_relations_chartRadial();
                },
                '_onExit': function() {
                    d3.select('#relationsLayer').remove();
                    this.toggleRadial(false);
                },
                'request-network': function(entityKey) {
                    this.requestNetwork(entityKey);
                },
                'request-random': function() {
                    this.requestRandom();
                },
                'show-network': function() {
                    this.transition('viewing-network');
                },
            },
            'requesting': {
                '_onEnter': function(fsm, entityKey, pushHistory) {
                    console.log("REQUESTING _onEnter");
                    this.toggleLoading(true);
                    this.toggleFilter(false);
                },
                '_onExit': function() {
                    console.log("REQUESTING _onExit");
                    this.toggleLoading(false);
                    this.toggleFilter(true);
                },
                'errored': function(error) {
                    this.handleError(error);
                },
                'received-network': function(data, pushHistory, params) {
                    console.log("REQUESTING received network", data);
                    var params = {
                        'roles': $('#filter select').val()
                    };
                    var entityKey = data.center.key;
                    dg.network.data.json = JSON.parse(JSON.stringify(data));
                    document.title = 'Discograph2: ' + data.center.name;
                    $(document).attr('body').id = entityKey;
                    if (pushHistory === true) {
                        this.pushState(entityKey, params);
                    }
                    dg.network.data.pageCount = data.pages;
                    dg.network.pageData.currentPage = 1;
                    if (data.pages > 1) {
                        $('#paging').fadeIn();
                    } else {
                        $('#paging').fadeOut();
                    }
                    console.log("received-network dg_network_processJson");
                    dg_network_processJson(data);
                    console.log("received-network dg_network_selectPage");
                    dg_network_selectPage(1);
                    console.log("received-network dg_network_startForceLayout");
                    dg_network_startForceLayout();
                    //                this.selectEntity(dg.network.data.json.center.key, false);
                    this.deferAndTransition('viewing-network');
                    this.selectEntity(dg.network.data.json.center.key, false);

                },
                'received-random': function(data) {
                    this.requestNetwork(data.center, true);
                },
                'received-radial': function(data) {
                    dg.relations.data = data;
                    dg.relations.byYear = d3.group(data.results,
                        function(d) {
                            return d.year;
                        },
                        function(d) {
                            return d.category;
                        });
                    //                    .nest()
                    //                    .key(function(d) { return d.year; })
                    //                    .key(function(d) { return d.category; })
                    //                    .entries(data.results);
                    dg.relations.byRole = d3.group(dg.relations.data.results, function(d) {
                            return d.role;
                        })
                        .groupSort(d3.ascending)
                        .rollup(function(leaves) {
                            return leaves.length;
                        });
                    //                    .nest()
                    //                    .key(function(d) { return d.role; })
                    //                    .sortKeys(d3.ascending)
                    //                    .rollup(function(leaves) { return leaves.length; })
                    //                    .entries(dg.relations.data.results);
                    this.transition('viewing-radial');
                },
            },
        },
        handleError: function(error) {
            var message = 'Something went wrong!';
            var status = error.status;
            if (status == 0) {
                status = 404;
            } else if (status == 429) {
                message = 'Hey, slow down, buddy. Give it a minute.'
            }
            var text = [
                '<div class="alert alert-danger alert-dismissible" role="alert">',
                '<button type="button" class="close" data-dismiss="alert" aria-label="Close">',
                '<span aria-hidden="true">&times;</span>',
                '</button>',
                '<strong>' + status + '!</strong> ' + message,
                '</div>'
            ].join('');
            $('#flash').append(text);
            $('#filter select').val(this.rolesBackup).trigger('change');
            this.transition('viewing-network');
        },
        getNetworkURL: function(entityKey) {
            var entityType = entityKey.split('-')[0];
            var entityId = entityKey.split('-')[1];
            var url = '/api/' + entityType + '/network/' + entityId;
            var params = {
                'roles': $('#filter select').val()
            };
            if (params.roles) {
                url += '?' + decodeURIComponent($.param(params));
            }
            return url;
        },
        getRandomURL: function() {
            var url = '/api/random?r=' + Math.floor(Math.random() * 1000000);
            var params = {
                'roles': $('#filter select').val()
            };
            if (params.roles) {
                url += '&' + decodeURIComponent($.param(params));
            }
            return url;
        },
        getRadialURL: function(entityKey) {
            var entityType = entityKey.split("-")[0];
            var entityId = entityKey.split("-")[1];
            return '/api/' + entityType + '/relations/' + entityId;
        },
        loadInlineData: function() {
            if (dgData) {
                this.handle('load-inline-data');
            }
        },
        pushState: function(entityKey, params) {
            console.log("pushstate");
            var entityType = entityKey.split("-")[0];
            var entityId = entityKey.split("-")[1];
            var title = document.title;
            var url = "/" + entityType + "/" + entityId;
            if (params) {
                url += "?" + decodeURIComponent($.param(params));
            }
            var state = {
                key: entityKey,
                params: params
            };
            window.history.pushState(state, title, url);
            // ### TODO setup analytics ga('send', 'pageview', url);
            // ### TODO setup analytics ga('set', 'page', url);
        },
        requestNetwork: function(entityKey, pushHistory) {
            console.log("requestNetwork");
            this.transition('requesting');
            var self = this;
            d3.json(this.getNetworkURL(entityKey))
                .then(function(data) {
                    self.handle('received-network', data, pushHistory);
                })
                .catch(function(error) {
                    self.handleError(error);
                });
        },
        requestRadial: function(entityKey) {
            this.transition('requesting');
            var self = this;
            d3.json(this.getRadialURL(entityKey))
                .then(function(data) {
                    self.handle('received-radial', data);
                })
                .catch(function(error) {
                    self.handleError(error);
                });
        },
        requestRandom: function() {
            this.transition('requesting');
            var self = this;
            d3.json(this.getRandomURL())
                .then(function(data) {
                    self.handle('received-random', data);
                })
                .catch(function(error) {
                    self.handleError(error);
                });
        },
        selectEntity: function(entityKey, fixed) {
            this.handle('select-entity', entityKey, fixed);
        },
        selectNextPage: function() {
            var page = dg.network.pageData.currentPage + 1;
            if (dg.network.data.pageCount < page) {
                page = 1;
            }
            this.selectPage(page);
        },
        selectPreviousPage: function() {
            var page = dg.network.pageData.currentPage - 1;
            if (page == 0) {
                page = dg.network.data.pageCount;
            }
            this.selectPage(page);
        },
        selectPage: function(page) {
            console.log("selectPage");
            dg_network_selectPage(page);
            dg_network_startForceLayout();
            this.selectEntity(dg.network.pageData.selectedNodeKey, true);
        },
        showNetwork: function() {
            this.handle('show-network');
        },
        showRadial: function() {
            this.handle('show-radial');
        },
        toggleFilter: function(status) {
            if (status) {
                $('#filter-roles').attr('disabled', false);
            } else {
                $('#filter-roles').attr('disabled', true);
            }
        },
        toggleNetwork: function(status) {
            console.log("toggleNetwork: ", status);
            if (status) {
                if (1 < dg.network.data.json.pages) {
                    $('#paging').fadeIn();
                } else {
                    $('#paging').fadeOut();
                }
                console.log("dg.network.layers.root: ", dg.network.layers.root);
                dg.network.layers.root
                    .transition()
                    .duration(250)
                    .style('opacity', 1);
                //                .on('end', function(d, i) {
                //                    dg.network.layers.link.selectAll('.link')
                //                        .classed('noninteractive', false);
                //                    dg.network.layers.node.selectAll('.node')
                //                        .classed('noninteractive', false);
                ////                    console.log("toggleNetwork forceLayout start");
                ////                    dg.network.forceLayout.restart()
                //                });
            } else {
                $('#paging').fadeOut();
                console.log("toggleNetwork: stop");
                dg.network.forceLayout.stop()
                dg.network.layers.root
                    .transition()
                    .duration(250)
                    .style('opacity', 0.25);
                //            dg.network.layers.link.selectAll('.link')
                //                .classed('noninteractive', true);
                //            dg.network.layers.node.selectAll('.node')
                //                .classed('noninteractive', true);
            }
        },
        toggleLoading: function(status) {
            if (status) {
                var input = dg_loading_makeArray();
                var data = input[0],
                    extent = input[1];
                $('#page-loading')
                    .addClass('glyphicon-animate glyphicon-refresh');
            } else {
                var data = [],
                    extent = [0, 0];
                $('#page-loading')
                    .removeClass('glyphicon-animate glyphicon-refresh')
            }
            dg_loading_update(data, extent);
        },
        toggleRadial: function(status) {
            var self = this;
            if (status) {
                $('#entity-relations')
                    .off('click')
                    .on('click', function(event) {
                        self.showNetwork();
                        event.preventDefault();
                    });
                $('#entity-relations .glyphicon')
                    .removeClass('glyphicon-eye-open')
                    .addClass('glyphicon-eye-close');
            } else {
                $('#entity-relations')
                    .off('click')
                    .on('click', function(event) {
                        self.showRadial();
                        event.preventDefault();
                    });
                $('#entity-relations .glyphicon')
                    .addClass('glyphicon-eye-open')
                    .removeClass('glyphicon-eye-close');
            }
        },
    });
    VIEWPORT_SIZE_MULTIPLIER = 3;

    $(document).ready(function() {
        dg_window_init();
        dg_svg_init();
        dg_svg_container_setup();
        dg_network_init();
        dg_relations_init();
        dg_loading_init();
        dg_typeahead_init();

        $('#request-random').on("click touchstart", function(event) {
            event.preventDefault();
            $(this).tooltip('hide');
            $(this).trigger({
                type: 'discograph:request-random',
            });
        });
        $('#start-layout').on("click touchstart", function(event) {
            event.preventDefault();
            dg_network_forceLayout_restart();
        });
        $('#stop-layout').on("click touchstart", function(event) {
            event.preventDefault();
            dg_network_forceLayout_stop();
        });
        $('#print').on("click touchstart", function(event) {
            event.preventDefault();
            dg_svg_print(dg.dimensions[0], dg.dimensions[1]);
        });
        $('#paging .next a').on("click", function(event) {
            $(this).trigger({
                type: 'discograph:select-next-page',
            });
            $(this).tooltip('hide');
        });
        $('#paging .previous a').on("click", function(event) {
            $(this).trigger({
                type: 'discograph:select-previous-page',
            });
            $(this).tooltip('hide');
        });
        $('#filter-roles').select2().on('select2:select', function(event) {
            $(window).trigger({
                type: 'discograph:request-network',
                entityKey: dg.network.data.json.center.key,
                pushHistory: true,
            });
        });
        $('#filter-roles').select2().on('select2:unselect', function(event) {
            $(window).trigger({
                type: 'discograph:request-network',
                entityKey: dg.network.data.json.center.key,
                pushHistory: true,
            });
        });
        $('#filter').fadeIn(3000);

        // Tooltip from Bootstrap
        $('[data-toggle="tooltip"]').tooltip();
        $(function() {
            $('[data-tooltip="tooltip"]').tooltip({
                trigger: 'hover'
            });
        });

        dg.fsm = new DiscographFsm();
        console.log('discograph initialized.');
    });

    function dg_svg_container_setup() {
        var windowWidth = dg.dimensions[0] / VIEWPORT_SIZE_MULTIPLIER;
        var windowHeight = dg.dimensions[1] / VIEWPORT_SIZE_MULTIPLIER;
        var navTopHeight = $('#nav-top').height();
        var navBottomHeight = $('#nav-bottom').height();
        $('#svg-container').width(windowWidth);
        $('#svg-container').height(windowHeight - navBottomHeight);
        $('#svg-container').scrollLeft((VIEWPORT_SIZE_MULTIPLIER - 1) * windowWidth / 2);
        $('#svg-container').scrollTop((VIEWPORT_SIZE_MULTIPLIER - 1) * windowHeight / 2);
    }

    function dg_window_init() {
        // Setup window dimensions
        var w = window,
            d = document,
            e = d.documentElement,
            g = d.getElementsByTagName('body')[0];
        dg.dimensions = [
            (w.innerWidth || e.clientWidth || g.clientWidth) * VIEWPORT_SIZE_MULTIPLIER,
            (w.innerHeight || e.clientHeight || g.clientHeight) * VIEWPORT_SIZE_MULTIPLIER,
        ];
        console.log("window dimensions: ", dg.dimensions);
        // All nodes start at center of the screen
        dg.network.newNodeCoords = [
            dg.dimensions[0] / 2,
            dg.dimensions[1] / 2,
        ];
    }
    if (typeof define === "function" && define.amd) define(dg);
    else if (typeof module === "object" && module.exports) module.exports = dg;
    this.dg = dg;
}();
