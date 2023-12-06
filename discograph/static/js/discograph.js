! function() {
    var dg = {
        version: "0.2"
    };

    function dg_color_greyscale(d) {
        var hue = 0;
        var saturation = 0;
        var lightness = (d.distance / (dg.network.data.maxDistance + 1));
        return d3.hsl(hue, saturation, lightness).toString();
    }

    function dg_color_heatmap(d) {
        var hue = ((d.distance / 12) * 360) % 360;
        var variation_a = ((d.id % 5) - 2) / 20;
        var variation_b = ((d.id % 9) - 4) / 80;
        var saturation = 0.67 + variation_a;
        var lightness = 0.5 + variation_b;
        return d3.hsl(hue, saturation, lightness).toString();
    }

    dg.loading = {};

    function dg_loading_init() {
        var layer = d3.select('#svg').append('g')
            .attr('id', 'loadingLayer')
            .attr('class', 'centered')
            .attr('transform', 'translate(' +
                dg.dimensions[0] / 2 +
                ',' +
                dg.dimensions[1] / 2 +
                ')'
            );
        dg.loading.arc = d3.svg.arc()
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
                //            .removeClass("glyphicon-random")
                .addClass("glyphicon-animate glyphicon-refresh");
        } else {
            var data = [],
                extent = [0, 0];
            $("#page-loading")
                .removeClass("glyphicon-animate glyphicon-refresh")
            //            .addClass("glyphicon-random");
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
        var barScale = d3.scale.linear()
            .domain(extent)
            .range([
                dg.loading.barHeight / 4,
                dg.loading.barHeight
            ]);
        dg.loading.selection = dg.loading.selection.data(
            data,
            function(d) {
                return Math.random();
            });
        var scale = d3.scale.category10();
        var selectionEnter = dg.loading.selection.enter()
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
        var selectionExit = dg.loading.selection.exit();
        dg_loading_transition_update(selectionEnter, barScale);
        dg_loading_transition_exit(selectionExit);
        if (dg.loading.selection.length) {
            dg_loading_rotate(dg.loading.selection);
        }
    }

    function dg_loading_transition_update(selection, barScale) {
        selection.transition()
            .duration(1000)
            .delay(function(d, i) {
                return (selection.length - i) * 100;
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
        selection.transition()
            .duration(1000)
            .delay(function(d, i) {
                return (selection.length - i) * 100;
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
            .each('end', function(d) {
                d.active = false;
                this.remove();
            });
    }

    function dg_loading_rotate(selection) {
        var start = Date.now();
        selection.each(function(d) {
            if (d.hasTimer) {
                return;
            }
            d.hasTimer = true;
            d3.timer(function() {
                if (!d.active) {
                    return true;
                }
                selection.attr('transform', function(d) {
                    var now = Date.now();
                    var angle = (now - start) * d.rotationRate;
                    if (0 < d.outerRadius) {
                        angle = angle / d.outerRadius;
                    }
                    return 'rotate(' + angle + ')';
                });
            });
        });
    }
    LINK_STRENGTH = 2.5
    //LINK_STRENGTH = 1.5
    FRICTION = 0.7
    //FRICTION = 0.9
    CHARGE = -900
    //CHARGE = -300
    GRAVITY = 0.4
    //GRAVITY = 0.2
    THETA = 1
    ALPHA = 0.1
    LINK_DISTANCE_ALIAS = 10
    //LINK_DISTANCE_ALIAS = 100
    LINK_DISTANCE_RELEASED_ON = 200
    LINK_DISTANCE = 100
    LINK_DISTANCE_RANDOM = 100
    //LINK_DISTANCE_RANDOM = 20

    function dg_network_setupForceLayout() {
        return d3.layout.force()
            .nodes(dg.network.pageData.nodes)
            .links(dg.network.pageData.links)
            .size(dg.dimensions)
            .on("tick", dg_network_tick)
            .linkStrength(LINK_STRENGTH)
            .friction(FRICTION)
            .linkDistance(function(d, i) {
                if (d.isSpline) {
                    if (d.role == 'Released On') {
                        return LINK_DISTANCE_RELEASED_ON / 2 * dg.zoomFactor;
                    }
                    return LINK_DISTANCE / 2 * dg.zoomFactor;
                } else if (d.role == 'Alias') {
                    return LINK_DISTANCE_ALIAS * dg.zoomFactor;
                } else if (d.role == 'Released On') {
                    return LINK_DISTANCE_RELEASED_ON * dg.zoomFactor;
                } else {
                    return LINK_DISTANCE * dg.zoomFactor + (Math.tanh(Math.random()) * LINK_DISTANCE_RANDOM * dg.zoomFactor);
                }
            })
            .charge(CHARGE)
            .gravity(GRAVITY)
            .theta(THETA)
            .alpha(ALPHA);
    }

    function dg_network_startForceLayout() {
        var keyFunc = function(d) {
            return d.key
        }
        var nodes = dg.network.pageData.nodes.filter(function(d) {
            return (!d.isIntermediate) &&
                (-1 != d.pages.indexOf(dg.network.pageData.currentPage));
        })
        var links = dg.network.pageData.links.filter(function(d) {
            return (!d.isSpline) &&
                (-1 != d.pages.indexOf(dg.network.pageData.currentPage));
        })
        dg.network.selections.halo = dg.network.selections.halo.data(nodes, keyFunc);
        dg.network.selections.node = dg.network.selections.node.data(nodes, keyFunc);
        dg.network.selections.text = dg.network.selections.text.data(nodes, keyFunc);
        dg.network.selections.link = dg.network.selections.link.data(links, keyFunc);
        var hullNodes = dg.network.pageData.nodes.filter(function(d) {
            return d.cluster !== undefined;
        });
        var hullData = d3.nest().key(function(d) {
                return d.cluster;
            })
            .entries(hullNodes)
            .filter(function(d) {
                return 1 < d.values.length;
            });
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
        dg.network.forceLayout.start();
    }

    function dg_network_processJson(json) {
        var newNodeMap = d3.map();
        var newLinkMap = d3.map();
        json.nodes.forEach(function(node) {
            node.radius = dg_network_getOuterRadius(node);
            newNodeMap.set(node.key, node);
        });
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
        var nodeKeysToRemove = [];
        dg.network.data.nodeMap.keys().forEach(function(key) {
            if (!newNodeMap.has(key)) {
                nodeKeysToRemove.push(key);
            };
        });
        nodeKeysToRemove.forEach(function(key) {
            dg.network.data.nodeMap.remove(key);
        });
        var linkKeysToRemove = [];
        dg.network.data.linkMap.keys().forEach(function(key) {
            if (!newLinkMap.has(key)) {
                linkKeysToRemove.push(key);
            };
        });
        linkKeysToRemove.forEach(function(key) {
            dg.network.data.linkMap.remove(key);
        });
        newNodeMap.entries().forEach(function(entry) {
            var key = entry.key;
            var newNode = entry.value;
            if (dg.network.data.nodeMap.has(key)) {
                var oldNode = dg.network.data.nodeMap.get(key);
                oldNode.cluster = newNode.cluster;
                oldNode.distance = newNode.distance;
                oldNode.links = newNode.links;
                oldNode.missing = newNode.missing;
                oldNode.missingByPage = newNode.missingByPage;
                oldNode.pages = newNode.pages;
            } else {
                newNode.x = dg.network.newNodeCoords[0] + (Math.random() * LINK_DISTANCE * 2.0 * dg.zoomFactor) - LINK_DISTANCE * dg.zoomFactor;
                newNode.y = dg.network.newNodeCoords[1] + (Math.random() * LINK_DISTANCE * 2.0 * dg.zoomFactor) - LINK_DISTANCE * dg.zoomFactor;
                dg.network.data.nodeMap.set(key, newNode);
            }
        });
        newLinkMap.entries().forEach(function(entry) {
            var key = entry.key;
            var newLink = entry.value;
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
        var distances = []
        dg.network.data.nodeMap.values().forEach(function(node) {
            if (node.distance !== undefined) {
                distances.push(node.distance);
            }
        })
        dg.network.data.maxDistance = Math.max.apply(Math, distances);
    }

    function dg_network_onHaloEnter(haloEnter) {
        var haloEnter = haloEnter.append("g")
            .attr("class", function(d) {
                var classes = [
                    "node",
                    d.key,
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
        var hullEnter = hullEnter.append("g")
            .attr("class", function(d) {
                return "hull hull-" + d.key
            });
        hullEnter.append("path");
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
    var tip = d3.tip()
        .attr('class', 'd3-tip')
        .direction('e')
        .offset([0, 20])
        .html(dg_network_tooltip);

    function dg_network_onLinkEnter(linkEnter) {
        var linkEnter = linkEnter.append("g")
            .attr("class", function(d) {
                var parts = d.key.split('-');
                var role = parts.slice(2, 2 + parts.length - 4).join('-')
                var classes = [
                    "link",
                    "link-" + d.key,
                    role,
                ];
                return classes.join(" ");
            });
        dg_network_onLinkEnterElementConstruction(linkEnter);
        dg_network_onLinkEnterEventBindings(linkEnter);
    }

    function dg_network_onLinkEnterElementConstruction(linkEnter) {
        linkEnter.append("path")
            .attr("class", "inner");
        linkEnter.append("text")
            .attr('class', 'outer')
            .text(dg_network_linkAnnotation);
        linkEnter.append("text")
            .attr('class', 'inner')
            .text(dg_network_linkAnnotation);
    }

    function dg_network_onLinkEnterEventBindings(linkEnter) {
        var debounce = $.debounce(250, function(self, d, status) {
            if (status) {
                tip.show(d, d3.select(self).select('text').node());
            } else {
                tip.hide(d);
            }
        });
        linkEnter.on("mouseover", function(d) {
            d3.select(this)
                .classed("selected", true)
                .transition();
            debounce(this, d, true);
        });
        linkEnter.on("mouseout", function(d) {
            d3.select(this)
                .classed("selected", false)
                .transition()
                .duration(500);
            debounce(this, d, false);
        });
        //    linkEnter.on("mouseover", function(d) {
        //        d3.select(this).select(".inner")
        //            .transition()
        //            .style("stroke-width", 3);
        //        debounce(this, d, true);
        //    });
        //    linkEnter.on("mouseout", function(d) {
        //        d3.select(this).select(".inner")
        //            .transition()
        //            .duration(500)
        //            .style("stroke-width", 1);
        //        debounce(this, d, false);
        //    });
    }

    function dg_network_tooltip(d) {
        var parts = [
            '<p>' + d.source.name + '</p>',
            '<p><strong>&laquo; ' + d.role + ' &raquo;</strong></p>',
            '<p>' + d.target.name + '</p>',
        ];
        return parts.join('');
    }

    function dg_network_linkAnnotation(d) {
        return d.role.split(' ').map(function(x) {
            return x[0];
        }).join('');
    }

    function dg_network_onLinkExit(linkExit) {
        linkExit.remove();
    }

    function dg_network_onLinkUpdate(linkSelection) {}

    function dg_network_onNodeEnter(nodeEnter) {
        var nodeEnter = nodeEnter.append("g")
            .attr("class", function(d) {
                var classes = [
                    "node",
                    d.key,
                    d.key.split('-')[0],
                ];
                return classes.join(" ");
            })
            .style("fill", function(d) {
                if (d.type == 'artist') {
                    return dg_color_heatmap(d);
                } else {
                    return dg_color_greyscale(d);
                }
            })
            .call(dg.network.forceLayout.drag);
        dg_network_onNodeEnterElementConstruction(nodeEnter);
        dg_network_onNodeEnterEventBindings(nodeEnter);
    }

    function dg_network_onNodeEnterElementConstruction(nodeEnter) {
        var artistEnter = nodeEnter.select(function(d) {
            return d.type == 'artist' ? this : null;
        });
        artistEnter
            .append("circle")
            .attr("class", "shadow")
            .attr("cx", 5)
            .attr("cy", 5)
            .attr("r", function(d) {
                return 7 + dg_network_getOuterRadius(d)
            });
        artistEnter
            .select(function(d, i) {
                return 0 < d.size ? this : null;
            })
            .append("circle")
            .attr("class", "outer")
            .attr("r", dg_network_getOuterRadius);
        artistEnter
            .append("circle")
            .attr("class", "inner")
            .attr("r", dg_network_getInnerRadius);

        var labelEnter = nodeEnter.select(function(d) {
            return d.type == 'label' ? this : null;
        });
        labelEnter
            .append("rect")
            .attr("class", "inner")
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
        nodeEnter.append("path")
            .attr("class", "more")
            .attr("d", d3.svg.symbol().type("cross").size(64))
            .style("opacity", function(d) {
                return 0 < d.missing ? 1 : 0;
            });
        nodeEnter.append("title")
            .text(function(d) {
                return d.name;
            });
    }

    function dg_network_onNodeEnterEventBindings(nodeEnter) {
        nodeEnter.on("mouseover", function(d) {
            var selection = dg.network.selections.node.select(function(n) {
                return n.key == d.key ? this : null;
            });
            selection.moveToFront();
        });
        nodeEnter.on("mousedown", function(d) {
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
            d3.event.stopPropagation(); // Prevents propagation to #svg element.
        });
        nodeEnter.on("touchstart", function(d) {
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
            d3.event.stopPropagation(); // Prevents propagation to #svg element.
        });
    }

    function dg_network_onNodeExit(nodeExit) {
        nodeExit.remove();
    }

    function dg_network_onNodeUpdate(nodeUpdate) {
        nodeUpdate.transition()
            .duration(1000)
            .style("fill", function(d) {
                if (d.type == 'artist') {
                    return dg_color_heatmap(d);
                } else {
                    return dg_color_greyscale(d);
                }
            })
        nodeUpdate.selectAll(".more").each(function(d, i) {
            var prevMissing = Boolean(d.hasMissing);
            var prevMissingByPage = Boolean(d.hasMissingByPage);
            var currMissing = Boolean(d.missing);
            if (!d.missingByPage) {
                var currMissingByPage = false;
            } else {
                var currMissingByPage = Boolean(
                    d.missingByPage[dg.network.pageData.currentPage]
                );
            }
            d3.select(this).transition().duration(1000)
                .style('opacity', function(d) {
                    return (currMissing || currMissingByPage) ? 1 : 0;
                })
                .attrTween('transform', function(d) {
                    var start = prevMissingByPage ? 45 : 0;
                    var stop = currMissingByPage ? 45 : 0;
                    return d3.interpolateString(
                        "rotate(" + start + ")",
                        "rotate(" + stop + ")"
                    );
                });
            d.hasMissing = currMissing;
            d.hasMissingByPage = currMissingByPage;
        });
    }
    dg.network = {
        dimensions: [0, 0],
        forceLayout: null,
        isUpdating: false,
        newNodeCoords: [0, 0],
        data: {
            json: null,
            nodeMap: d3.map(),
            linkMap: d3.map(),
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
        if ((1 <= page) && (page <= dg.network.data.pageCount)) {
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
        var filteredNodes = dg.network.data.nodeMap.values().filter(function(d) {
            return (-1 != d.pages.indexOf(currentPage));
        });
        var filteredLinks = dg.network.data.linkMap.values().filter(function(d) {
            return (-1 != d.pages.indexOf(currentPage));
        });
        dg.network.pageData.nodes.length = 0;
        dg.network.pageData.links.length = 0;
        Array.prototype.push.apply(dg.network.pageData.nodes, filteredNodes);
        Array.prototype.push.apply(dg.network.pageData.links, filteredLinks);
        dg.network.forceLayout.nodes(filteredNodes);
        dg.network.forceLayout.links(filteredLinks);
    }

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
    }

    function dg_network_onTextEnter(textEnter) {
        var textEnter = textEnter.append("g")
            .attr("class", function(d) {
                var classes = [
                    "node",
                    d.key,
                    d.key.split('-')[0],
                ];
                return classes.join(" ");
            })
        textEnter.append("text")
            .attr("class", "outer")
            .attr("dx", function(d) {
                return dg_network_getOuterRadius(d) + 3;
            })
            .attr("dy", ".35em")
            .text(dg_network_getNodeText);
        textEnter.append("text")
            .attr("class", "inner")
            .attr("dx", function(d) {
                return dg_network_getOuterRadius(d) + 3;
            })
            .attr("dy", ".35em")
            .text(dg_network_getNodeText);
    }

    function dg_network_onTextExit(textExit) {
        textExit.remove();
    }

    function dg_network_onTextUpdate(textUpdate) {
        textUpdate.select('.outer').text(dg_network_getNodeText);
        textUpdate.select('.inner').text(dg_network_getNodeText);
    }

    function dg_network_getOuterRadius(d) {
        if (0 < d.size) {
            return 12 + (Math.sqrt(d.size) * 2);
        }
        return 9 + (Math.sqrt(d.size) * 2);
    }

    function dg_network_getInnerRadius(d) {
        return 9 + (Math.sqrt(d.size) * 2);
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
            var radius = d.radius;
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
        var x1 = d.source.x,
            y1 = d.source.y,
            x2 = d.target.x,
            y2 = d.target.y;
        var node = path.node();
        var point = node.getPointAtLength(node.getTotalLength() / 2);
        var angle = Math.atan2((y2 - y1), (x2 - x1)) * (180 / Math.PI);
        var text = group.selectAll('text')
            .attr('transform', [
                'rotate(' + angle + ' ' + point.x + ' ' + point.y + ')',
                'translate(' + point.x + ',' + point.y + ')',
            ].join(' '));
    }

    function dg_network_translate(d) {
        return 'translate(' + d.x + ',' + d.y + ')';
    }

    function dg_network_tick(e) {
        var k = 1.0; //e.alpha * 5;
        if (dg.network.data.json) {
            var centerNode = dg.network.data.nodeMap.get(dg.network.data.json.center.key);
            if (!centerNode.fixed) {
                var dims = dg.dimensions;
                var dx = ((dims[0] / 2) - centerNode.x) * k;
                var dy = ((dims[1] / 2) - centerNode.y) * k;
                centerNode.x += dx;
                centerNode.y += dy;
            }
        }
        dg.network.selections.link.each(dg_network_tick_link);
        dg.network.selections.halo.attr('transform', dg_network_translate);
        dg.network.selections.node.attr('transform', dg_network_translate);
        dg.network.selections.text.attr('transform', dg_network_translate);
        dg.network.selections.hull.select('path').attr('d', function(d) {
            var vertices = d3.geom.hull(dg_network_getHullVertices(d.values));
            return 'M' + vertices.join('L') + 'Z';
        });
    }

    function dg_svg_init() {
        d3.selection.prototype.moveToFront = function() {
            return this.each(function() {
                this.parentNode.appendChild(this);
            });
        };
        var w = window,
            d = document,
            e = d.documentElement,
            g = d.getElementsByTagName('body')[0];
        dg.dimensions = [
            (w.innerWidth || e.clientWidth || g.clientWidth) * 2,
            (w.innerHeight || e.clientHeight || g.clientHeight) * 2,
        ];
        dg.zoomFactor = 0.2; //Math.min(dg.dimensions[0], dg.dimensions[1]) / 2048;
        dg.network.newNodeCoords = [
            dg.dimensions[0],
            dg.dimensions[1],
        ];
        d3.select("#svg")
            .attr("width", dg.dimensions[0])
            .attr("height", dg.dimensions[1]);
        dg_svg_setupDefs();
        d3.select('#svg').call(tip);
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
        var barScale = d3.scale.sqrt()
            .exponent(0.25)
            .domain(extent)
            .range([barHeight / 4, barHeight]);
        var keys = data.map(function(d, i) {
            return d.key;
        });
        var numBars = keys.length;
        var arc = d3.svg.arc()
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
            .on('mouseover', function() {
                d3.select(this).moveToFront();
            });
        var arcs = segments.append('path')
            .attr('class', 'arc')
            .attr('d', arc)
            .each(function(d) {
                d.outerRadius = 0;
            })
            .on('mousedown', function(d) {
                var values = $('#filter-roles').val();
                values.push(d.key);
                $('#filter-roles').val(values).trigger('change');
                dg.fsm.requestNetwork(dg.network.data.json.center.key, true);
                d3.event.stopPropagation();
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
        $('#search .clear').on("click", function() {
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
            $(window).on('discograph:request-random', function() {
                self.requestRandom();
            });
            $(window).on('discograph:select-entity', function(event) {
                self.selectEntity(event.entityKey, event.fixed);
            });
            $(window).on('discograph:select-next-page', function() {
                self.selectNextPage();
            });
            $(window).on('discograph:select-previous-page', function() {
                self.selectPreviousPage();
            });
            $(window).on('discograph:show-network', function() {
                self.showNetwork();
            });
            $(window).on('discograph:show-radial', function() {
                self.showRadial();
            });
            $(window).on('select2:selecting', function(event) {
                self.rolesBackup = $('#filter select').val();
            });
            $(window).on('select2:unselecting', function(event) {
                self.rolesBackup = $('#filter select').val();
            });
            window.onpopstate = function(event) {
                console.log("onpopstate");
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
                console.log("resize");
                var w = window,
                    d = document,
                    e = d.documentElement,
                    g = d.getElementsByTagName('body')[0];
                dg.dimensions = [
                    (w.innerWidth || e.clientWidth || g.clientWidth) * 2,
                    (w.innerHeight || e.clientHeight || g.clientHeight) * 2,
                ];

                windowWidth = $(window).width();
                windowHeight = $(window).height();
                navTopHeight = $('#nav-top').height();
                navBottomHeight = $('#nav-bottom').height();
                $('#svg-container').height(windowHeight - navBottomHeight);
                $('#svg-container').scrollLeft(windowWidth / 2);
                $('#svg-container').scrollTop(windowHeight / 2);

                dg.zoomFactor = 0.2; //Math.min(dg.dimensions[0], dg.dimensions[1]) / 2048;
                d3.select('#svg')
                    .attr('width', dg.dimensions[0])
                    .attr('height', dg.dimensions[1]);
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
                dg.network.forceLayout.size(dg.dimensions);
                if (self.state == 'viewing-network') {
                    dg.network.forceLayout.start();
                }
            }));
            $('#svg').on('mousedown', function() {
                if (self.state == 'viewing-network') {
                    self.selectEntity(null);
                } else if (self.state == 'viewing-radial') {
                    self.showNetwork();
                }
            });
            self.on("*", function(eventName, data) {
                console.log("FSM: ", eventName, data);
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
                    console.log("request-network");
                    this.requestNetwork(entityKey);
                },
                'request-random': function() {
                    this.requestRandom();
                },
                'load-inline-data': function() {
                    console.log("load-inline-data start");
                    var params = {
                        'roles': $('#filter select').val()
                    };
                    console.log("load-inline-data", params);
                    console.log("load-inline-data", dgData);
                    //                this.handle("received-network", dgData, false, params);
                    console.log("load-inline-data");
                    this.deferAndTransition("requesting");
                    this.handle("received-network", dgData, false, params);
                    console.log("load-inline-data end");
                },
            },
            'viewing-network': {
                '_onEnter': function() {
                    this.toggleNetwork(true);
                },
                '_onExit': function() {
                    this.toggleNetwork(false);
                },
                'request-network': function(entityKey) {
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
                    dg.network.pageData.selectedNodeKey = entityKey;
                    if (entityKey !== null) {
                        var selectedNode = dg.network.data.nodeMap.get(entityKey);
                        var currentPage = dg.network.pageData.currentPage;
                        if (-1 == selectedNode.pages.indexOf(currentPage)) {
                            dg.network.pageData.selectedNodeKey = null;
                        }
                    }
                    entityKey = dg.network.pageData.selectedNodeKey;
                    if (entityKey !== null) {
                        var nodeOn = dg.network.layers.root.selectAll('.' + entityKey);
                        var nodeOff = dg.network.layers.root.selectAll('.node:not(.' + entityKey + ')');
                        var linkKeys = nodeOn.datum().links;
                        var linkOn = dg.network.selections.link.filter(function(d) {
                            return 0 <= linkKeys.indexOf(d.key);
                        });
                        var linkOff = dg.network.selections.link.filter(function(d) {
                            return linkKeys.indexOf(d.key) == -1;
                        });
                        var node = dg.network.data.nodeMap.get(entityKey);
                        var url = 'http://discogs.com/' + node.type + '/' + node.id;
                        $('#entity-name').text(node.name);
                        $('#entity-link').attr('href', url);
                        $('#entity-details').removeClass('hidden').show(0);
                        nodeOn.moveToFront();
                        nodeOn.classed('selected', true);
                        if (fixed) {
                            //nodeOn.each(function(d) { d.fixed = true; });
                            node.fixed = true;
                        }
                        linkOn.classed('highlighted', true);
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
                    this.toggleLoading(true);
                    this.toggleFilter(false);
                },
                '_onExit': function() {
                    this.toggleLoading(false);
                    this.toggleFilter(true);
                },
                'errored': function(error) {
                    this.handleError(error);
                },
                'received-network': function(data, pushHistory, params) {
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
                    if (1 < data.pages) {
                        $('#paging').fadeIn();
                    } else {
                        $('#paging').fadeOut();
                    }
                    dg_network_processJson(data);
                    dg_network_selectPage(1);
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
                    dg.relations.byYear = d3.nest()
                        .key(function(d) {
                            return d.year;
                        })
                        .key(function(d) {
                            return d.category;
                        })
                        .entries(data.results);
                    dg.relations.byRole = d3.nest()
                        .key(function(d) {
                            return d.role;
                        })
                        .sortKeys(d3.ascending)
                        .rollup(function(leaves) {
                            return leaves.length;
                        })
                        .entries(dg.relations.data.results);
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
            d3.json(this.getNetworkURL(entityKey), function(error, data) {
                if (error) {
                    self.handleError(error);
                } else {
                    self.handle('received-network', data, pushHistory);
                }
            });
        },
        requestRadial: function(entityKey) {
            this.transition('requesting');
            var self = this;
            d3.json(this.getRadialURL(entityKey), function(error, data) {
                if (error) {
                    self.handleError(error);
                } else {
                    self.handle('received-radial', data);
                }
            });
        },
        requestRandom: function() {
            this.transition('requesting');
            var self = this;
            d3.json(this.getRandomURL(), function(error, data) {
                if (error) {
                    self.handleError(error);
                } else {
                    self.handle('received-random', data);
                }
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
            console.log("toggleNetwork");
            if (status) {
                if (1 < dg.network.data.json.pages) {
                    $('#paging').fadeIn();
                } else {
                    $('#paging').fadeOut();
                }
                dg.network.layers.root.transition()
                    .duration(250)
                    .style('opacity', 1)
                    .each('end', function(d, i) {
                        dg.network.layers.link.selectAll('.link')
                            .classed('noninteractive', false);
                        dg.network.layers.node.selectAll('.node')
                            .classed('noninteractive', false);
                        dg.network.forceLayout.start()
                    });
            } else {
                $('#paging').fadeOut();
                dg.network.forceLayout.stop()
                dg.network.layers.root.transition()
                    .duration(250)
                    .style('opacity', 0.25);
                dg.network.layers.link.selectAll('.link')
                    .classed('noninteractive', true);
                dg.network.layers.node.selectAll('.node')
                    .classed('noninteractive', true);
            }
        },
        toggleLoading: function(status) {
            console.log("toggleLoading");
            if (status) {
                console.log("state true");
                var input = dg_loading_makeArray();
                var data = input[0],
                    extent = input[1];
                $('#page-loading')
                    //                .removeClass('glyphicon-random')
                    .addClass('glyphicon-animate glyphicon-refresh');
            } else {
                console.log("state false");
                var data = [],
                    extent = [0, 0];
                $('#page-loading')
                    .removeClass('glyphicon-animate glyphicon-refresh')
                //                .addClass('glyphicon-random');
            }
            dg_loading_update(data, extent);
            console.log("toggleLoading end");
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
    $(document).ready(function() {
        dg_svg_init();
        dg_network_init();
        dg_relations_init();
        dg_loading_init();
        dg_typeahead_init();
        $('[data-toggle="tooltip"]').tooltip();
        $('#request-random').on("click touchstart", function(event) {
            event.preventDefault();
            $(this).tooltip('hide');
            $(this).trigger({
                type: 'discograph:request-random',
            });
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

        windowWidth = $(window).width();
        windowHeight = $(window).height();
        navTopHeight = $('#nav-top').height();
        navBottomHeight = $('#nav-bottom').height();
        $('#svg-container').height(windowHeight - navBottomHeight);
        $('#svg-container').scrollLeft(windowWidth / 2);
        $('#svg-container').scrollTop(windowHeight / 2);

        $(function() {
            $('[data-tooltip="tooltip"]').tooltip({
                trigger: 'hover'
            });
        });

        dg.fsm = new DiscographFsm();
        console.log('discograph initialized.');
    });
    if (typeof define === "function" && define.amd) define(dg);
    else if (typeof module === "object" && module.exports) module.exports = dg;
    this.dg = dg;
}();
