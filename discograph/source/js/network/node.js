NODE_DEBOUNCE_TIME = 250
NODE_OUT_TRANSITION_TIME = 500
NODE_UPDATE_TRANSITION_TIME = 5000
NODE_PALETTE = "Palette3"

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
            var classes = [
                "node",
                d.key.split('-')[0],
                NODE_PALETTE,
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
        .select(function(d, i) {return 0 < d.size ? this : null; })
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
        .attr("class", "inner")
        .attr("height", function(d) { return 2 * dg_network_getInnerRadius(d); })
        .attr("width", function(d) { return 2 * dg_network_getInnerRadius(d); })
        .attr("x", function(d) { return -1 * dg_network_getInnerRadius(d); })
        .attr("y", function(d) { return -1 * dg_network_getInnerRadius(d); });

    // MORE
    nodeEnter.append("path")
        .attr("class", "more")
        // Show a + symbol if there are extra links from this node that are missing / not shown
        .attr("d", d3.symbol(d3.symbolCross, 64))
        .style("opacity", function(d) {return d.missing > 0 ? 1 : 0; });
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
        .style("opacity", function(d) {return d.missing > 0 ? 1 : 0; });

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